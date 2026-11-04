package consumer

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLongRunMemoryLeak 长时间运行内存泄漏检测：
// 3 个 worker 持续消费（fetch + process 循环），周期采样 HeapAlloc / HeapObjects，
// 验证内存稳定不增长。
//
// 判定：稳态后（前 N 秒为预热），后续每 5 秒的 HeapAlloc 峰值不应持续超过
// 稳态基线的 X 倍；HeapObjects 不应单调增长。
func TestLongRunMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过长时间内存测试")
	}
	shards := makeShards(20)
	srv := newFlakyLTSServer(shards, 20*time.Millisecond) // 快速 fetch，制造大量对象
	defer srv.srv.Close()
	endpoint := strings.TrimPrefix(srv.srv.URL, "https://")

	// 起 3 个常驻 worker
	workers := make([]*ClientConsumerWorker, 0, 3)
	for i := 0; i < 3; i++ {
		w := startWorkerFast(endpoint, 100)
		workers = append(workers, w)
	}
	defer func() {
		for _, w := range workers {
			w.Shutdown()
		}
	}()

	// 预热 10 秒：让 worker 完全进入稳态（连接池/缓存/初始对象建立完毕）
	time.Sleep(10 * time.Second)

	// 持续运行 + 周期采样 30 秒（首个采样作为稳态参照，不用 0 时刻避免预热误判）
	duration := 30 * time.Second
	interval := 5 * time.Second
	start := time.Now()
	var samples []uint64
	var objectsSamples []uint64
	for time.Since(start) < duration {
		time.Sleep(interval)
		var alloc, objects uint64
		sampleHeap(&alloc, &objects)
		samples = append(samples, alloc)
		objectsSamples = append(objectsSamples, objects)
		t.Logf("sample %d: HeapAlloc=%d KB HeapObjects=%d", len(samples), alloc/1024, objects)
	}

	if len(samples) < 4 {
		t.Fatalf("采样不足: got %d", len(samples))
	}

	// 判定：末 1/3 样本的中位数不应持续高于首 1/3 样本中位数的 1.5 倍。
	// 用中位数而非峰值，排除 GC 周期/连接池填充的瞬时尖峰。
	median := func(v []uint64) uint64 {
		vv := append([]uint64{}, v...)
		for i := 0; i < len(vv); i++ {
			for j := i + 1; j < len(vv); j++ {
				if vv[j] < vv[i] {
					vv[i], vv[j] = vv[j], vv[i]
				}
			}
		}
		return vv[len(vv)/2]
	}
	third := len(samples) / 3
	firstAlloc := median(samples[:third])
	lastAlloc := median(samples[len(samples)-third:])
	firstObjs := median(objectsSamples[:third])
	lastObjs := median(objectsSamples[len(objectsSamples)-third:])

	t.Logf("first-third median Alloc=%d KB, last-third median Alloc=%d KB", firstAlloc/1024, lastAlloc/1024)
	t.Logf("first-third median Objects=%d, last-third median Objects=%d", firstObjs, lastObjs)

	if float64(lastAlloc) > float64(firstAlloc)*1.5 {
		t.Fatalf("heap alloc 持续增长: first=%dKB last=%dKB - possible leak", firstAlloc/1024, lastAlloc/1024)
	}
	if float64(lastObjs) > float64(firstObjs)*1.5 {
		t.Fatalf("heap objects 持续增长: first=%d last=%d - possible leak", firstObjs, lastObjs)
	}
}

func sampleHeap(alloc, objects *uint64) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	*alloc = m.HeapAlloc
	*objects = m.HeapObjects
}

// TestLongRunWithRebalanceMemoryLeak 长时间运行 + 周期性 rebalance 的内存泄漏：
// 常驻 worker 之外，每 10 秒加入/退出一个 worker 制造 rebalance，
// 验证 rebalance 频繁发生下内存不增长。
func TestLongRunWithRebalanceMemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过长时间内存测试")
	}
	shards := makeShards(20)
	srv := newFlakyLTSServer(shards, 20*time.Millisecond)
	defer srv.srv.Close()
	endpoint := strings.TrimPrefix(srv.srv.URL, "https://")

	// 常驻 2 个 worker
	resident := make([]*ClientConsumerWorker, 0, 2)
	for i := 0; i < 2; i++ {
		resident = append(resident, startWorkerFast(endpoint, 100))
	}
	defer func() {
		for _, w := range resident {
			w.Shutdown()
		}
	}()

	// 预热
	time.Sleep(10 * time.Second)

	// 同时：每 10 秒起一个临时 worker 再停掉（rebalance）
	var wg sync.WaitGroup
	var panicErr atomic.Value
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicErr.Store(fmt.Sprintf("%v", r))
					}
				}()
				tmp := startWorkerFast(endpoint, 100)
				time.Sleep(8 * time.Second)
				tmp.Shutdown()
			}()
		}
	}()

	// 采样 30 秒
	duration := 30 * time.Second
	interval := 5 * time.Second
	start := time.Now()
	var samples []uint64
	var objectsSamples []uint64
	for time.Since(start) < duration {
		time.Sleep(interval)
		var alloc, objects uint64
		sampleHeap(&alloc, &objects)
		samples = append(samples, alloc)
		objectsSamples = append(objectsSamples, objects)
		t.Logf("sample %d: HeapAlloc=%d KB HeapObjects=%d", len(samples), alloc/1024, objects)
	}
	close(stop)
	wg.Wait()

	if p := panicErr.Load(); p != nil {
		t.Fatalf("PANIC during rebalance memory test: %v", p)
	}

	if len(samples) < 4 {
		t.Fatalf("采样不足: got %d", len(samples))
	}
	median := func(v []uint64) uint64 {
		vv := append([]uint64{}, v...)
		for i := 0; i < len(vv); i++ {
			for j := i + 1; j < len(vv); j++ {
				if vv[j] < vv[i] {
					vv[i], vv[j] = vv[j], vv[i]
				}
			}
		}
		return vv[len(vv)/2]
	}
	third := len(samples) / 3
	firstAlloc := median(samples[:third])
	lastAlloc := median(samples[len(samples)-third:])
	firstObjs := median(objectsSamples[:third])
	lastObjs := median(objectsSamples[len(objectsSamples)-third:])

	t.Logf("first-third median Alloc=%d KB, last-third median Alloc=%d KB", firstAlloc/1024, lastAlloc/1024)
	t.Logf("first-third median Objects=%d, last-third median Objects=%d", firstObjs, lastObjs)

	growthAlloc := float64(lastAlloc) / float64(firstAlloc+1)
	growthObjects := float64(lastObjs) / float64(firstObjs+1)
	if growthAlloc > 1.5 {
		t.Fatalf("heap alloc 持续增长: first=%dKB last=%dKB - possible leak", firstAlloc/1024, lastAlloc/1024)
	}
	if growthObjects > 1.5 {
		t.Fatalf("heap objects 持续增长: first=%d last=%d - possible leak", firstObjs, lastObjs)
	}
}

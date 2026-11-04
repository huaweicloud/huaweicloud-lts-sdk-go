package consumer

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 微基准：发送路径 ----

// BenchmarkBareSend 裸 channel 发送（修复前）：基线。
func BenchmarkBareSend(b *testing.B) {
	ch := make(chan *TaskResult, 1)
	// 消费端：持续 drain，模拟主循环读取
	go func() {
		for range ch {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- &TaskResult{}
	}
}

// BenchmarkSelectSend select+default 发送（修复后）：验证额外开销。
func BenchmarkSelectSend(b *testing.B) {
	w := newTestShardConsumer()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-w.taskResultChannel:
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.sendTaskResult(&TaskResult{})
	}
	b.StopTimer()
}

// BenchmarkSelectSendFull select+default 发送且 channel 满（竞态窗口）：
// 验证 default 分支（丢弃）的开销。
func BenchmarkSelectSendFull(b *testing.B) {
	w := newTestShardConsumer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.sendTaskResult(&TaskResult{}) // channel cap=1 且无人读，走 default
	}
}

// ---- 微基准：heartbeat 快照 ----

// BenchmarkHeartbeatNoLock 无锁遍历（修复前）：基线。
func BenchmarkHeartbeatNoLock(b *testing.B) {
	hb := &LogConsumerHeartBeat{
		allHeartShards: makeShardsMap(50),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = convertAllHeartShards(hb.allHeartShards)
	}
}

// BenchmarkHeartbeatWithLock 锁内快照（修复后）。
func BenchmarkHeartbeatWithLock(b *testing.B) {
	hb := &LogConsumerHeartBeat{
		allHeartShards: makeShardsMap(50),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.lock.Lock()
		_ = convertAllHeartShards(hb.allHeartShards)
		hb.lock.Unlock()
	}
}

func makeShardsMap(n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m["shard-"+itoa(i)] = ""
	}
	return m
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// ---- 端到端吞吐基准：消费 fetch→process 循环 ----

// BenchmarkConsumeThroughput 端到端消费吞吐：mock LTS + 单 worker，
// 统计 10 秒内成功 fetch 的次数（logSize>0 的次数），对比修复前后。
func BenchmarkConsumeThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("短模式跳过")
	}
	// 每次基准运行跑 10 秒真实消费
	shards := makeShards(10)
	srv := newFlakyLTSServer(shards, 5*time.Millisecond) // 快 fetch
	defer srv.srv.Close()
	endpoint := strings.TrimPrefix(srv.srv.URL, "https://")

	w := startWorkerFast(endpoint, 50) // 50ms fetch 间隔
	defer w.Shutdown()

	// 预热 2 秒
	time.Sleep(2 * time.Second)

	// 采样 10 秒
	before := srv.fetchCalls.Load()
	time.Sleep(10 * time.Second)
	after := srv.fetchCalls.Load()

	// 结果
	b.ReportMetric(float64(after-before), "fetch/10s")
	b.ReportMetric(0, "ns/op") // 抑制默认 ns/op
}

// 并发锁竞争基准：rebalance 场景下 heartbeat 锁竞争
func BenchmarkLockContention(b *testing.B) {
	hb := &LogConsumerHeartBeat{
		allHeartShards:    makeShardsMap(50),
		currentHeldShards: makeShardsMap(25),
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写方：模拟 unsubscribe（主循环持锁）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				hb.lock.Lock()
				for k := range hb.allHeartShards {
					delete(hb.allHeartShards, k)
					break
				}
				hb.lock.Unlock()
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.lock.Lock()
		_ = convertAllHeartShards(hb.allHeartShards)
		hb.lock.Unlock()
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	_ = runtime.NumCPU
}

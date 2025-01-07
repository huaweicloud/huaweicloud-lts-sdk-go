package producer

import (
	"container/list"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type IoThreadPool struct {
	threadPoolShutDownFlag *atomic.Bool
	queue                  *list.List
	lock                   sync.RWMutex
	ioWorker               *IoWorker
}

func initIoThreadPool(ioWorker *IoWorker) *IoThreadPool {
	return &IoThreadPool{
		threadPoolShutDownFlag: atomic.NewBool(false),
		queue:                  list.New(),
		ioWorker:               ioWorker,
	}
}

func (threadPool *IoThreadPool) addTask(batch *Batch) {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	threadPool.queue.PushBack(batch)
}

func (threadPool *IoThreadPool) popTask() *Batch {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	if threadPool.queue.Len() <= 0 {
		return nil
	}
	ele := threadPool.queue.Front()
	threadPool.queue.Remove(ele)
	return ele.Value.(*Batch)
}

func (threadPool *IoThreadPool) hasTask() bool {
	defer threadPool.lock.RUnlock()
	threadPool.lock.RLock()
	return threadPool.queue.Len() > 0
}

func (threadPool *IoThreadPool) start(ioWorkerWaitGroup *sync.WaitGroup, ioThreadPoolwait *sync.WaitGroup) {
	defer ioThreadPoolwait.Done()
	for {
		if task := threadPool.popTask(); task != nil {
			threadPool.ioWorker.startSendTask(ioWorkerWaitGroup)
			go func(producerBatch *Batch) {
				defer threadPool.ioWorker.closeSendTask(ioWorkerWaitGroup)
				threadPool.ioWorker.sendToServer(producerBatch)
			}(task)
		} else {
			if !threadPool.threadPoolShutDownFlag.Load() {
				time.Sleep(100 * time.Millisecond)
			} else {
				break
			}
		}
	}

}

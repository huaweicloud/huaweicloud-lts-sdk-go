package producer

import (
	"container/heap"
	"sync"
	"time"
)

// RetryQueue cache ProducerBatch and retry latter
type RetryQueue struct {
	batch []*Batch
	mutex sync.Mutex
}

func initRetryQueue() *RetryQueue {
	retryQueue := RetryQueue{}
	heap.Init(&retryQueue)
	return &retryQueue
}

func (retryQueue *RetryQueue) sendToRetryQueue(producerBatch *Batch) {
	retryQueue.mutex.Lock()
	defer retryQueue.mutex.Unlock()
	if producerBatch != nil {
		heap.Push(retryQueue, producerBatch)
	}
}

func (retryQueue *RetryQueue) getRetryBatch(moverShutDownFlag bool) (producerBatchList []*Batch) {
	retryQueue.mutex.Lock()
	defer retryQueue.mutex.Unlock()
	if !moverShutDownFlag {
		for retryQueue.Len() > 0 {
			producerBatch := heap.Pop(retryQueue)
			if producerBatch.(*Batch).nextRetryMs < GetTimeMs(time.Now().UnixNano()) {
				producerBatchList = append(producerBatchList, producerBatch.(*Batch))
			} else {
				heap.Push(retryQueue, producerBatch.(*Batch))
				break
			}
		}
	} else {
		for retryQueue.Len() > 0 {
			producerBatch := heap.Pop(retryQueue)
			producerBatchList = append(producerBatchList, producerBatch.(*Batch))
		}
	}
	return producerBatchList
}

func (retryQueue *RetryQueue) Len() int {
	return len(retryQueue.batch)
}

func (retryQueue *RetryQueue) Less(i, j int) bool {
	return retryQueue.batch[i].nextRetryMs < retryQueue.batch[j].nextRetryMs
}
func (retryQueue *RetryQueue) Swap(i, j int) {
	retryQueue.batch[i], retryQueue.batch[j] = retryQueue.batch[j], retryQueue.batch[i]
}
func (retryQueue *RetryQueue) Push(x interface{}) {
	item, ok := x.(*Batch)
	if !ok {
		return
	}
	retryQueue.batch = append(retryQueue.batch, item)
}
func (retryQueue *RetryQueue) Pop() interface{} {
	old := retryQueue.batch
	n := len(old)
	item := old[n-1]
	retryQueue.batch = old[0 : n-1]
	return item
}

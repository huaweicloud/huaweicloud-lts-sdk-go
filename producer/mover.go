package producer

import (
	"go.uber.org/atomic"
	"sync"
	"time"
)

type Mover struct {
	moverShutDownFlag *atomic.Bool
	retryQueue        *RetryQueue
	ioWorker          *IoWorker
	logAccumulator    *LogAccumulator
	threadPool        *IoThreadPool
}

func initMover(logAccumulator *LogAccumulator, retryQueue *RetryQueue, ioWorker *IoWorker, threadPool *IoThreadPool) *Mover {
	mover := &Mover{
		moverShutDownFlag: atomic.NewBool(false),
		retryQueue:        retryQueue,
		ioWorker:          ioWorker,
		logAccumulator:    logAccumulator,
		threadPool:        threadPool,
	}
	return mover

}

func (mover *Mover) run(moverWaitGroup *sync.WaitGroup, config *Config) {
	defer moverWaitGroup.Done()
	for !mover.moverShutDownFlag.Load() {
		sleepMs := config.LingerMs

		nowTimeMs := GetTimeMs(time.Now().UnixNano())
		mover.logAccumulator.lock.Lock()
		mapCount := len(mover.logAccumulator.logGroupData)
		for key, batch := range mover.logAccumulator.logGroupData {
			timeInterval := batch.createTimeMs + config.LingerMs - nowTimeMs
			if timeInterval <= 0 {
				mover.sendToServer(key, batch, config)
			} else {
				if sleepMs > timeInterval {
					sleepMs = timeInterval
				}
			}
		}
		mover.logAccumulator.lock.Unlock()

		if mapCount == 0 {
			sleepMs = config.LingerMs
		}

		retryProducerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag.Load())
		if retryProducerBatchList == nil {
			// If there is nothing to send in the retry queue, just wait for the minimum time that was given to me last time.
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		} else {
			count := len(retryProducerBatchList)
			for i := 0; i < count; i++ {
				mover.threadPool.addTask(retryProducerBatchList[i])
			}
		}

		if mover.moverShutDownFlag.Load() {
			break
		}
	}
	mover.logAccumulator.lock.Lock()
	for _, batch := range mover.logAccumulator.logGroupData {
		mover.threadPool.addTask(batch)
	}
	mover.logAccumulator.logGroupData = make(map[string]*Batch)
	mover.logAccumulator.lock.Unlock()

	producerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag.Load())
	count := len(producerBatchList)
	for i := 0; i < count; i++ {
		mover.threadPool.addTask(producerBatchList[i])
	}
}

func (mover *Mover) sendToServer(key string, batch *Batch, config *Config) {
	if value, ok := mover.logAccumulator.logGroupData[key]; !ok {
		return
	} else if GetTimeMs(time.Now().UnixNano())-value.createTimeMs < config.LingerMs {
		return
	}
	mover.threadPool.addTask(batch)
	delete(mover.logAccumulator.logGroupData, key)
}

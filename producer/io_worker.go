package producer

import (
	uberAtomic "go.uber.org/atomic"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type CallBack interface {
	Success(result *Result)
	Fail(result *Result)
}

type IoWorker struct {
	taskCount              int64
	client                 ClientInterface
	retryQueue             *RetryQueue
	retryQueueShutDownFlag *uberAtomic.Bool
	maxIoWorker            chan int64
	noRetryStatusCodeMap   map[int]*string
	producer               *Producer
}

func initIoWorker(client ClientInterface, retryQueue *RetryQueue, maxIoWorkerCount int64, errorStatusMap map[int]*string, producer *Producer) *IoWorker {
	return &IoWorker{
		client:                 client,
		retryQueue:             retryQueue,
		taskCount:              0,
		retryQueueShutDownFlag: uberAtomic.NewBool(false),
		maxIoWorker:            make(chan int64, maxIoWorkerCount),
		noRetryStatusCodeMap:   errorStatusMap,
		producer:               producer,
	}
}

func (ioWorker *IoWorker) startSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	atomic.AddInt64(&ioWorker.taskCount, 1)
	ioWorker.maxIoWorker <- 1
	ioWorkerWaitGroup.Add(1)
}

func (ioWorker *IoWorker) closeSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	<-ioWorker.maxIoWorker
	atomic.AddInt64(&ioWorker.taskCount, -1)
	ioWorkerWaitGroup.Done()
}

func (ioWorker *IoWorker) sendToServer(producerBatch *Batch) {
	beginMs := GetTimeMs(time.Now().UnixNano())
	err := ioWorker.client.PutLogs(producerBatch.getGroupId(), producerBatch.getStreamId(), producerBatch.logGroup)
	if err == nil {
		if producerBatch.attemptCount < producerBatch.maxReservedAttempts {
			nowMs := GetTimeMs(time.Now().UnixNano())
			attempt := createAttempt(true, "", "", "", nowMs, nowMs-beginMs, 200)
			producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
		}
		producerBatch.result.successful = true
		// After successful delivery, producer removes the batch size sent out
		atomic.AddInt64(&ioWorker.producer.producerLogGroupSize, -producerBatch.totalDataSize)
		if len(producerBatch.callBackList) > 0 {
			for _, callBack := range producerBatch.callBackList {
				callBack.Success(producerBatch.result)
			}
		}
	} else {
		if ioWorker.retryQueueShutDownFlag.Load() {
			if len(producerBatch.callBackList) > 0 {
				for _, callBack := range producerBatch.callBackList {
					ioWorker.addErrorMessageToBatchAttempt(producerBatch, err, false, beginMs)
					callBack.Fail(producerBatch.result)
				}
			}
			return
		}
		if slsError, ok := err.(*Error); ok {
			if _, ok := ioWorker.noRetryStatusCodeMap[int(slsError.HTTPCode)]; ok {
				ioWorker.addErrorMessageToBatchAttempt(producerBatch, err, false, beginMs)
				ioWorker.excuteFailedCallback(producerBatch)
				return
			}
		}
		if producerBatch.attemptCount < producerBatch.maxRetryTimes {
			ioWorker.addErrorMessageToBatchAttempt(producerBatch, err, true, beginMs)
			retryWaitTime := producerBatch.baseRetryBackoffMs * int64(math.Pow(2, float64(producerBatch.attemptCount)-1))
			if retryWaitTime < producerBatch.maxRetryIntervalInMs {
				producerBatch.nextRetryMs = GetTimeMs(time.Now().UnixNano()) + retryWaitTime
			} else {
				producerBatch.nextRetryMs = GetTimeMs(time.Now().UnixNano()) + producerBatch.maxRetryIntervalInMs
			}
			ioWorker.retryQueue.sendToRetryQueue(producerBatch)
		} else {
			ioWorker.excuteFailedCallback(producerBatch)
		}
	}
}

func (ioWorker *IoWorker) addErrorMessageToBatchAttempt(producerBatch *Batch, err error, retryInfo bool, beginMs int64) {
	if producerBatch.attemptCount < producerBatch.maxReservedAttempts {
		slsError, ok := err.(*Error)
		if !ok {
		}
		if retryInfo {
		}
		nowMs := GetTimeMs(time.Now().UnixNano())
		attempt := createAttempt(false, slsError.RequestID, slsError.Code, slsError.Message, nowMs, nowMs-beginMs, slsError.HTTPCode)
		producerBatch.result.attemptList = append(producerBatch.result.attemptList, attempt)
	}
	producerBatch.result.successful = false
	producerBatch.attemptCount += 1
}

func (ioWorker *IoWorker) excuteFailedCallback(producerBatch *Batch) {
	atomic.AddInt64(&ioWorker.producer.producerLogGroupSize, -producerBatch.totalDataSize)
	if len(producerBatch.callBackList) > 0 {
		for _, callBack := range producerBatch.callBackList {
			callBack.Fail(producerBatch.result)
		}
	}
}

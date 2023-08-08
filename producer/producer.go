package producer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TimeoutExecption = "TimeoutExecption"
)

type Producer struct {
	producerConfig        *Config
	logAccumulator        *LogAccumulator
	mover                 *Mover
	threadPool            *IoThreadPool
	moverWaitGroup        *sync.WaitGroup
	ioWorkerWaitGroup     *sync.WaitGroup
	ioThreadPoolWaitGroup *sync.WaitGroup
	buckets               int
	producerLogGroupSize  int64
}

func InitProducer(producerConfig *Config) *Producer {
	client := CreateNormalInterface(producerConfig)

	finalProducerConfig := validateProducerConfig(producerConfig)
	retryQueue := initRetryQueue()
	errorStatusMap := func() map[int]*string {
		errorCodeMap := map[int]*string{}
		for _, v := range producerConfig.NoRetryCodeList {
			errorCodeMap[int(v)] = nil
		}
		return errorCodeMap
	}()
	producer := &Producer{
		producerConfig: finalProducerConfig,
		buckets:        finalProducerConfig.Buckets,
	}
	ioWorker := initIoWorker(client, retryQueue, finalProducerConfig.MaxIoWorkers, errorStatusMap, producer)
	threadPool := initIoThreadPool(ioWorker)
	logAccumulator := initLogAccumulator(finalProducerConfig, ioWorker, threadPool, producer)
	mover := initMover(logAccumulator, retryQueue, ioWorker, threadPool)

	producer.logAccumulator = logAccumulator
	producer.mover = mover
	producer.threadPool = threadPool
	producer.moverWaitGroup = &sync.WaitGroup{}
	producer.ioWorkerWaitGroup = &sync.WaitGroup{}
	producer.ioThreadPoolWaitGroup = &sync.WaitGroup{}
	return producer
}

func validateProducerConfig(producerConfig *Config) *Config {
	if producerConfig.MaxReservedAttempts <= 0 {
		producerConfig.MaxReservedAttempts = 11
	}
	if producerConfig.MaxBatchCount > 40960 || producerConfig.MaxBatchCount <= 0 {
		producerConfig.MaxBatchCount = 40960
	}
	if producerConfig.MaxBatchSize > 512*1024 || producerConfig.MaxBatchSize <= 0 {
		producerConfig.MaxBatchSize = 512 * 1024
	}
	if producerConfig.MaxIoWorkers <= 0 {
		producerConfig.MaxIoWorkers = 10
	}
	if producerConfig.BaseRetryBackoffMs <= 0 {
		producerConfig.BaseRetryBackoffMs = 100
	}
	if producerConfig.TotalSizeLnBytes <= 0 {
		producerConfig.TotalSizeLnBytes = 100 * 1024 * 1024
	}
	if producerConfig.LingerMs < 100 {
		producerConfig.LingerMs = 2000
	}
	return producerConfig
}

func (producer *Producer) Start() {
	producer.moverWaitGroup.Add(1)
	go producer.mover.run(producer.moverWaitGroup, producer.producerConfig)
	producer.ioThreadPoolWaitGroup.Add(1)
	go producer.threadPool.start(producer.ioWorkerWaitGroup, producer.ioThreadPoolWaitGroup)
}

func (producer *Producer) Close(timeoutMs int64) error {
	startCloseTime := time.Now()
	producer.sendCloseProdcerSignal()
	producer.moverWaitGroup.Wait()
	producer.threadPool.threadPoolShutDownFlag.Store(true)
	for {
		if atomic.LoadInt64(&producer.mover.ioWorker.taskCount) == 0 && !producer.threadPool.hasTask() {
			return nil
		}
		if time.Since(startCloseTime) > time.Duration(timeoutMs)*time.Millisecond {
			return errors.New(TimeoutExecption)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (producer *Producer) sendCloseProdcerSignal() {
	producer.mover.moverShutDownFlag.Store(true)
	producer.logAccumulator.shutDownFlag.Store(true)
	producer.mover.ioWorker.retryQueueShutDownFlag.Store(true)
}

func (producer *Producer) SendLog(groupId, streamId string, log *Log) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	return producer.logAccumulator.addLogToProducerBatch(groupId, streamId, log, nil)
}

func (producer *Producer) waitTime() error {
	//最大阻塞时间
	if producer.producerConfig.MaxBlockSec > 0 {
		for i := 0; i < producer.producerConfig.MaxBlockSec; i++ {
			// 每秒检查producer和配置的日志总大小
			if atomic.LoadInt64(&producer.producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
				time.Sleep(1 * time.Second)
			} else {
				return nil
			}
		}
		//
		return errors.New(TimeoutExecption)
	} else if producer.producerConfig.MaxBlockSec == 0 {
		// 不等待
		if atomic.LoadInt64(&producer.producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
			return errors.New(TimeoutExecption)
		}
	} else if producer.producerConfig.MaxBlockSec < 0 {
		for {
			// 阻塞写入
			if atomic.LoadInt64(&producer.producerLogGroupSize) > producer.producerConfig.TotalSizeLnBytes {
				time.Sleep(time.Second)
			} else {
				return nil
			}
		}
	}
	return nil
}

func (producer *Producer) SendLogWithCallBack(groupId, streamId string, log *Log, callback CallBack) error {
	err := producer.waitTime()
	if err != nil {
		return err
	}
	return producer.logAccumulator.addLogToProducerBatch(groupId, streamId, log, callback)
}

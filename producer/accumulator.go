package producer

import (
	"errors"
	uberAtomic "go.uber.org/atomic"
	"strings"
	"sync"
	"sync/atomic"
)

const Delimiter = "|"

type LogAccumulator struct {
	lock           sync.RWMutex
	logGroupData   map[string]*Batch
	producerConfig *Config
	ioWorker       *IoWorker
	shutDownFlag   *uberAtomic.Bool
	threadPool     *IoThreadPool
	producer       *Producer
}

func initLogAccumulator(config *Config, ioWorker *IoWorker, threadPool *IoThreadPool, producer *Producer) *LogAccumulator {
	return &LogAccumulator{
		logGroupData:   make(map[string]*Batch),
		producerConfig: config,
		ioWorker:       ioWorker,
		shutDownFlag:   uberAtomic.NewBool(false),
		threadPool:     threadPool,
		producer:       producer,
	}
}

func (logAccumulator *LogAccumulator) addLogToProducerBatch(groupId, streamId string,
	logData interface{}, callback CallBack) error {
	if logAccumulator.shutDownFlag.Load() {
		return errors.New("Producer has started and shut down and cannot write to new logs")
	}

	key := logAccumulator.getKeyString(groupId, streamId)
	defer logAccumulator.lock.Unlock()
	logAccumulator.lock.Lock()
	if mlog, ok := logData.(*Log); ok {
		if producerBatch, ok := logAccumulator.logGroupData[key]; ok == true {
			logSize := int64(GetLogSizeCalculate(mlog))
			atomic.AddInt64(&producerBatch.totalDataSize, logSize)
			atomic.AddInt64(&logAccumulator.producer.producerLogGroupSize, logSize)
			logAccumulator.addOrSendProducerBatch(key, groupId, streamId, producerBatch, mlog, callback)
		} else {
			logAccumulator.createNewProducerBatch(mlog, callback, key, groupId, streamId)
		}
	} else if logList, ok := logData.([]*Log); ok {
		if producerBatch, ok := logAccumulator.logGroupData[key]; ok == true {
			logListSize := int64(GetLogListSize(logList))
			atomic.AddInt64(&producerBatch.totalDataSize, logListSize)
			atomic.AddInt64(&logAccumulator.producer.producerLogGroupSize, logListSize)
			logAccumulator.addOrSendProducerBatch(key, groupId, streamId, producerBatch, logList, callback)

		} else {
			logAccumulator.createNewProducerBatch(logList, callback, key, groupId, streamId)
		}
	} else {
		return errors.New("Invalid logType")
	}
	return nil

}

func (logAccumulator *LogAccumulator) getKeyString(groupId, streamId string) string {
	var key strings.Builder
	key.WriteString(groupId)
	key.WriteString(Delimiter)
	key.WriteString(streamId)
	key.WriteString(Delimiter)
	return key.String()
}

func (logAccumulator *LogAccumulator) addOrSendProducerBatch(key, groupId, streamId string, producerBatch *Batch, log interface{}, callback CallBack) {
	totalDataCount := producerBatch.getLogGroupCount() + 1
	if producerBatch.totalDataSize > logAccumulator.producerConfig.MaxBatchSize && producerBatch.totalDataSize < 5242880 && totalDataCount <= logAccumulator.producerConfig.MaxBatchCount {
		producerBatch.addLogToLogGroup(log)
		if callback != nil {
			producerBatch.addProducerBatchCallBack(callback)
		}
		logAccumulator.innerSendToServer(key, producerBatch)
	} else if producerBatch.totalDataSize <= logAccumulator.producerConfig.MaxBatchSize && totalDataCount <= logAccumulator.producerConfig.MaxBatchCount {
		producerBatch.addLogToLogGroup(log)
		if callback != nil {
			producerBatch.addProducerBatchCallBack(callback)
		}
	} else {
		logAccumulator.innerSendToServer(key, producerBatch)
		logAccumulator.createNewProducerBatch(log, callback, key, groupId, streamId)
	}
}

func (logAccumulator *LogAccumulator) createNewProducerBatch(logType interface{}, callback CallBack, key, groupId, streamId string) {
	if mlog, ok := logType.(*Log); ok {
		newProducerBatch := initProducerBatch(mlog, callback, groupId, streamId, logAccumulator.producerConfig)
		logAccumulator.logGroupData[key] = newProducerBatch
	} else if logList, ok := logType.([]*Log); ok {
		newProducerBatch := initProducerBatch(logList, callback, groupId, streamId, logAccumulator.producerConfig)
		logAccumulator.logGroupData[key] = newProducerBatch
	}
}

func (logAccumulator *LogAccumulator) innerSendToServer(key string, producerBatch *Batch) {
	logAccumulator.threadPool.addTask(producerBatch)
	delete(logAccumulator.logGroupData, key)
}

package producer

import (
	"sync"
	"time"
)

type Batch struct {
	totalDataSize        int64
	lock                 sync.RWMutex
	logGroup             *LogGroup
	logGroupSize         int
	logGroupCount        int
	attemptCount         int
	baseRetryBackoffMs   int64
	nextRetryMs          int64
	maxRetryIntervalInMs int64
	callBackList         []CallBack
	createTimeMs         int64
	maxRetryTimes        int
	groupId              string
	streamId             string
	result               *Result
	maxReservedAttempts  int
}

func initProducerBatch(logData interface{}, callBackFunc CallBack, groupId, streamId string, config *Config) *Batch {
	var logs []*Log

	if log, ok := logData.(*Log); ok {
		logs = append(logs, log)
	} else if logList, ok := logData.([]*Log); ok {
		logs = append(logs, logList...)
	}

	logGroup := &LogGroup{
		Logs: logs,
	}
	currentTimeMs := GetTimeMs(time.Now().UnixNano())
	producerBatch := &Batch{
		logGroup:             logGroup,
		attemptCount:         0,
		maxRetryIntervalInMs: config.MaxRetryBackoffMs,
		callBackList:         []CallBack{},
		createTimeMs:         currentTimeMs,
		maxRetryTimes:        config.Retries,
		baseRetryBackoffMs:   config.BaseRetryBackoffMs,
		groupId:              groupId,
		streamId:             streamId,
		result:               initResult(),
		maxReservedAttempts:  config.MaxReservedAttempts,
	}
	producerBatch.totalDataSize = int64(producerBatch.logGroup.Size())

	if callBackFunc != nil {
		producerBatch.callBackList = append(producerBatch.callBackList, callBackFunc)
	}
	return producerBatch
}

func (producerBatch *Batch) getGroupId() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.groupId
}

func (producerBatch *Batch) getStreamId() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.streamId
}

func (producerBatch *Batch) getLogGroupCount() int {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return len(producerBatch.logGroup.GetLogs())
	return 0
}

func (producerBatch *Batch) addLogToLogGroup(log interface{}) {
	defer producerBatch.lock.Unlock()
	producerBatch.lock.Lock()
	if mlog, ok := log.(*Log); ok {
		producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, mlog)
	} else if logList, ok := log.([]*Log); ok {
		producerBatch.logGroup.Logs = append(producerBatch.logGroup.Logs, logList...)
	}
}

func (producerBatch *Batch) addProducerBatchCallBack(callBack CallBack) {
	defer producerBatch.lock.Unlock()
	producerBatch.lock.Lock()
	producerBatch.callBackList = append(producerBatch.callBackList, callBack)
}

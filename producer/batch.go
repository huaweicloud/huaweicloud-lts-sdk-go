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
	logType              int
	structLogs           *StructLogs
}

func initProducerBatch(logData interface{}, callBackFunc CallBack, groupId, streamId string, config *Config) *Batch {
	var logGroup = &LogGroup{}
	var structLogs = &StructLogs{}
	logType := LogTypeNormal
	if log, ok := logData.(*Log); ok {
		var logs []*Log
		logs = append(logs, log)
		logGroup.Logs = logs
	} else if logList, ok := logData.([]*Log); ok {
		var logs []*Log
		logs = append(logs, logList...)
		logGroup.Logs = logs
	} else if structLog, ok := logData.(*StructLog); ok {
		var structLogList []*StructLog
		structLogList = append(structLogList, structLog)
		structLogs.Logs = structLogList
		logType = LogTypeStruct
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
		structLogs:           structLogs,
		logType:              logType,
	}
	if 0 == logType {
		producerBatch.totalDataSize = int64(producerBatch.logGroup.Size())
	} else {
		producerBatch.totalDataSize = int64(producerBatch.structLogs.Size())
	}

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
	} else if structLog, ok := log.(*StructLog); ok {
		producerBatch.structLogs.Logs = append(producerBatch.structLogs.Logs, structLog)
	}
}

func (producerBatch *Batch) addProducerBatchCallBack(callBack CallBack) {
	defer producerBatch.lock.Unlock()
	producerBatch.lock.Lock()
	producerBatch.callBackList = append(producerBatch.callBackList, callBack)
}

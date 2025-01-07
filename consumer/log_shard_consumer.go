package consumer

import (
	"strconv"
	"time"
)

type LogShardConsumer struct {
	ConsumerStatus    string
	shardId           string
	logConsumerClient *LogConsumerClientAdapter
	config            *LogConsumerConfig
	startTime         time.Time
	endTime           time.Time
	lastLogErrorTime  time.Time
	shutDown          bool

	taskResultChannel chan *TaskResult
	taskIsExist       bool
	lastTaskResult    *TaskResult

	fetchDataTaskResultChannel chan *TaskResult
	fetchDataTaskIsExist       bool
	fetchDataTaskIsCancelled   bool

	currentStatus     string
	lastFetchedData   *FetchedLogData
	checkpointTracker *DefaultLogConsumerCheckPointTracker
	processor         ILogConsumerProcessor
	nextFetchCursor   string
	finalFetchCursor  string
}

type FetchedLogData struct {
	shardId     string
	cursor      string
	nextCursor  string
	fetchedData []LogData
}

type ShardCheckPoint struct {
	Shard      string `json:"shard_id"`
	Checkpoint int64  `json:"checkpoint"`
	UpdateTime int64  `json:"update_time"`
	Consumer   string `json:"consumer_name"`
}

type LogData struct {
	Labels map[string]string `json:"labels"`
}

type Cursor struct {
	Cursor string `json:"cursor"`
}

type BatchGetLog struct {
	Count int64     `json:"count"`
	Next  string    `json:"next"`
	Logs  []LogData `json:"logs"`
}

func (w *LogShardConsumer) consume() {
	w.checkAndGenerateNextTask()
	if w.currentStatus == PROCESSING && w.lastFetchedData == nil {
		w.fetchData()
	}
}

func (w *LogShardConsumer) checkAndGenerateNextTask() {
	if !w.taskIsExist || w.lastTaskResult != nil || len(w.taskResultChannel) != 0 {
		taskSuccess := false
		if len(w.taskResultChannel) != 0 {
			result, ok := <-w.taskResultChannel
			if !ok {
				return
			}
			w.lastTaskResult = result
		}
		if w.lastTaskResult != nil {
			if w.lastTaskResult.err == nil {
				taskSuccess = true
				if w.currentStatus == INITIALIZING {
					w.doProcessInitResult(w.lastTaskResult)
				} else {
					w.doProcessTaskResult(w.lastTaskResult)
				}
			}
			w.sampleLogError(w.lastTaskResult)
			w.updateStatus(taskSuccess)
			w.generateNextTask()
		}
	}
}

func (w *LogShardConsumer) generateNextTask() {
	switch w.currentStatus {
	case INITIALIZING:
		go w.InitializeTask()
	case PROCESSING:
		if w.lastFetchedData != nil {
			w.checkpointTracker.currentCursor = w.lastFetchedData.cursor
			w.checkpointTracker.nextCursor = w.lastFetchedData.nextCursor
			go w.ProcessTask(w.lastFetchedData.fetchedData)
			w.taskIsExist = true
			w.lastFetchedData = nil
		}
	case SHUTTING_DOWN:
		w.cancelCurrentFetch()
		go w.ShutDownTask()
	}
}

func (w *LogShardConsumer) doProcessInitResult(result *TaskResult) {
	w.nextFetchCursor = result.startTime
	w.finalFetchCursor = result.endTime
	w.checkpointTracker.initialCursor = w.nextFetchCursor
	if result.cursorPersistent {
		w.checkpointTracker.lastSavedCheckpoint = w.nextFetchCursor
	}
}

func (w *LogShardConsumer) doProcessTaskResult(result *TaskResult) {
	checkpoint := result.rollBackCheckPoint
	if checkpoint != "" {
		w.cancelCurrentFetch()
		w.nextFetchCursor = checkpoint
	}
}

func (w *LogShardConsumer) cancelCurrentFetch() {
	if !w.fetchDataTaskIsExist {
		w.fetchDataTaskIsCancelled = true
		w.fetchDataTaskIsExist = false
	}
	w.lastFetchedData = nil
}

func (w *LogShardConsumer) fetchData() {
	hasError := false
	if w.fetchDataTaskIsExist {
		if w.fetchDataTaskIsCancelled {
			w.fetchDataTaskIsExist = false
			w.lastFetchedData = nil
			return
		} else if len(w.fetchDataTaskResultChannel) == 0 {
			return
		}
		fetchResult, ok := <-w.fetchDataTaskResultChannel
		if !ok {
			return
		}
		if fetchResult.err == nil {
			w.lastFetchedData = &FetchedLogData{
				shardId:     w.shardId,
				fetchedData: fetchResult.fetchData,
				nextCursor:  fetchResult.nextCursor,
				cursor:      fetchResult.cursor,
			}
			w.nextFetchCursor = fetchResult.nextCursor
			w.sampleLogError(fetchResult)
		}
		hasError = fetchResult.err != nil
	}
	if !hasError {
		go w.LogConsumerFetchTask()
		w.fetchDataTaskIsExist = true
	} else {
		w.fetchDataTaskIsExist = false
	}
}

func (w *LogShardConsumer) sampleLogError(result *TaskResult) {
	if result != nil && result.err != nil {
		now := time.Now()
		if now.Sub(w.lastLogErrorTime) > 5*time.Second {
			w.lastLogErrorTime = now
		}
	}
}

func (w *LogShardConsumer) updateStatus(taskSuccess bool) {
	if w.currentStatus == SHUTTING_DOWN {
		if !w.taskIsExist || taskSuccess {
			w.currentStatus = SHUTDOWN_COMPLETE
		}
	} else if w.shutDown {
		w.currentStatus = SHUTTING_DOWN
	} else if taskSuccess {
		if w.currentStatus == INITIALIZING {
			w.currentStatus = PROCESSING
		}
	}
}

func (w *LogShardConsumer) InitializeTask() {
	w.processor.Initialize(w.shardId)
	client := w.logConsumerClient
	startTimeByCursor := ""
	endTimeByCursor := ""
	isCursorPersistent := false
	shardCheckPointList, err := client.fetchConsumerGroup(w.shardId)
	if err != nil {
		w.taskResultChannel <- &TaskResult{
			err: err,
		}
		return
	}
	if !w.endTime.IsZero() {
		cursor, err1 := client.client.getCursorByTime(client.projectId, client.logGroupId, client.logStreamId,
			w.shardId, strconv.FormatInt(w.endTime.UnixNano(), 10))
		if err1 != nil {
			w.taskResultChannel <- &TaskResult{
				err: err1,
			}
			return
		}
		endTimeByCursor = cursor.Cursor
	}
	if shardCheckPointList != nil && len(shardCheckPointList) > 0 {
		shardCheckPoint := shardCheckPointList[0]
		isCursorPersistent = true
		startTimeByCursor = strconv.FormatInt(shardCheckPoint.Checkpoint, 10)
	} else {
		if !w.startTime.IsZero() {
			cursor, err2 := client.client.getCursorByTime(client.projectId, client.logGroupId, client.logStreamId,
				w.shardId, strconv.FormatInt(w.startTime.UnixNano(), 10))
			if err2 != nil {
				w.taskResultChannel <- &TaskResult{
					err: err2,
				}
				return
			}
			startTimeByCursor = cursor.Cursor
		}
	}
	w.taskResultChannel <- &TaskResult{
		startTime:        startTimeByCursor,
		endTime:          endTimeByCursor,
		cursorPersistent: isCursorPersistent,
	}
}

func (w *LogShardConsumer) ProcessTask(fetchedData []LogData) {
	w.checkpointTracker.pendingCheckpoint = w.checkpointTracker.nextCursor
	checkpoint := w.processor.Process(fetchedData, w.checkpointTracker)
	err := w.checkpointTracker.consumeDataEndFlushCheckPoint()
	w.taskResultChannel <- &TaskResult{
		err:                err,
		rollBackCheckPoint: checkpoint,
	}
}

func (w *LogShardConsumer) ShutDownTask() {
	err := w.processor.Shutdown(w.checkpointTracker)
	if flushErr := w.checkpointTracker.flushCheckpoint(); flushErr != nil {
		if err == nil {
			err = flushErr
		}
	}
	w.taskResultChannel <- &TaskResult{
		err: err,
	}
	if err == nil {
		close(w.taskResultChannel)
		close(w.fetchDataTaskResultChannel)
	}
}

func (w *LogShardConsumer) LogConsumerFetchTask() {
	logs, err := w.logConsumerClient.batchGetLogs(w.shardId, strconv.Itoa(w.config.BatchSize), w.nextFetchCursor, w.finalFetchCursor)
	if err != nil {
		w.fetchDataTaskResultChannel <- &TaskResult{
			err: err,
		}
	} else {
		fetchedData := logs.Logs
		nextCursor := logs.Next
		if nextCursor == "" {
			nextCursor = w.nextFetchCursor
		}
		w.fetchDataTaskResultChannel <- &TaskResult{
			cursor:     w.nextFetchCursor,
			nextCursor: nextCursor,
			fetchData:  fetchedData,
		}
	}
}

func (w *LogShardConsumer) shutdown() {
	w.shutDown = true
	if w.currentStatus != SHUTDOWN_COMPLETE {
		w.checkAndGenerateNextTask()
	}
}

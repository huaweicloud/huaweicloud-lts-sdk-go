package consumer

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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

	shutdownLock        sync.Mutex
	shutDownTaskStarted bool

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
	Labels map[string]interface{} `json:"labels"`
}

type Cursor struct {
	Cursor string `json:"cursor"`
}

type BatchGetLog struct {
	Count int64     `json:"count"`
	Next  int64     `json:"next"`
	Logs  []LogData `json:"logs"`
}

func (w *LogShardConsumer) sendTaskResult(result *TaskResult) {
	select {
	case w.taskResultChannel <- result:
	default:
		// channel 已关闭或已满（shutdown 竞态），丢弃该结果，避免 panic: send on closed channel
		slog.Debug("task result channel closed or full, drop result", "shardId", w.shardId)
	}
}

func (w *LogShardConsumer) sendFetchDataResult(result *TaskResult) {
	select {
	case w.fetchDataTaskResultChannel <- result:
	default:
		// channel 已关闭或已满（shutdown 竞态），丢弃该结果，避免 panic: send on closed channel
		slog.Debug("fetch data result channel closed or full, drop result", "shardId", w.shardId)
	}
}

func (w *LogShardConsumer) consume() {
	// 防御性检查：shutdown 后不再发起新任务，避免与 ShutDownTask 并发
	if w.shutDown {
		return
	}
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
			slog.Debug("after get task result", "result", result)
			logrus.WithField("result", result).Debug("after get task result")
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
	slog.Debug("when generate next task", "status", w.currentStatus)
	logrus.WithField("status", w.currentStatus).Debug("when generate next task")
	switch w.currentStatus {
	case INITIALIZING:
		go w.InitializeTask()
	case PROCESSING:
		if w.lastFetchedData != nil {
			//slog.Info("last fetched data is not none")
			logrus.Info("last fetched data is not none")
			w.checkpointTracker.currentCursor = w.lastFetchedData.cursor
			w.checkpointTracker.nextCursor = w.lastFetchedData.nextCursor
			go w.ProcessTask(w.lastFetchedData.fetchedData)
			w.taskIsExist = true
			w.lastFetchedData = nil
		} else {
			slog.Debug("last fetched data is none")
			logrus.Debug("last fetched data is none")
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
		slog.Info("cancel a fetch task", "shardId", w.shardId)
		logrus.WithField("shardId", w.shardId).Info("cancel a fetch task")
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
			slog.Warn("error", "err", result.err)
			logrus.WithError(result.err).Error("sampleLogError error")
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
		slog.Error("error fetching initial position", "err", err)
		logrus.WithError(err).Error("error fetching initial position")
		w.sendTaskResult(&TaskResult{
			err: err,
		})
		return
	}
	if !w.endTime.IsZero() {
		cursor, err1 := client.client.getCursorByTime(client.projectId, client.logGroupId, client.logStreamId,
			w.shardId, strconv.FormatInt(w.endTime.UnixNano(), 10))
		if err1 != nil {
			slog.Error("error fetching initial position, get cursor by time error", "err", err)
			logrus.WithError(err).Error("error fetching initial position, get cursor by time error")
			w.sendTaskResult(&TaskResult{
				err: err1,
			})
			return
		}
		endTimeByCursor = cursor.Cursor
		slog.Debug("initialize task", "shardId", w.shardId, "endTime", w.endTime, "endTimeByCursor", endTimeByCursor)
		logrus.WithField("shardId", w.shardId).WithField("endTime", w.endTime).WithField("endTimeByCursor", endTimeByCursor).Debug("initialize task")
	}
	if shardCheckPointList != nil && len(shardCheckPointList) > 0 {
		shardCheckPoint := shardCheckPointList[0]
		slog.Debug("shard checkpoint", "shard checkpoint", shardCheckPoint)
		logrus.WithField("shard checkpoint", shardCheckPoint).Debug("shard checkpoint")
		isCursorPersistent = true
		startTimeByCursor = strconv.FormatInt(shardCheckPoint.Checkpoint, 10)
	} else {
		if !w.startTime.IsZero() {
			cursor, err2 := client.client.getCursorByTime(client.projectId, client.logGroupId, client.logStreamId,
				w.shardId, strconv.FormatInt(w.startTime.UnixNano(), 10))
			if err2 != nil {
				slog.Error("error fetching initial position, get cursor by time error", "err", err)
				logrus.WithError(err).Error("error fetching initial position, get cursor by time error")
				w.sendTaskResult(&TaskResult{
					err: err2,
				})
				return
			}
			startTimeByCursor = cursor.Cursor
			slog.Debug("initialize task", "shardId", w.shardId, "endTime", w.endTime, "startTimeByCursor", startTimeByCursor)
			logrus.WithField("shardId", w.shardId).WithField("endTime", w.endTime).WithField("startTimeByCursor", startTimeByCursor).Debug("initialize task")
		}
	}

	slog.Debug("initialize task", "shardId", w.shardId, "startTimeByCursor", startTimeByCursor, "endTimeByCursor", endTimeByCursor, "isCursorPersistent", isCursorPersistent)
	logrus.WithField("shardId", w.shardId).WithField("startTimeByCursor", startTimeByCursor).WithField("endTimeByCursor", endTimeByCursor).WithField(endTimeByCursor, "isCursorPersistent").Debug("initialize task")
	w.sendTaskResult(&TaskResult{
		startTime:        startTimeByCursor,
		endTime:          endTimeByCursor,
		cursorPersistent: isCursorPersistent,
	})
}

func (w *LogShardConsumer) ProcessTask(fetchedData []LogData) {
	w.checkpointTracker.pendingCheckpoint = w.checkpointTracker.nextCursor
	checkpoint := w.processor.Process(fetchedData, w.checkpointTracker)
	err := w.checkpointTracker.consumeDataEndFlushCheckPoint()
	w.sendTaskResult(&TaskResult{
		err:                err,
		rollBackCheckPoint: checkpoint,
	})
}

func (w *LogShardConsumer) ShutDownTask() {
	// 幂等保护：rebalance 期间 cleanConsumer 每 tick 都会对失去的 shard 调 shutdown()
	// -> SHUTTING_DOWN -> go ShutDownTask()，不拦截会重复执行用户 processor.Shutdown 回调
	// 并产生无意义 goroutine。
	w.shutdownLock.Lock()
	if w.shutDownTaskStarted {
		w.shutdownLock.Unlock()
		return
	}
	w.shutDownTaskStarted = true
	w.shutdownLock.Unlock()

	err := w.processor.Shutdown(w.checkpointTracker)
	if flushErr := w.checkpointTracker.flushCheckpoint(); flushErr != nil {
		if err == nil {
			err = flushErr
		}
	}
	w.sendTaskResult(&TaskResult{
		err: err,
	})
	// 注意：不再 close 两个 result channel。
	// 关闭 channel 后，仍在运行的 InitializeTask/ProcessTask/LogConsumerFetchTask
	// 向已关闭 channel 发送会触发 panic: send on closed channel（线上崩溃根因）。
	// channel 随 shard consumer 被 cleanConsumer 清理后一起被 GC，无泄漏。
}

func (w *LogShardConsumer) LogConsumerFetchTask() {
	logs, err := w.logConsumerClient.batchGetLogs(w.shardId, strconv.Itoa(w.config.BatchSize), w.nextFetchCursor, w.finalFetchCursor)
	if err != nil {
		slog.Error("batch get logs error", "err", err)
		logrus.WithError(err).Error("batch get logs error")
		w.sendFetchDataResult(&TaskResult{
			err: err,
		})
	} else {
		if len(logs.Logs) != 0 {
			slog.Info("batch get logs success", "logSize", len(logs.Logs))
		}
		logrus.WithField("logSize", len(logs.Logs)).Info("batch get logs success")
		fetchedData := logs.Logs
		nextCursor := logs.Next
		nextCursorStr := strconv.FormatInt(nextCursor, 10)
		if nextCursor == 0 {
			nextCursorStr = w.nextFetchCursor
		}
		w.sendFetchDataResult(&TaskResult{
			cursor:     w.nextFetchCursor,
			nextCursor: nextCursorStr,
			fetchData:  fetchedData,
		})
		slog.Debug("batch get logs send to channel success")
		logrus.Debug("batch get logs send to channel success")
	}
}

func (w *LogShardConsumer) shutdown() {
	w.shutDown = true
	if w.currentStatus != SHUTDOWN_COMPLETE {
		w.checkAndGenerateNextTask()
	}
}

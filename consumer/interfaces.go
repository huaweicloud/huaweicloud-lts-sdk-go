package consumer

type ILogConsumerProcessorFactory interface {
	GeneratorProcessor() ILogConsumerProcessor
}

type ILogConsumerProcessor interface {
	Initialize(shardId string)
	Process(logGroups []LogData, checkPointTracker ILogConsumerCheckPointTracker) string
	Shutdown(checkPointTracker ILogConsumerCheckPointTracker) error
}

type ILogConsumerCheckPointTracker interface {
	SaveCheckPoint(persistent bool) error
	GetCheckPoint() string
	GetCurrentCursor() string
}

type ILogConsumerSTSToken interface {
	// GetSTSTokenConfig SDK会定期从此方法中获取临时AK, 临时SK, 临时securityToken. 如果临时认证信息有变化, 在此方法中实现即可
	GetSTSTokenConfig() STSTokenConfig
}

type DefaultLogConsumerCheckPointTracker struct {
	logConsumerClient   *LogConsumerClientAdapter
	pendingCheckpoint   string
	lastSavedCheckpoint string
	nextCursor          string
	consumer            string
	shardId             string
	currentCursor       string
	initialCursor       string
	heartBeat           *LogConsumerHeartBeat
}

func (tracker *DefaultLogConsumerCheckPointTracker) SaveCheckPoint(persistent bool) error {
	tracker.pendingCheckpoint = tracker.nextCursor
	if persistent {
		return tracker.flushCheckpoint()
	}
	return nil
}

func (tracker *DefaultLogConsumerCheckPointTracker) GetCheckPoint() string {
	if tracker.pendingCheckpoint != "" {
		return tracker.pendingCheckpoint
	} else {
		return tracker.initialCursor
	}
}

func (tracker *DefaultLogConsumerCheckPointTracker) GetCurrentCursor() string {
	return tracker.currentCursor
}

func (tracker *DefaultLogConsumerCheckPointTracker) consumeDataEndFlushCheckPoint() error {
	return tracker.flushCheckpoint()
}

func (tracker *DefaultLogConsumerCheckPointTracker) flushCheckpoint() error {
	tracker.pendingCheckpoint = tracker.nextCursor
	checkpoint := tracker.pendingCheckpoint
	if checkpoint == "" || checkpoint == tracker.lastSavedCheckpoint {
		return nil
	}
	err := tracker.logConsumerClient.updateCheckPoint(tracker.shardId, tracker.consumer, checkpoint)
	if err == nil {
		tracker.lastSavedCheckpoint = checkpoint
	}
	return err
}

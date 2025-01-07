package consumer

import (
	"time"
)

type ClientConsumerWorker struct {
	processorFactory     ILogConsumerProcessorFactory
	logConsumerConfig    *LogConsumerConfig
	logConsumerHeartBeat *LogConsumerHeartBeat
	logConsumerClient    *LogConsumerClientAdapter
	shutDown             bool
	mainLoopExit         bool
	shardConsumer        map[string]*LogShardConsumer
}

func GetClientConsumerWorker(factory ILogConsumerProcessorFactory, config *LogConsumerConfig) *ClientConsumerWorker {
	worker := new(ClientConsumerWorker)
	worker.processorFactory = factory
	worker.logConsumerConfig = config
	worker.logConsumerClient = GetLogConsumerClientAdapter(config)
	worker.shardConsumer = make(map[string]*LogShardConsumer, 0)
	err := worker.checkConsumerGroupExist()
	if err != nil {
		return nil
	}
	worker.logConsumerHeartBeat = GetLogConsumerHeartBeat(worker.logConsumerClient, config)
	return worker
}

func (w *ClientConsumerWorker) checkConsumerGroupExist() error {
	_, err := w.logConsumerClient.fetchConsumerGroup("")
	return err
}

func (w *ClientConsumerWorker) Run() {
	go func() {
		w.logConsumerHeartBeat.start()
		ticker := time.NewTicker(time.Millisecond * time.Duration(w.logConsumerConfig.FetchIntervalMillis))
		for range ticker.C {
			if !w.shutDown {
				w.logConsumerHeartBeat.lock.Lock()
				for shard := range w.logConsumerHeartBeat.currentHeldShards {
					consumer := w.consumerForShard(shard)
					consumer.consume()
				}
				w.cleanConsumer(w.logConsumerHeartBeat.currentHeldShards)
				w.logConsumerHeartBeat.lock.Unlock()
			} else {
				break
			}
		}
		w.mainLoopExit = true
	}()
}

func (w *ClientConsumerWorker) consumerForShard(shardId string) *LogShardConsumer {
	if consumer, ok := w.shardConsumer[shardId]; ok {
		return consumer
	}
	return w.getLogShardConsumer(shardId)
}

func (w *ClientConsumerWorker) getLogShardConsumer(shardId string) *LogShardConsumer {
	consumer := &LogShardConsumer{
		shardId:           shardId,
		logConsumerClient: w.logConsumerClient,
		config:            w.logConsumerConfig,
		startTime:         w.logConsumerConfig.StartTimeNs,
		endTime:           w.logConsumerConfig.EndTimeNs,
		checkpointTracker: &DefaultLogConsumerCheckPointTracker{
			logConsumerClient: w.logConsumerClient,
			consumer:          w.logConsumerConfig.consumer,
			shardId:           shardId,
			heartBeat:         w.logConsumerHeartBeat,
		},
		taskResultChannel:          make(chan *TaskResult, 1),
		fetchDataTaskResultChannel: make(chan *TaskResult, 1),
		processor:                  w.processorFactory.GeneratorProcessor(),
		currentStatus:              INITIALIZING,
		fetchDataTaskIsExist:       false,
	}
	w.shardConsumer[shardId] = consumer
	go consumer.InitializeTask()
	consumer.taskIsExist = true
	return consumer
}

func (w *ClientConsumerWorker) cleanConsumer(ownedShard map[string]string) {
	shardToUnload := make(map[string]string)
	for shardId, consumer := range w.shardConsumer {
		if _, ok := ownedShard[shardId]; ok {
			continue
		}
		consumer.shutdown()
		if consumer.currentStatus == SHUTDOWN_COMPLETE {
			shardToUnload[shardId] = ""
		}
	}
	for shard := range shardToUnload {
		delete(w.shardConsumer, shard)
	}
	for shard := range ownedShard {
		if _, ok := w.shardConsumer[shard]; !ok {
			shardToUnload[shard] = ""
		}
	}
	if len(shardToUnload) != 0 {
		w.logConsumerHeartBeat.unsubscribe(shardToUnload)
	}
}

func (w *ClientConsumerWorker) Shutdown() {
	w.shutDown = true
	times := 0
	for !w.mainLoopExit && times < 20 {
		times++
		time.Sleep(time.Second)
	}
	for _, consumer := range w.shardConsumer {
		consumer.shutdown()
	}
	w.logConsumerHeartBeat.stop()
	w.logConsumerClient.shutDown()
}

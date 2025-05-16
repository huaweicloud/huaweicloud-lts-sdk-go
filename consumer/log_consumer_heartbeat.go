package consumer

import (
	"github.com/sirupsen/logrus"
	"log/slog"
	"sync"
	"time"
)

type LogConsumerHeartBeat struct {
	heartbeatIntervalMillis int64
	client                  *LogConsumerClientAdapter
	allHeartShards          map[string]string
	currentHeldShards       map[string]string
	lastSuccessTime         time.Time
	isStop                  bool
	timeOutSeconds          int32
	lock                    sync.RWMutex
}

func GetLogConsumerHeartBeat(client *LogConsumerClientAdapter, config *LogConsumerConfig) *LogConsumerHeartBeat {
	return &LogConsumerHeartBeat{
		client:                  client,
		heartbeatIntervalMillis: config.heartbeatIntervalMillis,
		timeOutSeconds:          config.timeOutSeconds,
		currentHeldShards:       make(map[string]string, 0),
		allHeartShards:          make(map[string]string, 0),
	}
}

func (heartbeat *LogConsumerHeartBeat) start() {
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(heartbeat.heartbeatIntervalMillis))
		for range ticker.C {
			if heartbeat.isStop {
				return
			}
			shards, err := heartbeat.client.heartBeat(convertAllHeartShards(heartbeat.allHeartShards))
			if err != nil {
				now := time.Now()
				if int64(now.Sub(heartbeat.lastSuccessTime))/1000000 >
					int64(heartbeat.timeOutSeconds*1000)+heartbeat.heartbeatIntervalMillis {
					heartbeat.currentHeldShards = make(map[string]string, 0)
				}
			} else {
				heartbeat.lock.Lock()
				heartbeat.currentHeldShards = make(map[string]string, 0)
				for _, shard := range shards {
					heartbeat.currentHeldShards[shard] = ""
					heartbeat.allHeartShards[shard] = ""
				}
				heartbeat.lock.Unlock()
				heartbeat.lastSuccessTime = time.Now()
				slog.Debug("after heart beat", "currentHeldShards", heartbeat.currentHeldShards, "allHeartShards", heartbeat.allHeartShards, "lastSuccessTime", heartbeat.lastSuccessTime)
				logrus.WithField("currentHeldShards", heartbeat.currentHeldShards).WithField("allHeartShards", heartbeat.allHeartShards).WithField("lastSuccessTime", heartbeat.lastSuccessTime).Debug("after heart beat")
			}
		}
	}()
}

func (heartbeat *LogConsumerHeartBeat) unsubscribe(shards map[string]string) {
	for shard := range shards {
		delete(heartbeat.allHeartShards, shard)
	}
}
func (heartbeat *LogConsumerHeartBeat) stop() {
	heartbeat.isStop = true
}

func convertAllHeartShards(allHeartShards map[string]string) []string {
	shards := make([]string, 0)
	for key, _ := range allHeartShards {
		shards = append(shards, key)
	}
	return shards
}

package consumer

import (
	"github.com/google/uuid"
	"time"
)

const (
	default_fetch_interval_ms  int64   = 200
	default_commit_interval_ms int64   = 60 * 1000
	default_heartbeat_interval int64   = 1000
	default_timeout_sec        int32   = 60
	default_batch_size         int     = 1000
	version                    float32 = 1.0 //sdk版本号
)

type LogConsumerConfig struct {
	RegionName               string
	ProjectId                string
	LogGroupId               string
	LogStreamId              string
	AccessKeyId              string
	AccessKeySecret          string
	ConsumerGroupName        string
	consumer                 string
	SecurityToken            string
	ILogConsumerSTSToken     ILogConsumerSTSToken
	StartTimeNs              time.Time
	EndTimeNs                time.Time
	FetchIntervalMillis      int64
	heartbeatIntervalMillis  int64
	autoCommitIntervalMs     int64
	BatchSize                int
	timeOutSeconds           int32
	autoCommitEnabled        bool
	unloadAfterCommitEnabled bool
}

type STSTokenConfig struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

func GetConsumerConfig() *LogConsumerConfig {
	return &LogConsumerConfig{
		consumer:                 uuid.New().String(),
		FetchIntervalMillis:      default_fetch_interval_ms,
		heartbeatIntervalMillis:  default_heartbeat_interval,
		autoCommitIntervalMs:     default_commit_interval_ms,
		timeOutSeconds:           default_timeout_sec,
		autoCommitEnabled:        true,
		unloadAfterCommitEnabled: false,
		BatchSize:                default_batch_size,
	}
}

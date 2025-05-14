package sample

import (
	"github.com/huaweicloud/huaweicloud-lts-sdk-go/consumer"
	"github.com/huaweicloud/huaweicloud-lts-sdk-go/producer"
	"github.com/sirupsen/logrus"
	"log/slog"
	"time"
)

const (
	// TEST_REGION_NAME 云日志服务的区域
	TEST_REGION_NAME = "TEST_REGION_NAME"

	// TEST_PROJECT 华为云帐号的项目ID（project id）
	TEST_PROJECT = "TEST_PROJECT"

	// TEST_LOG_GROUP_ID LTS的日志组ID
	TEST_LOG_GROUP_ID = "TEST_LOG_GROUP_ID"

	// TEST_LOG_STREAM_ID LTS的日志流ID
	TEST_LOG_STREAM_ID = "TEST_LOG_STREAM_ID"

	// ACCESS_KEY_ID 华为云帐号的AK
	ACCESS_KEY_ID = "ACCESS_KEY_ID"

	// ACCESS_KEY_SECRET 华为云帐号的SK
	ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET"

	// CONSUMER_GROUP_NAME LTS日志流对应的消费组名称, 请您从云日志服务LTS页面创建日志流的消费组
	CONSUMER_GROUP_NAME = "CONSUMER_GROUP_NAME"

	// CONSUMER_COUNT 启动消费者数量,由客户根据自身资源状态决定启动多少消费者
	CONSUMER_COUNT = 1
)

func ConsumeLog(regionName, projectId, logGroupId, logStreamId, ak, sk, consumerGroupName, logLevel, logDest string, consumeCount, batchSize int, startTime, endTime int64) {
	// 消费开始时间 括号中填毫秒值
	var StartTime time.Time
	if startTime != 0 {
		StartTime = time.UnixMilli(startTime)
	}

	// 消费结束时间
	var EndTime time.Time
	if endTime != 0 {
		EndTime = time.UnixMilli(endTime)
	}

	var logConfig producer.LogConf
	if logDest == "file" {
		logConfig = producer.LogConf{
			Dir:     "/opt/clouds",
			Name:    "lts-go-sdk.log",
			Level:   logLevel,
			MaxSize: 100,
		}
		producer.InitLoggerFile(logConfig)
	} else {
		logConfig = producer.LogConf{
			Dir:     "",
			Name:    "",
			Level:   logLevel,
			MaxSize: 100,
		}
		producer.InitLoggerStd(logConfig)
	}
	slog.Info("region is: ", "region", regionName)
	slog.Info("projectId is: ", "region", projectId)
	slog.Info("logGroupId is: ", "logGroupId", logGroupId)
	slog.Info("ak is: ", "ak", ak)
	slog.Info("sk is: ", "sk", sk)
	slog.Info("consumerGroupName is: ", "consumerGroupName", consumerGroupName)
	slog.Info("consumerCount is: ", "consumerCount", consumeCount)
	slog.Info("start time is:", "startTime", StartTime)
	slog.Info("end time:", "endTime", EndTime, "endTime is Zero", EndTime.IsZero())

	workers := make([]*consumer.ClientConsumerWorker, 0)
	for i := 0; i < consumeCount; i++ {
		config := consumer.GetConsumerConfig()
		// 构建消费者配置, 参数有必填的：regionName, projectId, logGroupId, logStreamId, ak, sk, consumerGroupName, startTime
		config.ProjectId = projectId
		config.LogGroupId = logGroupId
		config.LogStreamId = logStreamId
		config.AccessKeyId = ak
		config.AccessKeySecret = sk
		config.BatchSize = batchSize //BatchSize默认值1000
		config.StartTimeNs = StartTime
		config.EndTimeNs = EndTime
		config.ConsumerGroupName = consumerGroupName
		config.RegionName = regionName

		// 构建消费者的工作者
		worker := consumer.GetClientConsumerWorker(new(DemoLogConsumerProcessorFactory), config)
		workers = append(workers, worker)
	}

	for _, work := range workers {
		// 启动消费者, ClientConsumerWorker启动后, 内置的消费任务会自动运行
		work.Run()
	}

	time.Sleep(30 * time.Minute)

	for _, work := range workers {
		// 调用ClientConsumerWorker的shutdown方法, 安全的关闭消费者, 消费者中启动的内置线程也会自动停止
		work.Shutdown()
	}

	// 调用ClientConsumerWorker的shutdown方法后, 由于消费者内置多个异步任务, 建议停止1分钟在关闭整个服务, 目的就是让消费者完成后台的异步任务, 安全的退出
	// 如果消费者突然关闭, 没有调用shutdown方法; 或者调用shutdown方法之后, 没有等待一定的时间. 那么可能造成下次消费时, 会有一定的重复数据, 因为消费者后台的异步任务没有保存checkPoint点
	time.Sleep(time.Minute)
}

type DemoLogConsumerProcessor struct {
	LogCount int
}

// Initialize 这个方法给您回调返回的ShardId, 是告诉您当前这个shard-consumer在消费那个shard
func (processor *DemoLogConsumerProcessor) Initialize(shardId string) {

}

// Process 数据处理方法, logGroups为拉取到的日志
func (processor *DemoLogConsumerProcessor) Process(logGroups []consumer.LogData, checkPointTracker consumer.ILogConsumerCheckPointTracker) string {
	processor.LogCount = processor.LogCount + len(logGroups)
	slog.Info("this time process log", "consume log", len(logGroups), "total log num", processor.LogCount)
	logrus.WithField("consume log", len(logGroups)).WithField("total log num", processor.LogCount).Info("this time process log")
	//for _, logData := range logGroups {
	//	// logData为您的一条日志，日志内容在Labels属性中。
	//	// Labels为一个JSON，存放您的这个条日志的内容，比如: "log_content": "日志内容"
	//	fmt.Println(fmt.Sprintf("日志内容：%v", logData.Labels))
	//}
	// 方法的返回值为一个checkPoint
	// 如果您在处理这批数据的时候, 遇到什么异常或者说想重新获取这一次的数据, 那么 return checkPointTracker.GetCurrentCursor();
	return ""
}

// Shutdown 当调用ClientConsumerWorker的shutdown方法, 会调用此函数, 您可以在此处写一些关闭流程
func (processor *DemoLogConsumerProcessor) Shutdown(checkPointTracker consumer.ILogConsumerCheckPointTracker) error {
	// 关闭前, 立即保存checkPoint
	return checkPointTracker.SaveCheckPoint(true)
}

type DemoLogConsumerProcessorFactory struct {
}

func (processor *DemoLogConsumerProcessorFactory) GeneratorProcessor() consumer.ILogConsumerProcessor {
	demoProcessor := new(DemoLogConsumerProcessor)
	demoProcessor.LogCount = 0
	return demoProcessor
}

type DemoLogConsumerSTSToken struct {
}

// GetSTSTokenConfig SDK会定期从此方法中获取临时AK, 临时SK, 临时securityToken. 如果临时认证信息有变化, 在此方法中实现即可
func (processor *DemoLogConsumerSTSToken) GetSTSTokenConfig() consumer.STSTokenConfig {
	return consumer.STSTokenConfig{
		AccessKeyId:     "",
		AccessKeySecret: "",
		SecurityToken:   "",
	}
}

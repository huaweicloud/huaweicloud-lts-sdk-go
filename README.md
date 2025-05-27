# lts-go-sdk

## 1. 使用前提

- 要使用LTS GO SDK ，您需要拥有云账号以及该账号对应的 Access Key（AK）和 Secret Access Key（SK）。
  请在华为云控制台“我的凭证-访问密钥”页面上创建和查看您的 AK&SK
  。更多信息请查看 [访问密钥](https://support.huaweicloud.com/usermanual-ca/zh-cn_topic_0046606340.html)  。
- 要使用LTS GO SDK 上报日志
    - 您需要确认已在 [华为云控制台](https://console.huaweicloud.com/console/?locale=zh-cn&region=cn-north-4#/home) 开通LTS服务
    - 您需要确认已经在LTS控制台创建日志组和日志流
- 要使用LTS GO SDK 消费日志
    - 您需要确认已经在LTS控制台创建日志组合日志流
    - 您需要确认已经在消费的日志流中创建消费组
- LTS GO SDK 适用于
    - GO 1.22 及以上版本

## 2、使用方法

1. 获取go sdk包

go get github.com/huaweicloud/huaweicloud-lts-sdk-go


2. 引用云日志服务的包

import github.com/huaweicloud/huaweicloud-lts-sdk-go

## 3、参数配置以及代码样例

### 1.日志发送场景

#### 参数配置

| 参数名称               | 描述                                                                                              | 类型     | 是否必填 | 默认值                     |
|--------------------|-------------------------------------------------------------------------------------------------|--------|------|-------------------------|
| ProjectId          | 华为云帐号的项目ID（project id）。                                                                         | String | 必填   |                         |
| AccessKeyId        | 华为云帐号的AK。                                                                                       | String | 必填   |                         |
| AccessKeySecret    | 华为云帐号的SK。                                                                                       | String | 必填   |                         |
| RegionName         | 云日志服务的区域。                                                                                       | String | 必填   |                         |
| Endpoint           | 日志上报的目的地址。                                                                                      | String | 必填   |                         |   |
| totalSizeInBytes   | 单个producer实例能缓存的日志大小上限。                                                                         | int    | 选填   | 100M（100 * 1024 * 1024） |
| maxBlockSec        | 如果 producer 可用空间不足，调用者在 send 方法上的最大阻塞时间，默认为 60 秒。建议为0秒。                                         | int    | 选填   | 0                       |
| maxIoWorkers       | 执行日志发送任务的任务池大小。                                                                                 | int    | 选填   | 10                      |
| maxBatchSize       | 当一个 ProducerBatch 中缓存的日志大小大于等于 batchSizeThresholdInBytes 时，该 batch 将被发送。                        | int    | 选填   | 0.5M（512 * 1024）        |
| maxBatchCount      | 当一个 ProducerBatch 中缓存的日志条数大于等于 batchCountThreshold 时，该 batch 将被发送。                              | int    | 选填   | 4096                    |
| lingerMs           | 一个 ProducerBatch 从创建到可发送的逗留时间。                                                                  | int    | 选填   | 2S                      |
| retries            | 如果某个 ProducerBatch 首次发送失败，能够对其重试的次数，建议为 3 次。如果 retries 小于等于 0，该 ProducerBatch 首次发送失败后将直接进入失败队列。 | int    | 选填   | 10                      |
| baseRetryBackoffMs | 首次重试的退避时间。                                                                                      | long   | 选填   | 0.1S                    |
| maxRetryBackoffMs  | 重试的最大退避时间。                                                                                      | long   | 选填   | 50S                     |

#### 代码样例

```go

import (
    "fmt"
    "github.com/huaweicloud/huaweicloud-lts-sdk-go/producer"
    "sync"
    "time"
)

func ProduceLog(endpoint, ak, sk, region, projectId, logGroup, logStream string) {
    producerConfig := producer.GetConfig()
    producerConfig.Endpoint = endpoint
    producerConfig.AccessKeyID = ak
    producerConfig.AccessKeySecret = sk
    producerConfig.RegionId = region
    producerConfig.ProjectId = projectId
    producerInstance := producer.InitProducer(producerConfig)

    producerInstance.Start()
    wg := sync.WaitGroup{}
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            for j := 0; j < 1000; j++ {
                labels := make(map[string]string)
                labels["keyA"] = "valueA"
                labels["keyB"] = "valueB"
                labels["keyC"] = "valueC"

                customLogs := producer.CustomLog{
                    LogTimeNs: time.Now().UnixNano(),
                    Log:       fmt.Sprintf("content for this test [%d]", j),
                }

                log := producer.GenerateLogWithCustomTime([]producer.CustomLog{customLogs}, labels)
                err := producerInstance.SendLog(logGroup, logStream, log)
                err = producerInstance.SendLog(logGroup, logStream, log)

                // send log with callback，user can deal error by self
                handle := ErrorHandle{}
                err = producerInstance.SendLogWithCallBack(logGroup, logStream, log, handle)
                err = producerInstance.SendLogWithCallBack(logGroup, logStream, log, handle)

                if err != nil {
                    fmt.Println(err)
                }
                var sLog producer.StructLog
                logContent := make(map[string]string)
                logContent["keyA"] = "valueA"
                logContent["keyB"] = "valueB"
                logContent1 := make(map[string]string)
                logContent["keyA1"] = "valueA1"
                logContent["keyB1"] = "valueB1"
                sLog.Contents = append(sLog.Contents, logContent1)

                err = producerInstance.SendLogStruct(logGroup, logStream, &sLog)
                if nil != err {
                    continue
                }

                time.Sleep(100 * time.Microsecond)
            }

            wg.Done()
            fmt.Printf("test func finished\n")
        }()
    }

    wg.Wait()
    fmt.Printf("send all complate ...")
    time.Sleep(10 * 60 * time.Second)
}

type ErrorHandle struct{}

func (ErrorHandle) Success(result *producer.Result) {
    fmt.Printf("send log to lts success, success flag: %v\n", result.IsSuccessful())
}

func (ErrorHandle) Fail(result *producer.Result) {
    fmt.Printf("send log to lts error, success flag: %v requestId: %s, httpcode: %d, errorCode: %s, errorMsg: %s\n",
        result.IsSuccessful(), result.GetRequestId(), result.GetHttpCode(), result.GetErrorCode(), result.GetErrorMessage())
}

```
#### 错误处理

lts-go-sdk提供了日志发送失败时的回调方法，只需要实现CallBack接口的两个方法，就可以自行处理发送失败后的错误，使用方法参见sample下样例

### 2.日志消费场景

#### 参数配置

| 参数名称          | 描述                               | 类型   | 是否必填 | 默认值 |  |
|-------------------|------------------------------------|--------|----------|--------|--|
| ProjectId         | 华为云帐号的项目ID（project id）。 | String | 必填     |        |  |
| AccessKeyId       | 华为云帐号的AK。                   | String | 必填     |        |  |
| AccessKeySecret   | 华为云帐号的SK。                   | String | 必填     |        |  |
| RegionName        | 云日志服务的区域。                 | String | 必填     |        |  |
| consumerGroupName | 消费组名称                         | String | 必填     |        |  |
| LogGroupId        | 日志组ID                           | string | 必填     |        |  |
| LogStreamId       | 日志流ID                           | string | 必填     |        |  |
| startTime         | 消费开始时间（毫秒）               | int    | 必填     |        |  |
| endTime           | 消费结束时间（毫秒）               | int    | 必填     |        |  |
| consumerCount     | 消费者的数量                       | int    | 选填     | 1      |  |
| batchSize         | 一次能拉取最大日志数量             | int    | 选填     | 500    |  |

#### 代码样例

```go
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
    slog.Info("batchSize is: ", "batchSize", batchSize)
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
	atomic.AddInt64(&allShardLogCount, int64(len(logGroups)))
	processor.LogCount = processor.LogCount + len(logGroups)
	slog.Info("this time process log", "consume log", len(logGroups), "total log num", processor.LogCount)
	slog.Info("after this consume", "consume log", len(logGroups), "all shard consume total log num", allShardLogCount)
	logrus.WithField("consume log", len(logGroups)).WithField("total log num", processor.LogCount).Info("this time process log")
	logrus.WithField("consume log", len(logGroups)).WithField("all shard consume total log num", allShardLogCount).Info("after this consume")
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
```

#### 使用约束
1. 使用sdk消费日志前，日志组、日志流以及消费组必须存在；
2. 使用AKSK认证时，当前不支持临时AKSK认证
3. 消费历史日志时，只支持开通公测白名单时间点后的日志
4. 当前只支持消费startTime为某一具体时间点的日志



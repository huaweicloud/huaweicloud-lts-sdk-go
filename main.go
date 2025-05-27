package main

import (
	"flag"
	"github.com/huaweicloud/huaweicloud-lts-sdk-go/sample"
)

var (
	function          = flag.String("function", "consumer", "sdk功能开关")
	regionName        = flag.String("region", "", "云日志服务的区域")
	projectId         = flag.String("projectId", "", "华为云帐号的项目ID")
	logGroupId        = flag.String("groupId", "", "LTS的日志组ID")
	logStreamId       = flag.String("streamId", "", "LTS的日志流ID")
	ak                = flag.String("ak", "", "华为云帐号的AK")
	sk                = flag.String("sk", "", "华为云帐号的SK")
	consumerGroupName = flag.String("consumerName", "", "LTS日志流对应的消费组名称")
	startTime         = flag.Int64("startTime", 0, "消费开始时间")
	endTime           = flag.Int64("endTime", 0, "消费开始时间")
	logLevel          = flag.String("logLevel", "debug", "打印日志的级别")
	logDest           = flag.String("logDest", "file", "sdk日志输出")
	endPoint          = flag.String("endPoint", "endPoint", "云服务地址")
	consumerCount     = flag.Int("consumerCount", 1, "启动消费者数量")
	batchSize         = flag.Int("batchSize", 500, "每次拉取时的batch大小")
)

func main() {
	flag.Parse()
	if *function == "consumer" {
		sample.ConsumeLog(*endPoint, *regionName, *projectId, *logGroupId, *logStreamId, *ak, *sk, *consumerGroupName, *logLevel, *logDest, *consumerCount, *batchSize, *startTime, *endTime)
	} else if *function == "producer" {
		sample.ProduceLog(*endPoint, *ak, *sk, *regionName, *projectId, *logGroupId, *logStreamId)
	}
}

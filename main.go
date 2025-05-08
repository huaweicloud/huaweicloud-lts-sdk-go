package main

import "flag"

var (
	function          = flag.String("function", "consumer", "sdk功能开关")
	regionName        = flag.String("region", "", "云日志服务的区域")
	projectId         = flag.String("projectId", "", "华为云帐号的项目ID")
	logGroupId        = flag.String("groupId", "", "LTS的日志组ID")
	logStreamId       = flag.String("streamId", "", "LTS的日志流ID")
	ak                = flag.String("ak", "", "华为云帐号的AK")
	sk                = flag.String("sk", "", "华为云帐号的SK")
	consumerGroupName = flag.String("consumerName", "", "LTS日志流对应的消费组名称")
	consumerCount     = flag.Int("consumerCount", 1, "启动消费者数量")
	startTime         = flag.Int64("startTime", 0, "消费开始时间")
	endTime           = flag.Int64("endTime", 0, "消费开始时间")
	logLevel          = flag.String("logLevel", "debug", "打印日志的级别")
	logDest           = flag.String("logDest", "file", "sdk日志输出")
)

func main() {
	flag.Parse()

}

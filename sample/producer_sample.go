package main

import (
	"fmt"
	"github.com/huaweicloud/huaweicloud-lts-sdk-go/producer"
	"sync"
	"time"
)

func main() {
	producerConfig := producer.GetConfig()
	producerConfig.Endpoint = "endpoint"
	producerConfig.AccessKeyID = "ak"
	producerConfig.AccessKeySecret = "sk"
	producerConfig.RegionId = "region"
	producerConfig.ProjectId = "pid"
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

				logContent := fmt.Sprintf("content for this test [%d]", j)

				log := producer.GenerateLog([]string{logContent}, labels)
				err := producerInstance.SendLog("groupId", "streamId1", log)
				err = producerInstance.SendLog("groupId", "streamId2", log)
				if err != nil {
					fmt.Println(err)
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

# lts-go-sdk

### 使用方法
go get github.com/huaweicloud/huaweicloud-lts-sdk-go
### 样例
参见sample下样例
### 配置参数
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

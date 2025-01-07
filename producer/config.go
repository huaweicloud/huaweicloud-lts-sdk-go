package producer

import (
	"net/http"
)

type Config struct {
	TotalSizeLnBytes    int64
	MaxIoWorkers        int64
	MaxBlockSec         int
	MaxBatchSize        int64
	MaxBatchCount       int
	LingerMs            int64
	Retries             int
	MaxReservedAttempts int
	BaseRetryBackoffMs  int64
	MaxRetryBackoffMs   int64
	Buckets             int
	Endpoint            string
	AccessKeyID         string
	AccessKeySecret     string
	ProjectId           string
	RegionId            string
	NoRetryCodeList     []int
	HTTPClient          *http.Client
}

func GetConfig() *Config {
	return &Config{
		TotalSizeLnBytes:    100 * 1024 * 1024,
		MaxIoWorkers:        10,
		MaxBlockSec:         60,
		MaxBatchSize:        512 * 1024,
		LingerMs:            2000,
		Retries:             10,
		MaxReservedAttempts: 11,
		BaseRetryBackoffMs:  100,
		MaxRetryBackoffMs:   50 * 1000,
		Buckets:             64,
		MaxBatchCount:       4096,
		NoRetryCodeList:     []int{413, 429},
	}
}

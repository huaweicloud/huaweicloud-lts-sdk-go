package consumer

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

var (
	defaultRequestTimeout = 60 * time.Second
	defaultRetryTimeout   = 90 * time.Second
	defaultHttpClient     = &http.Client{
		Timeout: defaultRequestTimeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
)

type Client struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	RequestTimeOut  time.Duration
	RetryTimeOut    time.Duration
	HTTPClient      *http.Client
	RegionName      string
	ProjectId       string
	Endpoint        string

	accessKeyLock sync.RWMutex
}

type LogConsumerClientAdapter struct {
	client               *Client
	regionName           string
	projectId            string
	logGroupId           string
	logStreamId          string
	consumerGroupName    string
	consumer             string
	ILogConsumerSTSToken ILogConsumerSTSToken
	stsTokenClientCache  sync.Map
	lock                 sync.RWMutex
	isShutdown           bool
}

func GetLogConsumerClientAdapter(config *LogConsumerConfig) *LogConsumerClientAdapter {
	client := &LogConsumerClientAdapter{
		projectId:         config.ProjectId,
		logGroupId:        config.LogGroupId,
		logStreamId:       config.LogStreamId,
		consumerGroupName: config.ConsumerGroupName,
		consumer:          config.consumer,
		regionName:        config.RegionName,
		isShutdown:        false,
	}
	if config.ILogConsumerSTSToken != nil {
		client.ILogConsumerSTSToken = config.ILogConsumerSTSToken
		go func() {
			for true {
				if client.isShutdown {
					return
				}
				stsTokenConfig := client.ILogConsumerSTSToken.GetSTSTokenConfig()
				if value, ok := client.stsTokenClientCache.Load(stsTokenConfig); !ok {
					newClient := &Client{
						ProjectId:       client.projectId,
						RegionName:      config.RegionName,
						AccessKeyID:     stsTokenConfig.AccessKeyId,
						AccessKeySecret: stsTokenConfig.AccessKeySecret,
						SecurityToken:   stsTokenConfig.SecurityToken,
						HTTPClient:      defaultHttpClient,
						Endpoint:        config.EndPoint,
					}
					client.stsTokenClientCache.Store(stsTokenConfig, newClient)
					client.client = newClient
				} else {
					client.client = value.(*Client)
				}
				time.Sleep(time.Minute)
			}
		}()
		time.Sleep(time.Second)
	} else {
		client.client = &Client{
			ProjectId:       client.projectId,
			RegionName:      config.RegionName,
			AccessKeyID:     config.AccessKeyId,
			AccessKeySecret: config.AccessKeySecret,
			SecurityToken:   config.SecurityToken,
			HTTPClient:      defaultHttpClient,
			Endpoint:        config.EndPoint,
		}
	}
	return client
}

func (c *LogConsumerClientAdapter) shutDown() {
	c.isShutdown = true
}

func (c *LogConsumerClientAdapter) heartBeat(allShards []string) ([]string, error) {
	defer c.lock.RUnlock()
	c.lock.RLock()
	return c.client.heartBeat(c.projectId, c.logGroupId, c.logStreamId, c.consumerGroupName, c.consumer, allShards)
}

func (c *LogConsumerClientAdapter) fetchConsumerGroup(shardId string) ([]ShardCheckPoint, error) {
	defer c.lock.RUnlock()
	c.lock.RLock()
	return c.client.fetchConsumerGroup(c.projectId, c.logGroupId, c.logStreamId, c.consumerGroupName, shardId)
}

func (c *LogConsumerClientAdapter) updateCheckPoint(shard string, consumer string, checkPoint string) error {
	defer c.lock.RUnlock()
	c.lock.RLock()
	return c.client.updateCheckPoint(c.projectId, c.logGroupId, c.logStreamId, c.consumerGroupName, shard, consumer, checkPoint)
}

func (c *LogConsumerClientAdapter) batchGetLogs(shardId string, batchSize string, startTime string, endTime string) (*BatchGetLog, error) {
	defer c.lock.RUnlock()
	c.lock.RLock()
	return c.client.batchGetLog(c.projectId, c.logGroupId, c.logStreamId, shardId, batchSize, startTime, endTime)
}

func (c *Client) heartBeat(projectId string, logGroupId string, logStreamId string, consumerGroupName string,
	consumer string, allShards []string) ([]string, error) {

	uri := fmt.Sprintf("/v2/%s/groups/%s/streams/%s/consumer-groups/%s/heartbeat",
		projectId, logGroupId, logStreamId, consumerGroupName)
	queryMap := make(map[string]string)
	queryMap["consumer_name"] = consumer
	url := fmt.Sprintf("https://%s%s", c.Endpoint, uri)
	url = connectQueryString(queryMap, url)
	body, _ := json.Marshal(allShards)
	body, err := c.httpSend("POST", url, body, queryMap)
	if err != nil {
		return nil, err
	}
	responseShards := make([]string, 0)
	if err := json.Unmarshal(body, &responseShards); err != nil {
		slog.Error("heartBeat unmarshal result error", "response", body, "err", err)
		logrus.WithError(err).WithField("response", body).Error("heartBeat unmarshal result error")
		return nil, err
	}
	return responseShards, nil
}

func (c *Client) fetchConsumerGroup(projectId string, logGroupId string, logStreamId string, consumerGroupName string,
	shardId string) ([]ShardCheckPoint, error) {
	uri := fmt.Sprintf("/v2/%s/groups/%s/streams/%s/consumer-groups/%s",
		projectId, logGroupId, logStreamId, consumerGroupName)
	queryMap := make(map[string]string)
	queryMap["shard_id"] = shardId
	url := fmt.Sprintf("https://%s%s", c.Endpoint, uri)
	url = connectQueryString(queryMap, url)
	body, err := c.httpSend("GET", url, *new([]byte), queryMap)
	if err != nil {
		return nil, err
	}
	shardCheckPoints := make([]ShardCheckPoint, 0)
	if err := json.Unmarshal(body, &shardCheckPoints); err != nil {
		slog.Error("fetchConsumerGroup unmarshal result error", "response", body, "err", err)
		logrus.WithError(err).WithField("response", body).Error("fetchConsumerGroup unmarshal result error")
		return nil, err
	}
	return shardCheckPoints, nil
}

func (c *Client) getCursorByTime(projectId string, logGroupId string, logStreamId string, shardId string, time string) (*Cursor, error) {
	uri := fmt.Sprintf("/v2/%s/groups/%s/streams/%s/shards/%s/cursor",
		projectId, logGroupId, logStreamId, shardId)
	queryMap := make(map[string]string)
	queryMap["from"] = time
	url := fmt.Sprintf("https://%s%s", c.Endpoint, uri)
	url = connectQueryString(queryMap, url)
	body, err := c.httpSend("GET", url, *new([]byte), queryMap)
	if err != nil {
		return nil, err
	}
	cursor := new(Cursor)
	if err := json.Unmarshal(body, &cursor); err != nil {
		slog.Error("getCursorByTime unmarshal result error", "response", body, "err", err)
		logrus.WithError(err).WithField("response", body).Error("getCursorByTime unmarshal result error")
		return nil, err
	}
	return cursor, nil
}

func (c *Client) updateCheckPoint(projectId string, logGroupId string, logStreamId string, consumerGroupName string,
	shard string, consumer string, checkPoint string) error {
	uri := fmt.Sprintf("/v2/%s/groups/%s/streams/%s/consumer-groups/%s",
		projectId, logGroupId, logStreamId, consumerGroupName)
	queryMap := make(map[string]string)
	queryMap["consumer_name"] = consumer
	url := fmt.Sprintf("https://%s%s", c.Endpoint, uri)
	url = connectQueryString(queryMap, url)
	requestBody := make([]map[string]string, 0)
	dict := make(map[string]string)
	dict["shard_id"] = shard
	dict["checkpoint"] = checkPoint
	requestBody = append(requestBody, dict)
	body, _ := json.Marshal(requestBody)
	_, err := c.httpSend("POST", url, body, queryMap)
	return err
}

func (c *Client) batchGetLog(projectId string, logGroupId string, logStreamId string, shardId string,
	batchSize string, startTime string, endTime string) (*BatchGetLog, error) {
	uri := fmt.Sprintf("/v2/%s/groups/%s/streams/%s/shards/%s",
		projectId, logGroupId, logStreamId, shardId)
	queryMap := make(map[string]string)
	queryMap["limit"] = batchSize
	queryMap["start"] = startTime
	if endTime != "" {
		queryMap["end"] = endTime
	}
	url := fmt.Sprintf("https://%s%s", c.Endpoint, uri)
	url = connectQueryString(queryMap, url)

	responseBody, err := c.httpSend("GET", url, *new([]byte), queryMap)
	if err != nil {
		return nil, err
	}
	batchGetLog := new(BatchGetLog)
	if err := json.Unmarshal(responseBody, &batchGetLog); err != nil {
		slog.Error("batchGetLog unmarshal result error", "response", responseBody, "err", err)
		logrus.WithError(err).WithField("response", responseBody).Error("batchGetLog unmarshal result error")
		return nil, err
	}
	return batchGetLog, nil
}

func (c *Client) httpSend(method, url string, body []byte, queryMap map[string]string) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		slog.Error("http generate request error", "url", url, "err", err)
		logrus.WithError(err).WithField("url", url).Error("http generate request error")
		return nil, err
	}
	req.Header.Add("content-type", "application/json")
	SignHeaderBasic(req, c.AccessKeyID, c.AccessKeySecret, "lts", c.RegionName, queryMap)
	if len(c.SecurityToken) != 0 {
		req.Header.Add("SecurityToken", c.SecurityToken)
	}
	beginTime := time.Now().UnixMilli()
	resp, err := c.HTTPClient.Do(req)
	endTime := time.Now().UnixMilli()
	if err != nil {
		slog.Error("http send error", "url", url, "err", err)
		logrus.WithError(err).WithField("url", url).Error("http send error")
		return nil, err
	}

	defer resp.Body.Close()
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		slog.Error("http read response body error", "url", url, "err", err)
		logrus.WithError(err).WithField("url", url).Error("http read response body error")
		return nil, err
	}
	slog.Debug("http client end", "url", url, "code", resp.StatusCode, "result", string(responseBody), "cost", endTime-beginTime)
	logrus.WithField("code", resp.StatusCode).WithField("url", url).WithField("result", string(responseBody)).WithField("cost", endTime-beginTime).Debug("http client end")
	return responseBody, err
}

func connectQueryString(queryMap map[string]string, url string) string {
	url += "?"
	length := len(queryMap)
	number := 0
	for key, value := range queryMap {
		number++
		url += key + "=" + value
		if number < length {
			url += "&"
		}
	}
	return url
}

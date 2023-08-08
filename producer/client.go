package producer

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Error struct {
	HTTPCode  int32  `json:"httpCode"`
	Code      string `json:"errorCode"`
	Message   string `json:"errorMessage"`
	RequestID string `json:"requestID"`
}

func (e Error) Error() string {
	return e.String()
}

func (e Error) String() string {
	b, err := json.MarshalIndent(e, "", "    ")
	if err != nil {
		return ""
	}
	return string(b)
}

type Client struct {
	Endpoint        string // IP or hostname of SLS endpoint
	AccessKeyID     string
	AccessKeySecret string
	RequestTimeOut  time.Duration
	RetryTimeOut    time.Duration
	HTTPClient      *http.Client
	Region          string
	ProjectId       string

	accessKeyLock sync.RWMutex
}

type LogProject struct {
	Region          string // region id
	Endpoint        string // IP or hostname
	AccessKeyID     string
	AccessKeySecret string
	baseURL         string
	retryTimeout    time.Duration
	httpClient      *http.Client
	ProjectId       string
}

type ClientInterface interface {
	PutLogs(project, logStore string, lg *LogGroup) (err error)
}

func CreateNormalInterface(config *Config) ClientInterface {
	return &Client{
		Endpoint:        config.Endpoint,
		AccessKeyID:     config.AccessKeyID,
		AccessKeySecret: config.AccessKeySecret,
		Region:          config.RegionId,
		ProjectId:       config.ProjectId,
	}
}

func (c *Client) PutLogs(groupId, streamId string, lg *LogGroup) (err error) {
	ls := convertLogstore(c, groupId, streamId)
	return ls.PutLogs(lg)
}

func convertLogstore(c *Client, groupId, streamId string) *LogStore {
	c.accessKeyLock.RLock()
	proj := convertLocked(c)
	c.accessKeyLock.RUnlock()
	return &LogStore{
		project:            proj,
		putLogCompressType: CompressNone,
		GroupId:            groupId,
		StreamId:           streamId,
	}
}

func convertLocked(c *Client) *LogProject {
	p, err := NewLogProject(c)
	if nil != err {
	}
	p.Region = c.Region
	if c.HTTPClient != nil {
		p.httpClient = c.HTTPClient
	}
	if c.RequestTimeOut != time.Duration(0) {
		p.WithRequestTimeout(c.RequestTimeOut)
	}
	if c.RetryTimeOut != time.Duration(0) {
		p.WithRetryTimeout(c.RetryTimeOut)
	}

	return p
}

func NewLogProject(client *Client) (p *LogProject, err error) {
	p = &LogProject{
		Endpoint:        client.Endpoint,
		AccessKeyID:     client.AccessKeyID,
		AccessKeySecret: client.AccessKeySecret,
		httpClient:      defaultHttpClient,
		retryTimeout:    defaultRetryTimeout,
		Region:          client.Region,
		ProjectId:       client.ProjectId,
	}
	p.parseEndpoint()
	return p, nil
}

func NewClientError(err error) *Error {
	if err == nil {
		return nil
	}
	if clientError, ok := err.(*Error); ok {
		return clientError
	}
	clientError := new(Error)
	clientError.HTTPCode = -1
	clientError.Code = "ClientError"
	clientError.Message = err.Error()
	return clientError
}

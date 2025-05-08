package producer

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	CompressLz4  = iota // 0
	CompressNone        // 1
	CompressGzip

	LogTypeNormal = 0
	LogTypeStruct = 1
)

type LogStore struct {
	project            *LogProject
	putLogCompressType int
	GroupId            string
	StreamId           string
	LogType            int
}

func (s *LogStore) PutLogs(lg interface{}) (err error) {
	if LogTypeNormal == s.LogType {
		if logG, ok := lg.(*LogGroup); ok {
			if len(logG.Logs) == 0 {
				// empty log group
				return nil
			}
		}
	} else {
		if logS, ok := lg.(*StructLogs); ok {
			if len(logS.Logs) == 0 {
				return nil
			}
		}
	}

	body, err := json.Marshal(lg)
	if err != nil {
		return NewClientError(err)
	}

	var out []byte
	var h map[string]string
	var outLen int
	switch s.putLogCompressType {
	case CompressNone:
		// no compress
		out = body
		h = map[string]string{
			"content-type": "application/json",
		}
		outLen = len(out)
	case CompressGzip:
		var zBuf bytes.Buffer
		zw := gzip.NewWriter(&zBuf)
		zw.Write([]byte(body))
		zw.Close()
		out = zBuf.Bytes()
		outLen = len(out)
		h = map[string]string{
			"content-type":     "application/json",
			"X-Access-Type":    "terminal",
			"Content-Encoding": "gzip",
		}
	}

	var uri string
	if LogTypeNormal == s.LogType {
		uri = fmt.Sprintf("/v2/internal/%s/lts/groups/%s/streams/%s/tenant/batch-contents", s.project.ProjectId, s.GroupId, s.StreamId)
	} else {
		uri = fmt.Sprintf("/v3/%s/lts/groups/%s/streams/%s/logs", s.project.ProjectId, s.GroupId, s.StreamId)
	}
	r, err := request(s.project, "POST", uri, h, out[:outLen])
	if err != nil {
		return NewClientError(err)
	}
	defer r.Body.Close()
	body, err = ioutil.ReadAll(r.Body)
	if nil != err {
		return NewClientError(err)
	}
	if r.StatusCode != http.StatusOK {
		err := new(Error)
		if jErr := json.Unmarshal(body, err); jErr != nil {
			return NewBadResponseError(string(body), r.Header, r.StatusCode)
		}
		return err
	}
	return nil
}

func GenerateLogWithCustomTime(customLogs []CustomLog, labels map[string]string) *Log {
	content := []*LogContent{}
	for _, custom := range customLogs {
		content = append(content, &LogContent{
			LogTimeNs: custom.LogTimeNs,
			Log:       custom.Log,
		})
	}

	labelStr := "{}"
	if nil == labels {

	} else {
		labelJson, err := json.Marshal(labels)
		if nil != err {
		} else {
			labelStr = string(labelJson)
		}
	}

	return &Log{
		Contents: content,
		Labels:   labelStr,
	}
}

type CustomLog struct {
	LogTimeNs int64  `json:"log_time_ns"`
	Log       string `json:"log"`
}

func GenerateLog(addLogMap []string, labels map[string]string) *Log {

	content := []*LogContent{}
	for _, value := range addLogMap {
		content = append(content, &LogContent{
			LogTimeNs: time.Now().UnixNano(),
			Log:       value,
		})
	}

	labelStr := "{}"
	if nil == labels {

	} else {
		labelJson, err := json.Marshal(labels)
		if nil != err {
		} else {
			labelStr = string(labelJson)
		}
	}

	return &Log{
		Contents: content,
		Labels:   labelStr,
	}
}

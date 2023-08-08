package producer

import (
	"encoding/json"
	"time"
)

type Attempt struct {
	Success           bool
	HttpCode          int32
	RequestId         string
	ErrorCode         string
	ErrorMessage      string
	TimeStampMs       int64
	LastAttemptCostMs int64
}

type Result struct {
	attemptList []*Attempt
	successful  bool
}

type BadResponseError struct {
	RespBody   string
	RespHeader map[string][]string
	HTTPCode   int
}

func initResult() *Result {
	return &Result{
		attemptList: []*Attempt{},
		successful:  false,
	}
}

func createAttempt(success bool,
	requestId, errorCode, errorMessage string,
	timeStampMs, lastAttemptCostMs int64, httpCode int32) *Attempt {
	return &Attempt{
		Success:           success,
		RequestId:         requestId,
		ErrorCode:         errorCode,
		ErrorMessage:      errorMessage,
		TimeStampMs:       timeStampMs,
		LastAttemptCostMs: lastAttemptCostMs,
		HttpCode:          httpCode,
	}
}

func (e BadResponseError) Error() string {
	return e.String()
}

func (e BadResponseError) String() string {
	b, err := json.MarshalIndent(e, "", "    ")
	if err != nil {
		return ""
	}
	return string(b)
}

func NewBadResponseError(body string, header map[string][]string, httpCode int) *BadResponseError {
	return &BadResponseError{
		RespBody:   body,
		RespHeader: header,
		HTTPCode:   httpCode,
	}
}

func (p *LogProject) WithRetryTimeout(timeout time.Duration) *LogProject {
	p.retryTimeout = timeout
	return p
}

func (result *Result) IsSuccessful() bool {
	return result.successful
}

func (result *Result) GetReservedAttempts() []*Attempt {
	return result.attemptList
}

func (result *Result) GetErrorCode() string {
	if len(result.attemptList) == 0 {
		return ""
	}
	cursor := len(result.attemptList) - 1
	return result.attemptList[cursor].ErrorCode
}

func (result *Result) GetErrorMessage() string {
	if len(result.attemptList) == 0 {
		return ""
	}
	cursor := len(result.attemptList) - 1
	return result.attemptList[cursor].ErrorMessage
}

func (result *Result) GetRequestId() string {
	if len(result.attemptList) == 0 {
		return ""
	}
	cursor := len(result.attemptList) - 1
	return result.attemptList[cursor].RequestId
}

func (result *Result) GetTimeStampMs() int64 {
	if len(result.attemptList) == 0 {
		return 0
	}
	cursor := len(result.attemptList) - 1
	return result.attemptList[cursor].TimeStampMs
}

func (result *Result) GetHttpCode() int32 {
	if len(result.attemptList) == 0 {
		return 0
	}
	cursor := len(result.attemptList) - 1
	return result.attemptList[cursor].HttpCode
}

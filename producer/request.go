package producer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
)

var GlobalDebugLevel = 0
var RetryOnServerErrorEnabled = true
var (
	defaultRequestTimeout = 30 * time.Second
	defaultRetryTimeout   = 90 * time.Second
	defaultHttpClient     = &http.Client{
		Timeout: defaultRequestTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
	}
)

var (
	ipRegex = regexp.MustCompile(ipRegexStr)
)

const (
	httpScheme      = "http://"
	httpsScheme     = "https://"
	ipRegexStr      = `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*`
	RequestIDHeader = "X-Request-Id"
	authServiceName = "lts"
)

type DefaultHttpRequest struct {
	endpoint string
	path     string
	method   string

	queryParams  map[string]interface{}
	pathParams   map[string]string
	headerParams map[string]string
	body         []byte
}

func (p *LogProject) init() {
	if p.retryTimeout == time.Duration(0) {
		if p.httpClient == nil {
			p.httpClient = defaultHttpClient
		}
		p.retryTimeout = defaultRetryTimeout
		p.parseEndpoint()
	}
}

func request(project *LogProject, method, uri string, headers map[string]string,
	body []byte) (*http.Response, error) {

	var r *http.Response
	var slsErr error
	var err error

	project.init()
	ctx, cancel := context.WithTimeout(context.Background(), project.retryTimeout)
	defer cancel()

	err = RetryWithCondition(ctx, backoff.NewExponentialBackOff(), func() (bool, error) {
		r, slsErr = realRequest(ctx, project, method, uri, headers, body)
		return retryWriteErrorCheck(ctx, slsErr)
	})

	if err != nil {
		return r, err
	}
	return r, slsErr
}

func (p *LogProject) WithRequestTimeout(timeout time.Duration) *LogProject {
	if p.httpClient == defaultHttpClient || p.httpClient == nil {
		p.httpClient = &http.Client{
			Timeout: timeout,
		}
	} else {
		p.httpClient.Timeout = timeout
	}
	return p
}

func (p *LogProject) parseEndpoint() {
	scheme := httpScheme // default to http scheme
	host := p.Endpoint

	if strings.HasPrefix(p.Endpoint, httpScheme) {
		scheme = httpScheme
		host = strings.TrimPrefix(p.Endpoint, scheme)
	} else if strings.HasPrefix(p.Endpoint, httpsScheme) {
		scheme = httpsScheme
		host = strings.TrimPrefix(p.Endpoint, scheme)
	}

	if ipRegex.MatchString(host) { // ip format
		// use direct ip proxy
		_, err := url.Parse(fmt.Sprintf("%s%s", scheme, host))
		if nil != err {
		}
		if p.httpClient == nil {
			p.httpClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				Timeout: defaultRequestTimeout,
			}
		} else {
		}

	}
	p.baseURL = fmt.Sprintf("%s%s", scheme, host)
}

func RetryWithCondition(ctx context.Context, b backoff.BackOff, o ConditionOperation) error {
	ticker := backoff.NewTicker(b)
	defer ticker.Stop()
	var err error
	var needRetry bool
	for {
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "stopped retrying err: %v", err)
		default:
			select {
			case _, ok := <-ticker.C:
				if !ok {
					return err
				}
				needRetry, err = o()
				if !needRetry {
					return err
				}
			case <-ctx.Done():
				return errors.Wrapf(ctx.Err(), "stopped retrying err: %v", err)
			}
		}
	}
}

func realRequest(ctx context.Context, project *LogProject, method, uri string, headers map[string]string,
	body []byte) (*http.Response, error) {

	baseURL := project.getBaseURL()

	// Initialize http request
	reader := bytes.NewReader(body)

	// Handle the endpoint
	urlStr := fmt.Sprintf("%s%s", baseURL, uri)
	req, err := http.NewRequest(method, urlStr, reader)
	if err != nil {
		return nil, NewClientError(err)
	}
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	SignHeaderBasic(req, project.AccessKeyID, project.AccessKeySecret, authServiceName, project.Region)

	// Get ready to do request
	resp, err := project.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	// Parse the sls error from body.
	if resp.StatusCode != http.StatusOK {
		err := &Error{}
		err.HTTPCode = (int32)(resp.StatusCode)
		defer resp.Body.Close()
		buf, ioErr := ioutil.ReadAll(resp.Body)
		if ioErr != nil {
			return nil, NewBadResponseError(ioErr.Error(), resp.Header, resp.StatusCode)
		}
		if jErr := json.Unmarshal(buf, err); jErr != nil {
			return nil, NewBadResponseError(string(buf), resp.Header, resp.StatusCode)
		}
		err.RequestID = resp.Header.Get(RequestIDHeader)
		return nil, err
	}
	if IsDebugLevelMatched(5) {
		_, e := httputil.DumpResponse(resp, true)
		if e != nil {
		}
	}
	return resp, nil
}

func retryWriteErrorCheck(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}

	switch e := err.(type) {
	case *Error:
		if RetryOnServerErrorEnabled {
			if e.HTTPCode == 500 || e.HTTPCode == 502 || e.HTTPCode == 503 {
				return true, e
			}
		}
	case *BadResponseError:
		if RetryOnServerErrorEnabled {
			if e.HTTPCode == 500 || e.HTTPCode == 502 || e.HTTPCode == 503 {
				return true, e
			}
		}
	default:
		return false, e
	}

	return false, err
}

type ConditionOperation func() (bool, error)

func (p *LogProject) getBaseURL() string {
	if len(p.baseURL) > 0 {
		return p.baseURL
	}
	p.parseEndpoint()
	return p.baseURL
}

func IsDebugLevelMatched(level int) bool {
	return level <= GlobalDebugLevel
}

type HttpRequestBuilder struct {
	httpRequest *DefaultHttpRequest
}

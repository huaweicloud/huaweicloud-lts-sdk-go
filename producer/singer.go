package producer

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"golang.org/x/crypto/hkdf"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
)

const (
	BasicDateFormat     = "20060102T150405Z"
	HeaderXDate         = "X-Sdk-Date"
	HeaderHost          = "host"
	HeaderAuthorization = "Authorization"
	HeaderContentSha256 = "X-Sdk-Content-Sha256"
	DerivationAlgorithm = "V11-HMAC-SHA256"
	DerivedDateFormat   = "20060102"
	shortTimeFormat     = "20060102"
)

func (httpRequest *DefaultHttpRequest) GetHeaderParams() map[string]string {
	return httpRequest.headerParams
}

func (httpRequest *DefaultHttpRequest) GetBodyToBytes() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if httpRequest.body != nil {
		v := reflect.ValueOf(httpRequest.body)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		if v.Kind() == reflect.String {
			buf.WriteString(v.Interface().(string))
		} else {
			buf.Write(httpRequest.body)
		}
	}

	return buf, nil
}

func StringToSignDerived(canonicalRequest string, info string, t time.Time) (string, error) {
	hash := sha256.New()
	_, err := hash.Write([]byte(canonicalRequest))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n%s\n%s\n%x",
		DerivationAlgorithm, t.UTC().Format(shortTimeFormat), info, hash.Sum(nil)), nil
}

func GetDerivationKey(accessKey string, secretKey string, info string) (string, error) {
	hash := sha256.New
	derivationKeyReader := hkdf.New(hash, []byte(secretKey), []byte(accessKey), []byte(info))
	derivationKey := make([]byte, 32)
	_, err := io.ReadFull(derivationKeyReader, derivationKey)
	return hex.EncodeToString(derivationKey), err
}

func SignStringToSign(stringToSign string, signingKey []byte) (string, error) {
	hm, err := hmacsha256(signingKey, stringToSign)
	return fmt.Sprintf("%x", hm), err
}

func DerivationAuthHeaderValue(signature, accessKey string, info string, signedHeaders []string) string {
	return fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s", DerivationAlgorithm, accessKey, info, strings.Join(signedHeaders, ";"), signature)
}

func HexEncodeSHA256Hash(body []byte) (string, error) {
	hash := sha256.New()
	if body == nil || 0 == len(body) {
		body = []byte("")
	}
	_, err := hash.Write(body)
	return fmt.Sprintf("%x", hash.Sum(nil)), err
}

func authMakeHmac(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	_, err := hash.Write(data)
	if err != nil {
	}
	return hash.Sum(nil)
}

func hmacsha256(key []byte, data string) ([]byte, error) {
	h := hmac.New(sha256.New, key)
	if _, err := h.Write([]byte(data)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (httpRequest *DefaultHttpRequest) GetPath() string {
	return httpRequest.path
}

func (httpRequest *DefaultHttpRequest) GetMethod() string {
	return httpRequest.method
}

func (httpRequest *DefaultHttpRequest) GetQueryParams() map[string]interface{} {
	return httpRequest.queryParams
}

func (httpRequest *DefaultHttpRequest) GetEndpoint() string {
	return httpRequest.endpoint
}

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '_' || c == '-' || c == '~' || c == '.' {
		return false
	}
	return true
}

func escape(s string) string {
	hexCount := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

func SignHeaderBasic(r *http.Request, ak string, sk string, derivedAuthServiceName string, regionId string) (map[string]string, error) {
	if derivedAuthServiceName == "" || regionId == "" {
		return nil, nil
	}

	var err error
	var t time.Time
	var headerParams = make(map[string]string)
	userHeaders := r.Header
	if date, ok := userHeaders[HeaderXDate]; ok {
		t, err = time.Parse(BasicDateFormat, date[0])
		if date[0] == "" || err != nil {
			t = time.Now()
			userHeaders[HeaderXDate][0] = t.UTC().Format(BasicDateFormat)
			headerParams[HeaderXDate] = t.UTC().Format(BasicDateFormat)
		}
	} else {
		t = time.Now()
		userHeaders.Add(HeaderXDate, t.UTC().Format(BasicDateFormat))
		headerParams[HeaderXDate] = t.UTC().Format(BasicDateFormat)
	}
	signedHeaders := SignedHeadersBasic(userHeaders)
	canonicalRequest, err := CanonicalRequestBasic(r, signedHeaders)
	if err != nil {
		return nil, err
	}
	info := t.UTC().Format(DerivedDateFormat) + "/" + regionId + "/" + derivedAuthServiceName
	stringToSign, err := StringToSignDerived(canonicalRequest, info, t)
	if err != nil {
		return nil, err
	}

	derivedSk, err := GetDerivationKey(ak, sk, info)
	if err != nil {
		return nil, err
	}

	signature, err := SignStringToSign(stringToSign, []byte(derivedSk))
	if err != nil {
		return nil, err
	}
	headerParams[HeaderAuthorization] = DerivationAuthHeaderValue(signature, ak, info, signedHeaders)
	userHeaders.Add(HeaderAuthorization, headerParams[HeaderAuthorization])
	return headerParams, nil
}

func CanonicalRequestBasic(r *http.Request, signedHeaders []string) (string, error) {
	var hexEncode string

	userHeaders := r.Header
	if hex, ok := userHeaders[HeaderContentSha256]; ok {
		hexEncode = hex[0]
	} else {
		buffer, err := r.GetBody()
		if err != nil {
			return "", err
		}
		data, err := ioutil.ReadAll(buffer)
		if nil != err {
		}

		hexEncode, err = HexEncodeSHA256Hash(data)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		r.Method,
		CanonicalURIBasic(r),
		CanonicalQueryStringBasic(r),
		CanonicalHeadersBasic(r, signedHeaders),
		strings.Join(signedHeaders, ";"), hexEncode), nil
}

func CanonicalURIBasic(r *http.Request) string {
	pattens := strings.Split(r.URL.Path, "/")

	var uri []string
	for _, v := range pattens {
		uri = append(uri, escape(v))
	}

	urlPath := strings.Join(uri, "/")
	if len(urlPath) == 0 || urlPath[len(urlPath)-1] != '/' {
		urlPath = urlPath + "/"
	}

	return urlPath
}

func CanonicalQueryStringBasic(r *http.Request) string {
	return ""
}

func CanonicalHeadersBasic(r *http.Request, signerHeaders []string) string {
	var a []string
	header := make(map[string][]string)
	userHeaders := r.Header

	for k, v := range userHeaders {
		if _, ok := header[strings.ToLower(k)]; !ok {
			header[strings.ToLower(k)] = make([]string, 0)
		}
		header[strings.ToLower(k)] = append(header[strings.ToLower(k)], v[0])
	}

	for _, key := range signerHeaders {
		value := header[key]
		if strings.EqualFold(key, HeaderHost) {
			if u, err := url.Parse(r.Host); err == nil {
				header[HeaderHost] = []string{u.Host}
			}
		}

		sort.Strings(value)
		for _, v := range value {
			a = append(a, key+":"+strings.TrimSpace(v))
		}
	}

	return fmt.Sprintf("%s\n", strings.Join(a, "\n"))
}

func SignedHeadersBasic(headers map[string][]string) []string {
	var signedHeaders []string
	for key := range headers {
		signedHeaders = append(signedHeaders, strings.ToLower(key))
	}
	sort.Strings(signedHeaders)

	return signedHeaders
}

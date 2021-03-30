package util

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
)

var client = http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

func bodyData(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	resData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return resData, nil
}

func execute(req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return bodyData(resp)
}

func GetRequest(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return execute(req)
}

func PostRequest(urlStr string, data url.Values) ([]byte, error) {
	resp, err := client.PostForm(urlStr, data)
	if err != nil {
		return nil, err
	}

	return bodyData(resp)
}

func PostRequestJSON(url string, obj interface{}) ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	return execute(req)
}

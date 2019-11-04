package utils

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/letterbaby/manzo/logger"
)

func CatchPanic() {
	if x := recover(); x != nil {
		logger.Error("Panic %v", x)
		i := 0
		funcName, file, line, ok := runtime.Caller(i)
		for ok {
			logger.Error("Frame %v:[func:%v,file:%v,line:%v]\n",
				i, runtime.FuncForPC(funcName).Name(), file, line)
			i++
			funcName, file, line, ok = runtime.Caller(i)
		}
	}
}

// 读取文件
func LoadFile(fileName string) ([]byte, error) {
	fi, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func HttpRequest(url string, postStr string,
	headParam map[string]interface{}, method string) (int, []byte, error) {

	request, err := http.NewRequest(method, url, bytes.NewBufferString(postStr))
	if err != nil {
		return 0, nil, err
	}

	//request.Header.Add("Connection", "close")

	client := &http.Client{
		Timeout: time.Second * 5,
	}

	// set https
	idx := strings.Index(url, "https://")
	if idx == 0 {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// set header
	if headParam != nil {
		for k, v := range headParam {
			value := fmt.Sprintf("%v", v)
			request.Header.Set(k, value)
		}
	}

	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()

	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}

	return response.StatusCode, contents, nil
}

func SplitToStringArray(s string, sep string) ([]string, error) {
	ret := make([]string, 0)
	sepd := strings.Split(s, sep)
	for _, v := range sepd {
		v = strings.Trim(v, " ")
		v = strings.Trim(v, "\t")
		ret = append(ret, v)
	}
	return ret, nil
}

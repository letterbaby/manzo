package utils

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/letterbaby/manzo/logger"
)

const (
	DEBUG_CALL = 1
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

func Memset(s unsafe.Pointer, c byte, n uintptr) {
	ptr := uintptr(s)
	var i uintptr
	for i = 0; i < n; i++ {
		pByte := (*byte)(unsafe.Pointer(ptr + i))
		*pByte = c
	}
}

func ASyncWait(f func() bool) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !f() {
			time.Sleep(time.Second * 1)
		}
	}()
	wg.Wait()
}

func DebugCall(f func(), to int64) {
	if DEBUG_CALL == 0 {
		f()
	} else {
		ch := make(chan byte, 1)
		go func() {
			f()
			ch <- 1
		}()

		select {
		case <-ch:
		case <-time.After(time.Second * time.Duration(to)):
			panic("DebugCall")
		}
	}
}

// 多语言字符统计
func GetStrWordLen(str string) int32 {
	var j int32 = 0
	for i := 0; i < len(str); i++ {
		//不等于10开头就一定是一个单字符或者一个多字符
		if (str[i] & 0xc0) != 0x80 {
			j++
		}
	}
	return j
}

// 取携程id,goroutine 55 [running]:
func GetGoroutineId() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// Md5Byte 获取字节数组md5值
func Md5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

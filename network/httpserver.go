package network

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/letterbaby/manzo/logger"
)

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	Release     func()
	Now         time.Time
}

func (self *limitListenerConn) Close() error {

	tt := time.Now().Sub(self.Now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("limitListenerConn:close conn:%v,time:%v", self.Conn, tt)
	}

	self.releaseOnce.Do(self.Release)

	return self.Conn.Close()
}

type limitListener struct {
	net.Listener
	MaxCount int // 最大
	count    int32
}

func (self *limitListener) acquire() {
	for atomic.LoadInt32(&self.count) > int32(self.MaxCount) {
		time.Sleep(time.Second * 1)
	}

	atomic.AddInt32(&self.count, 1)
}

func (self *limitListener) release() {
	atomic.AddInt32(&self.count, -1)
}

func (self *limitListener) Accept() (net.Conn, error) {
	self.acquire()
	c, err := self.Listener.Accept()
	if err != nil {
		self.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, Release: self.release, Now: time.Now()}, nil
}

func HttpLimitListenTimeOut(addr string, max int) error {
	if max <= 0 {
		server := &http.Server{Addr: addr, Handler: nil,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
		server.SetKeepAlivesEnabled(false)
		return server.ListenAndServe()
	} else {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		defer lis.Close()
		lis = &limitListener{Listener: lis, MaxCount: max}
		server := &http.Server{Addr: addr, Handler: nil,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
		server.SetKeepAlivesEnabled(false)
		return server.Serve(lis)
	}
	//panic("???")
}

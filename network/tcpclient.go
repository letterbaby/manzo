package network

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

type TcpClient struct {
	Agent

	addr          *net.TCPAddr
	autoReconnect int32 //自动重连

	OnPing       func()
	OnConnect    func()
	OnData       func(msg *RawMessage) *RawMessage
	OnDisconnect func()

	pingctrl chan bool
}

func NewTcpClient(cfg *Config) *TcpClient {
	client := &TcpClient{}

	client.OnClose = client.Hand_Close
	client.OnMessage = client.Hand_Message
	client.OnStart = client.Hand_Start

	if client.init(cfg) == nil {
		return nil
	}

	return client
}

func (self *TcpClient) init(cfg *Config) *net.TCPAddr {
	self.Init(cfg)

	addr, err := net.ResolveTCPAddr("tcp", self.cfg.ServerAddress)
	if err != nil {
		logger.Error("TcpClient:connect a:%v", self.cfg.ServerAddress)
		return nil
	}

	self.addr = addr
	return addr
}

//???
func (self *TcpClient) Initx(cfg *Config) *net.TCPAddr {
	return self.init(cfg)
}

func (self *TcpClient) Connect(re bool) {
	if re {
		atomic.AddInt32(&self.autoReconnect, 1)
	}

	if !self.directConn() {
		self.reConnect()
	}
}

func (self *TcpClient) directConn() bool {
	// Resolve??
	conn, err := net.DialTCP("tcp", nil, self.addr)
	if err != nil {
		return false
	}

	logger.Info("TcpClient:connect tcp:%v", self.cfg)

	go func() {
		defer utils.CatchPanic()
		//conn.SetNoDelay(false)
		self.Start(conn)
	}()
	return true
}

func (self *TcpClient) reConnect() {

RETRY:
	time.Sleep(5 * time.Second)

	logger.Warning("TcpClient:reconnect tcp:%v,%v", self.Conn, self.cfg.ServerAddress)
	// 等待当前连接全部释放
	for self.IsConnected() {
		time.Sleep(2 * time.Second)
	}

	if atomic.LoadInt32(&self.autoReconnect) <= 0 {
		return
	}

	if !self.directConn() {
		goto RETRY
	}
}

func (self *TcpClient) Hand_Start() {
	// CallBack OnConnect
	if self.OnConnect != nil {
		self.OnConnect()
	}

	if self.OnPing != nil {
		self.pingctrl = make(chan bool, 0)
		go self.ping()
	}
}

func (self *TcpClient) Hand_Close() {
	if self.OnDisconnect != nil {
		self.OnDisconnect()
	}

	if self.OnPing != nil {
		self.pingctrl <- true
	}

	// 去重连
	if atomic.LoadInt32(&self.autoReconnect) > 0 {
		go func() {
			defer utils.CatchPanic()

			self.reConnect()
		}()
	}
}

func (self *TcpClient) Hand_Message(msg *RawMessage) *RawMessage {
	if self.OnData != nil {
		return self.OnData(msg)
	}
	return nil
}

func (self *TcpClient) ping() {
	defer utils.CatchPanic()

	timer := time.NewTicker(10 * time.Second)
	//???
	defer func() {
		timer.Stop()
	}()

	for {
		select {
		case <-timer.C:
			self.OnPing()
		case <-self.pingctrl:
			return
		}
	}
}

func (self *TcpClient) Disconnect() {
	logger.Info("TcpClient:close tcp:%v", self.Conn)
	// 停止重连
	atomic.AddInt32(&self.autoReconnect, -1)
	self.Close()
}

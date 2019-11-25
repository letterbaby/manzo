package network

import (
	"net"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

type TcpClient struct {
	Agent

	addr          *net.TCPAddr
	autoReconnect bool //自动重连

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

func (self *TcpClient) Connect(reconnect bool) {
	self.autoReconnect = reconnect

	self.reconnect()
}

func (self *TcpClient) reconnect() {

RETRY:
	// 等待当前连接全部释放
	for self.IsConnected() {
		time.Sleep(2 * time.Second)
	}

	// Resolve??
	conn, err := net.DialTCP("tcp", nil, self.addr)
	if err != nil {
		time.Sleep(5 * time.Second)
		logger.Warning("TcpClient:reconnect tcp:%v,%v", self.Conn, self.cfg.ServerAddress)
		goto RETRY
	}

	logger.Info("TcpClient:connect tcp:%v", self.cfg)

	go func() {
		defer utils.CatchPanic()
		//conn.SetNoDelay(false)
		self.Start(conn)
	}()
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
	if self.autoReconnect {
		logger.Warning("TcpClient:reconnect tcp:%v,%v", self.Conn, self.cfg.ServerAddress)
		go func() {
			defer utils.CatchPanic()

			self.reconnect()
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
	self.autoReconnect = false
	self.Close()
}

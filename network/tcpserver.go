package network

import (
	"net"
	"sync"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

type Config struct {
	ServerAddress string `json:"serveraddress"` // 服务地址
	MaxConnNum    int    `json:"maxconnnum"`    // 最大连接数

	ReadDeadline  int `json:"-"` // 连接读超时
	WriteDeadline int `json:"-"` // 连接写超时

	Rpm     int `json:"-"` // 流量上限/min
	AsyncMQ int `json:"-"` // 异步ipc队列上限

	Parser IMessage `json:"-"`

	// 接口对象
	//MsgParser func() IMessage          // for message
	Agent func() IAgent `json:"-"` // 回话
}

type TcpServer struct {
	cfg        *Config
	listener   net.Listener
	conns      map[net.Conn]struct{}
	mutexConns sync.Mutex
	wgConns    sync.WaitGroup
	mutexWG    sync.WaitGroup
}

func NewTcpServer(cfg *Config) *TcpServer {
	server := &TcpServer{}

	server.conns = make(map[net.Conn]struct{}, 0)

	listener := server.init(cfg)
	if listener == nil {
		return nil
	}

	return server
}

func (self *TcpServer) init(cfg *Config) net.Listener {
	self.cfg = cfg

	listener, err := net.Listen("tcp", self.cfg.ServerAddress)
	if err != nil {
		logger.Fatal("TcpServer:init listen:%v", err)
		return nil
	}

	if self.cfg.MaxConnNum <= 0 {
		self.cfg.MaxConnNum = 10240
		logger.Warning("TcpServer:init MaxConnNum <= 0 defalut 10240")
	}

	if self.cfg.ReadDeadline <= 0 {
		self.cfg.ReadDeadline = 5 * 60
		logger.Warning("TcpServer:init ReadDeadline <= 0 defalut 5m")
	}
	if self.cfg.WriteDeadline <= 0 {
		self.cfg.WriteDeadline = 10
		logger.Warning("TcpServer:init WriteDeadline <= 0 defalut 10s")
	}

	/*
		if self.cfg.Rpm <= 0 {
			self.cfg.Rpm = 1024
			logger.Warning("TcpServer:init Rpm <= 0 defalut 1024")
		}
	*/
	if self.cfg.AsyncMQ <= 0 {
		self.cfg.AsyncMQ = 10240
		logger.Warning("TcpServer:init AsyncMQ <= 0 defalut 10240")
	}

	self.listener = listener
	return listener
}

func (self *TcpServer) Serve(block bool) {
	if block {
		self.run()
	}

	self.mutexWG.Add(1)
	go self.run()
}

func (self *TcpServer) run() {
	defer utils.CatchPanic()
	defer self.mutexWG.Done()

	for {
		conn, err := self.listener.Accept()
		if err != nil {
			return
		}

		self.mutexConns.Lock()
		if len(self.conns) >= self.cfg.MaxConnNum {
			self.mutexConns.Unlock()
			conn.Close()
			continue
		}
		self.conns[conn] = struct{}{}
		self.mutexConns.Unlock()

		self.wgConns.Add(1)
		go func() {
			defer utils.CatchPanic()

			defer func() {
				self.wgConns.Done()

				self.mutexConns.Lock()
				delete(self.conns, conn)
				self.mutexConns.Unlock()
			}()
			// for agent
			//conn.(*net.TCPConn).SetNoDelay(true)
			self.cfg.Agent().Start(conn)
		}()
	}
}

func (self *TcpServer) Close() {
	self.listener.Close()

	// Listen
	self.mutexWG.Wait()

	self.mutexConns.Lock()
	logger.Info("TcpServer:close conns:%d", len(self.conns))

	for conn := range self.conns {
		conn.Close()
	}
	self.mutexConns.Unlock()

	// Agent
	self.wgConns.Wait()
}

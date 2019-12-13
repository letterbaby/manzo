package network

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

const (
	SESS_KICKED_OUT = 0x1 // 剔除
)

type IAgent interface {
	Start(net.Conn) // 启动
	SendMsg(msg interface{}, to int32)
	Init(cfg *Config)
}

type Agent struct {
	cfg  *Config
	Conn IConn // 当前会话

	in  chan *RawMessage
	out chan interface{} // 返回给客户端的异步消息

	die               chan struct{} // 会话关闭信号
	flag              int32         // 会话标记
	connectTime       time.Time     // 链接建立时间
	packetTime        time.Time     // 当前包的到达时间
	lastPacketTime    time.Time     // 上一个包到达时间
	packetCountOneMin int           // 每分钟的包统计，用于RPM判断

	OnStart   func()
	OnClose   func()
	OnMessage func(msg *RawMessage) *RawMessage

	Authed bool // 认证过了

	status int32 // 当前状态

	disconn chan bool
}

func NewAgent(cfg *Config) IAgent {
	a := &Agent{}
	a.Init(cfg)
	return a
}

func (self *Agent) Init(cfg *Config) {
	self.cfg = cfg

	if self.cfg.ReadDeadline <= 0 {
		self.cfg.ReadDeadline = 5 * 60
		logger.Warning("Agent:init ReadDeadline <= 0 defalut 5m")
	}
	if self.cfg.WriteDeadline <= 0 {
		self.cfg.WriteDeadline = 10
		logger.Warning("Agent:init WriteDeadline <= 0 defalut 10s")
	}

	/*
		if self.cfg.Rpm <= 0 {
			self.cfg.Rpm = 1024
			logger.Warning("TcpServer:init Rpm <= 0 defalut 1024")
		}
	*/
	if self.cfg.AsyncMQ <= 0 {
		self.cfg.AsyncMQ = 10240
		logger.Warning("Agent:init AsyncMQ <= 0 defalut 10240")
	}

	self.out = make(chan interface{}, self.cfg.AsyncMQ)
	self.disconn = make(chan bool, 1)
}

func (self *Agent) SendMsg(msg interface{}, to int32) {
	if to <= 0 {
		to = 1
	}

	select {
	case self.out <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		logger.Warning("Agent:sendMsg conn:%v,msg:%v", self.Conn, msg)
	}
}

func (self *Agent) RecvMsg(msg *RawMessage, to int32) {
	if to <= 0 {
		to = 1
	}

	select {
	case self.in <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		logger.Warning("Agent:RecvMsg conn:%v,msg:%v", self.Conn, msg)
	}
}

func (self *Agent) IsConnected() bool {
	return atomic.LoadInt32(&self.status) > 0
}

// 外部调用
func (self *Agent) Close() bool {
	select {
	case self.disconn <- true:
		return true
	default:
		logger.Warning("Agent:Close conn:%v", self.Conn)
	}
	//close(self.disconn)
	return false
}

// 内部调用
func (self *Agent) SetCloseFlag() {
	self.flag |= SESS_KICKED_OUT
}

func (self *Agent) Start(conn net.Conn) {
	// 取链接配置
	self.Conn = &Conn{}
	self.Conn.Init(self.cfg, conn)

	atomic.AddInt32(&self.status, 1)
	defer func() {
		self.Conn.Close()
		atomic.AddInt32(&self.status, -1)
	}()

	logger.Info("Agent:start conn:%v", self.Conn)
	self.in = make(chan *RawMessage, self.cfg.AsyncMQ)

	defer func() {
		close(self.in)
	}()

	self.die = make(chan struct{})

	if self.OnStart != nil {
		self.OnStart()
	}
	go self.runSend()
	go self.runAgent()

	tc := time.Duration(self.cfg.ReadDeadline) * time.Second
	for {
		//??客户端
		self.Conn.SetReadDeadline(time.Now().Add(tc))

		msg, err := self.Conn.RecvMsg()
		if err != nil {
			logger.Error("Agent:start conn:%v,recvmsg:%v", self.Conn, err)
			return
		}

		select {
		case self.in <- msg:
			//logger.Debug("Agent:start msg:%v,%v", self.Conn, msg)
		case <-self.die:
			logger.Warning("Agent:start conn:%v,die:?", self.Conn)
			return
		}
	}
}

func (self *Agent) runAgent() {
	defer utils.CatchPanic()

	tc := time.NewTimer(time.Minute)
	defer func() {
		close(self.die)

		if self.OnClose != nil {
			self.OnClose()
		}
	}()
	for {
		select {
		case msg, ok := <-self.in:
			if !ok {
				return
			}
			rt := self.route(msg)
			if rt != nil {
				self.SendMsg(rt, 1)
			}
		case <-tc.C:
			// 不认证的连接都干了,调试阶段不开启
			if !self.Authed {
				//self.flag |= SESS_KICKED_OUT
			} else {
				tc.Reset(time.Minute)
				self.timerCheck()
			}

		case <-self.die:
			self.flag |= SESS_KICKED_OUT
		case <-self.disconn:
			self.flag |= SESS_KICKED_OUT
		}

		if self.flag&SESS_KICKED_OUT != 0 {
			return
		}
	}
}

func (self *Agent) route(msg *RawMessage) *RawMessage {
	self.packetCountOneMin++
	self.packetTime = time.Now()
	self.lastPacketTime = self.packetTime

	logger.Debug("Agent:route conn:%v,msg:%v", self.Conn, msg)

	now := time.Now()

	var outmsg *RawMessage
	if self.OnMessage != nil {
		outmsg = self.OnMessage(msg)
	}
	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("Agent:route conn:%v,msg:%v,time:%v", self.Conn, msg, tt)
	}
	return outmsg
}

func (self *Agent) timerCheck() {
	defer func() {
		self.packetCountOneMin = 0
	}()
	if self.packetCountOneMin > self.cfg.Rpm {
		//self.flag |= SESS_KICKED_OUT
		//logger.Warning("Agent:timercheck conn:%v,rpm:%v", self.Conn, self.packetCountOneMin)
	}
}

func (self *Agent) runSend() {
	defer utils.CatchPanic()

	for {
		select {
		case data := <-self.out:
			self.rawSend(data)
		case <-self.die:
			return
		}
	}
}

func (self *Agent) rawSend(msg interface{}) {
	tc := time.Duration(self.cfg.WriteDeadline) * time.Second
	self.Conn.SetWriteDeadline(time.Now().Add(tc))
	err := self.Conn.SendMsg(msg)
	if err != nil {
		logger.Error("Agent:rawSend conn:%v,sendmsg:%v", self.Conn, err)
	}
}

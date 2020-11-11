package network

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

const (
	SESS_KICKED_OUT = 0x1  // 剔除
	TIMER_ACC       = 1000 // 定时器精度
)

type IAgent interface {
	Start(net.Conn) // 启动
	SendMsg(msg interface{}, to int32)
	InnerMsg(msg interface{}, to int32)
	Init(cfg *Config)
	GetGoroutineId() uint64
}

type Agent struct {
	cfg  *Config
	Conn IConn // 当前会话

	in      chan *RawMessage // 收到客户端的消息
	out     chan interface{} // 返回给客户端的异步消息
	inner   chan interface{} // 内部消息队列
	disconn chan bool

	flag int32 // 会话标记
	// sync.Once die1 die2?
	die1 chan struct{} // 会话关闭信号
	die2 chan struct{} // 会话关闭信号
	//Authed            bool          // 认证过了
	started           int32 // 当前状态
	packetCountOneMin int   // 每分钟的包统计，用于RPM判断
	packetMin         int

	OnStart     func()
	OnClose     func(flag int32)
	OnMessage   func(msg *RawMessage) *RawMessage
	OnInnerMsg  func(msg interface{})
	OnTimer     func(d int64)
	OnAuthCheck func() bool // 回调认证检查

	goroutineId uint64 // run线程ID
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

	if self.cfg.Rpm <= 0 {
		self.cfg.Rpm = 1024
		logger.Warning("Agent:init Rpm <= 0 defalut 1024")
	}

	if self.cfg.AsyncMQ <= 0 {
		self.cfg.AsyncMQ = 10240
		logger.Warning("Agent:init AsyncMQ <= 0 defalut 10240")
	}

	self.disconn = make(chan bool, 1)
	self.inner = make(chan interface{}, 1024)
	self.out = make(chan interface{}, self.cfg.AsyncMQ)
	self.in = make(chan *RawMessage, self.cfg.AsyncMQ)
}

func (self *Agent) SendMsg(msg interface{}, to int32) {
	if to < 2 {
		to = 2
	}

	select {
	case self.out <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		logger.Warning("Agent:sendMsg conn:%v,msg:%v", self.Conn, msg)
	}
}

func (self *Agent) InnerMsg(msg interface{}, to int32) {
	if to < 2 {
		to = 2
	}

	select {
	case self.inner <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		logger.Warning("Agent:InnerMsg conn:%v,msg:%v", self.Conn, msg)
	}
}

// 外部调用
func (self *Agent) Close() bool {
	select {
	case self.disconn <- true:
		return true
	default:
	}
	//close(self.disconn)
	return false
}

func (self *Agent) GetGoroutineId() uint64 {
	return self.goroutineId
}

// 内部调用
func (self *Agent) SetCloseFlag() {
	self.flag |= SESS_KICKED_OUT
}

func (self *Agent) IsStarted() bool {
	return atomic.LoadInt32(&self.started) > 0
}

func (self *Agent) Start(conn net.Conn) {
	// 取链接配置
	self.Conn = &Conn{}
	self.Conn.Init(self.cfg, conn)
	logger.Info("Agent:start conn:%v", self.Conn)
	// reconnect
	self.flag = 0

	self.packetCountOneMin = 0
	self.packetMin = 0
	self.started = 0
	self.die1 = make(chan struct{})
	self.die2 = make(chan struct{})

	atomic.AddInt32(&self.started, 1)
	defer func() {
		atomic.AddInt32(&self.started, -1)
	}()

	if self.OnStart != nil {
		self.OnStart()
	}

	defer func() {
		close(self.die1)
	}()

	go self.runSend(self.die2)
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
		case <-self.die2:
			logger.Warning("Agent:start conn:%v,die:?", self.Conn)
			return
		}
	}
}

func (self *Agent) runAgent() {
	defer utils.CatchPanic()

	self.goroutineId = utils.GetGoroutineId()

	tc := time.NewTicker(time.Millisecond * TIMER_ACC)
	defer func() {
		tc.Stop()

		close(self.die2)

		self.Conn.Close()

		if self.OnClose != nil {
			self.OnClose(self.flag)
		}
	}()
	for {
		select {
		case msg, ok := <-self.in:
			if !ok {
				return
			}
			rt := self.handIn(msg)
			if rt != nil {
				self.SendMsg(rt, 1)
			}

		case msg, ok := <-self.inner:
			if !ok {
				return
			}

			self.handInner(msg)
		case <-tc.C:
			if !self.timerCheck() {
				return
			}
		case <-self.disconn:
			self.SetCloseFlag()
			return
		case <-self.die1:
			return
		}

		if self.flag&SESS_KICKED_OUT != 0 {
			return
		}
	}
}

func (self *Agent) handIn(msg *RawMessage) *RawMessage {
	defer utils.CatchPanic()

	self.packetCountOneMin++

	logger.Debug("Agent:handin conn:%v,msg:%v", self.Conn, msg)

	now := time.Now()

	var outmsg *RawMessage
	if self.OnMessage != nil {
		outmsg = self.OnMessage(msg)
	}
	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("Agent:handin conn:%v,msg:%v,time:%v", self.Conn, msg, tt)
	}
	return outmsg
}

func (self *Agent) handInner(msg interface{}) {
	defer utils.CatchPanic()

	logger.Debug("Agent:handinner conn:%v,msg:%v", self.Conn, msg)

	now := time.Now()

	if self.OnInnerMsg != nil {
		self.OnInnerMsg(msg)
	}
	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("Agent:handinner conn:%v,msg:%v,time:%v", self.Conn, msg, tt)
	}
}

func (self *Agent) timerCheck() bool {
	defer utils.CatchPanic()

	if self.OnTimer != nil {
		self.OnTimer(TIMER_ACC)
	}

	self.packetMin = self.packetMin + TIMER_ACC
	if self.packetMin < 60*1000 {
		return true
	}

	authed := self.OnAuthCheck == nil || self.OnAuthCheck()

	// 不认证的连接都干了,调试阶段可以不开启
	if !authed || self.packetCountOneMin > self.cfg.Rpm {
		logger.Warning("Agent:timercheck conn:%v,rpm:%v,athed:%v", self.Conn,
			self.packetCountOneMin, authed)
		return false
	}

	self.packetMin = 0
	self.packetCountOneMin = 0
	return true
}

func (self *Agent) runSend(die chan struct{}) {
	defer utils.CatchPanic()

	for {
		select {
		case data := <-self.out:
			self.rawSend(data)
		case <-die:
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

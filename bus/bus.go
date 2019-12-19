package bus

import (
	"math"
	"sync"
	"time"

	"github.com/letterbaby/manzo/container"
	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/network"
	"github.com/letterbaby/manzo/rand"
	"github.com/letterbaby/manzo/utils"
)

type BusDataCall func(msg *network.RawMessage) *network.RawMessage
type BusMountCall func(id int64, flag int64)

type Config struct {
	SvrId int64 // 服务器ID

	Parser network.IMessage

	BusCfg []*NewSvrInfo

	OnData   BusDataCall
	OnNewBus BusMountCall
}

/*
type IBusClientMgr interface {
	RegBus(clt *BusClient)
	UnRegBus(clt *BusClient)
	OnBusData(msg *network.RawMessage) *network.RawMessage
}
*/

var (
	pingMsg *network.RawMessage
)

func init() {
	msg := &CommonMessage{}
	msg.Code = Cmd_PING
	pingMsg = NewBusRawMessage(msg)
}

// TODO:路线管理，支持3站或以上路由，最短路径
type BusClient struct {
	sync.RWMutex

	network.TcpClient

	Id     int64
	seqId  uint32
	caller map[uint32]chan *network.RawMessage

	mgr *BusClientMgr
}

func newBusClient(sinfo *NewSvrInfo, mgr *BusClientMgr) *BusClient {
	client := &BusClient{}

	client.Id = sinfo.DestId
	client.mgr = mgr

	cfg := &network.Config{}
	cfg.ServerAddress = sinfo.Ip + ":" + sinfo.Port

	cfg.Parser = mgr.parser
	cfg.ReadDeadline = 9999999

	if client.init(cfg) {
		return nil
	}
	return client
}

func (self *BusClient) init(cfg *network.Config) bool {
	self.OnClose = self.Hand_Close
	self.OnMessage = self.Hand_Message
	self.OnStart = self.Hand_Start

	if self.Initx(cfg) == nil {
		return false
	}

	self.Authed = false
	self.caller = make(map[uint32]chan *network.RawMessage, 0)

	self.OnData = self.OnBusData
	self.OnConnect = func() {
		self.mgr.RegBus(self)
	}

	self.OnDisconnect = func() {
		self.mgr.UnRegBus(self)
	}

	self.OnPing = func() {
		self.SendMsg(pingMsg, 1)
	}

	go func() {
		defer utils.CatchPanic()

		self.Connect(true)
	}()
	return true
}

func (self *BusClient) SetAuthed() {
	self.Lock()
	defer self.Unlock()

	self.Authed = true
}

func (self *BusClient) GetAuthed() bool {
	self.RLock()
	defer self.RUnlock()

	return self.Authed
}

func (self *BusClient) RecvRouteMsg(msg *network.RawMessage) {
	rmsg := NewRouteRawMessageIn(msg, self.mgr.cfg.Parser)

	if msg.Seq != 0 {
		self.callerDone(msg.Seq, rmsg)
		return
	}

	self.mgr.RecvRouteMsg(rmsg)
}

func (self *BusClient) OnBusData(msg *network.RawMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	if msgdata.Code == Cmd_ROUTE_MSG {
		self.RecvRouteMsg(msg)
	} else {
		if msg.Seq == 0 {
			return self.mgr.OnBusData(msg)
		} else {
			self.callerDone(msg.Seq, msg)
		}
	}
	return nil
}

func (self *BusClient) newCaller() (uint32, chan *network.RawMessage) {
	self.Lock()
	defer self.Unlock()

	if self.seqId >= math.MaxUint32 {
		self.seqId = 0
	}

	self.seqId = self.seqId + 1
	w := make(chan *network.RawMessage, 1)
	self.caller[self.seqId] = w
	return self.seqId, w
}

func (self *BusClient) callerDone(seqId uint32, msg *network.RawMessage) {
	self.Lock()
	defer self.Unlock()

	v, ok := self.caller[seqId]
	if !ok {
		logger.Error("BusClient:callerDone conn:%v,seqId:%v", self.Conn, seqId)
		return
	}

	if msg != nil {
		v <- msg
	}

	delete(self.caller, seqId)
}

func (self *BusClient) SendData(msg *network.RawMessage, sync bool, to int32) *network.RawMessage {
	var id uint32
	var w chan *network.RawMessage

	if sync {
		id, w = self.newCaller()

		//!!!!
		msg.Seq = id
	}

	// 发送数据
	self.SendMsg(msg, to)

	if sync {
		if to <= 0 {
			to = 1
		}

		select {
		case rd := <-w:
			return rd
		case <-time.After(time.Second * time.Duration(to)):
			logger.Warning("BusClient:SendData conn:%v,msg:%v", self.Conn, msg)
		}
		// 手动done
		self.callerDone(id, nil)
	}
	return nil
}

//--------------------------------------------------------------------------
type BusClientMgr struct {
	sync.RWMutex

	//rbin int // 轮询
	buss *container.ListMap

	cfg *Config

	parser network.IMessage
}

func NewBusClientMgr(cfg *Config) *BusClientMgr {
	mgr := &BusClientMgr{}
	mgr.init(cfg)
	return mgr
}

func (self *BusClientMgr) init(cfg *Config) {
	parser := network.NewProtocParser()
	parser.Register(uint16(Cmd_NONE), CommonMessage{})
	self.parser = parser

	self.buss = container.NewListMap()

	self.cfg = cfg
	for _, v := range cfg.BusCfg {
		self.NewBusClient(v)
	}
}

func (self *BusClientMgr) NewBusClient(sinfo *NewSvrInfo) {
	self.RLock()
	_, ok, _ := self.buss.Get(sinfo.DestId)
	self.RUnlock()
	if ok {
		logger.Error("BusClientMgr:NewBusClient id:%v", sinfo.DestId)
		return
	}
	newBusClient(sinfo, self)
}

func (self *BusClientMgr) RegBus(clt *BusClient) {
	logger.Info("BusClientMgr:RegBus id:%v", clt.Id)

	self.Lock()
	self.buss.Add(clt.Id, clt)
	self.Unlock()

	// 发送注册消息
	msg := &CommonMessage{}
	msg.Code = Cmd_REG_SVR

	msg.SvrInfo = &RegSvrInfo{}
	msg.SvrInfo.SrcId = self.cfg.SvrId
	msg.SvrInfo.DestId = clt.Id
	rmsg := NewBusRawMessage(msg)
	clt.SendData(rmsg, false, 1)
}

func (self *BusClientMgr) UnRegBus(clt *BusClient) {
	logger.Info("BusClientMgr:UnRegBus id:%v", clt.Id)

	self.Lock()
	self.buss.Del(clt.Id)
	self.Unlock()

	if self.cfg.OnNewBus != nil {
		self.cfg.OnNewBus(clt.Id, 0)
	}
}

func (self *BusClientMgr) busOk(id int64) {
	self.Lock()
	v, ok, _ := self.buss.Get(id)
	self.Unlock()

	if !ok {
		logger.Error("BusClientMgr:BusOk id:%v", id)
		return
	}
	v.(*BusClient).SetAuthed()

	if self.cfg.OnNewBus != nil {
		self.cfg.OnNewBus(id, 1)
	}
}

func (self *BusClientMgr) OnBusData(msg *network.RawMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	if msgdata.Code == Cmd_REG_SVR {
		self.busOk(msgdata.SvrInfo.DestId)
	} else if msgdata.Code == Cmd_NEW_SVR {
		// CHECK wfunc
		self.NewBusClient(msgdata.NewSvrInfo)
	} else if msgdata.Code == Cmd_PING {
	} else {
		logger.Error("BusClientMgr:OnBusData code:%v", msgdata.Code)
	}

	return nil
}

func (self *BusClientMgr) GetBusClientById(svrId int64) []*BusClient {
	self.RLock()
	hs := self.buss.Values()
	self.RUnlock()

	t := make([]*BusClient, 0)

	for _, v := range hs {
		clt := v.(*BusClient)
		// 必须认证过的
		if clt.GetAuthed() && (svrId == 0 || (IsSameWorldFuncId(svrId, clt.Id) &&
			(GetServerLogicId(svrId) == 0 ||
				GetServerLogicId(svrId) == GetServerLogicId(clt.Id)))) {
			t = append(t, clt)
		}
	}
	return t
}

// 轮询\广播\指定
func (self *BusClientMgr) SendData(msg *network.RawMessage,
	sync bool, to int32, svrId int64, all bool) *network.RawMessage {
	t := self.GetBusClientById(svrId)

	if len(t) <= 0 {
		logger.Error("BusClientMgr:SendData svrId:%v", svrId)
		return nil
	}

	var rt *network.RawMessage
	if all {
		for _, v := range t {
			rmsg := msg
			if sync {
				// !!!sync
				rmsg = NewBusRawMessage(msg.MsgData.(*CommonMessage))
			}
			v.SendData(rmsg, sync, to)
		}
	} else {
		rt = t[rand.RandInt(0, int32(len(t)-1))].SendData(msg, sync, to)
	}
	return rt
}

func (self *BusClientMgr) RecvRouteMsg(msg *network.RawMessage) {
	if self.cfg.OnData != nil {
		self.cfg.OnData(msg)
	}
}

// svrId要去哪里
// all是不是要去所有的wfuncId
// destsvr到wfuncId不是要下车还是继续
// sync是不是rpc
// to发送超时
func (self *BusClientMgr) SendRouteMsg(destSvr int64, destAll bool,
	msg *network.RawMessage, sync bool, to int32, svrId int64, all bool) *network.RawMessage {

	rmsg := NewRouteRawMessageOut(destSvr, destAll, msg, self.cfg.Parser)
	if rmsg != nil {
		return self.SendData(rmsg, sync, to, svrId, all)
	}
	return nil
}

//------------------------------------------------------------------------------------

type BusServer struct {
	network.Agent

	Id  int64 // client ID
	Mgr *BusServerMgr

	OnDisconnect func()
	OnData       BusDataCall
}

func (self *BusServer) Initx(cfg *network.Config, mgr *BusServerMgr) {
	self.Mgr = mgr

	ncfg := &network.Config{}
	ncfg.Parser = mgr.parser

	self.Init(ncfg)
}

func (self *BusServer) Hand_Close() {
	logger.Debug("BusServer:onclose conn:%v,cid:%v", self.Conn, self.Id)

	if self.OnDisconnect != nil {
		self.OnDisconnect()
	}

	self.Mgr.DelSvr(self.Id)
}

func (self *BusServer) Hand_Message(msg *network.RawMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	logger.Debug("BusServer:message conn:%v,msg:%v", self.Conn, msg)

	if msgdata.Code == Cmd_ROUTE_MSG {
		self.RecvRouteMsg(msg)
	} else if msgdata.Code == Cmd_REG_SVR {
		self.RegClt(msgdata)
	} else if msgdata.Code == Cmd_PING {
	} else {
		logger.Warning("BusServer:Hand_Message code:%v", msgdata.Code)
	}
	return nil
}

func (self *BusServer) RegClt(msg *CommonMessage) {
	req := msg.SvrInfo

	logger.Debug("BusServer:RegSvr con:%v, id:%v", self.Conn, req.SrcId)

	// 绑定id
	self.Id = req.SrcId
	self.Mgr.AddSvr(req.SrcId, self)

	// 少序列化点数据
	//msg.SvrInfo = nil

	rmsg := NewBusRawMessage(msg)
	self.SendMsg(rmsg, 1)
}

func (self *BusServer) SendRouteMsg(msg *network.RawMessage) {
	rmsg := NewRouteRawMessageOut(0, false, msg, self.Mgr.parser)
	if rmsg != nil {
		self.SendMsg(rmsg, 1)
	}
}

func (self *BusServer) RecvRouteMsg(msg *network.RawMessage) {
	msgdata := msg.MsgData.(*CommonMessage)
	req := msgdata.RouteInfo

	if IsSameWorldFuncId(req.DestSvr, self.Mgr.cfg.SvrId) {
		if GetServerLogicId(req.DestSvr) == 0 ||
			GetServerLogicId(req.DestSvr) == GetServerLogicId(self.Mgr.cfg.SvrId) {
			if self.OnData != nil {
				rmsg := NewRouteRawMessageIn(msg, self.Mgr.cfg.Parser)
				if rmsg != nil {
					self.OnData(rmsg)
				}
			}
		}
		return
	}

	self.Mgr.RecvRouteMsg(msg)
}

//-------------------------------------------------------------------------
type BusServerMgr struct {
	sync.RWMutex

	cfg     *Config
	servers map[int64]*BusServer

	parser network.IMessage
}

func NewBusServerMgr(cfg *Config) *BusServerMgr {
	mgr := &BusServerMgr{}
	mgr.init(cfg)
	return mgr
}

func (self *BusServerMgr) init(cfg *Config) {
	parser := network.NewProtocParser()
	parser.Register(uint16(Cmd_NONE), CommonMessage{})
	self.parser = parser

	self.cfg = cfg
	self.servers = make(map[int64]*BusServer, 0)
}

func (self *BusServerMgr) AddSvr(id int64, svr *BusServer) {
	self.Lock()
	defer self.Unlock()

	_, ok := self.servers[id]
	if ok {
		logger.Warning("BusServerMgr:AddSvr id:%v", id)
	}

	self.servers[id] = svr
}

func (self *BusServerMgr) DelSvr(id int64) {
	self.Lock()
	defer self.Unlock()

	_, ok := self.servers[id]
	if !ok {
		logger.Warning("BusServerMgr:DelSvr id:%v", id)
		return
	}

	delete(self.servers, id)
}

func (self *BusServerMgr) GetServersById(id int64) []*BusServer {
	self.RLock()
	defer self.RUnlock()

	svrs := make([]*BusServer, 0)

	if id == 0 {
		for _, v := range self.servers {
			svrs = append(svrs, v)
		}
		return svrs
	}

	lid := GetServerLogicId(id)
	if lid != 0 {
		v, ok := self.servers[id]
		if ok {
			svrs = append(svrs, v)
		}
		return svrs
	}

	for _, v := range self.servers {
		if IsSameWorldFuncId(id, v.Id) {
			svrs = append(svrs, v)
		}
	}
	return svrs
}

//是不是给自己
func (self *BusServerMgr) RecvRouteMsg(msg *network.RawMessage) {
	msgdata := msg.MsgData.(*CommonMessage)
	req := msgdata.RouteInfo

	self.SendData(req.DestSvr, req.DestAll, msg, 1)
}

func (self *BusServerMgr) NewServer(id int64, ip string, port string) {
	msg := &CommonMessage{}
	msg.Code = Cmd_NEW_SVR
	msg.NewSvrInfo = &NewSvrInfo{}
	msg.NewSvrInfo.DestId = id
	msg.NewSvrInfo.Ip = ip
	msg.NewSvrInfo.Port = port

	rmsg := NewBusRawMessage(msg)

	self.SendData(0, true, rmsg, 1)
}

func (self *BusServerMgr) SendData(svrId int64, all bool,
	msg *network.RawMessage, to int32) {

	// !!!
	if msg.Seq != 0 {
		logger.Error("BusServerMgr:SendData id:%v", msg)
		return
	}

	svrs := self.GetServersById(svrId)

	if len(svrs) <= 0 {
		logger.Error("BusServerMgr:SendData id:%v", svrId)
		return
	}

	buf, err := self.parser.Serialize(msg)
	if err != nil {
		logger.Error("BusServerMgr:SendData id:%v", err)
		return
	}

	if all {
		for _, v := range svrs {
			//增加引用次数
			buf.Ref()
			v.SendMsg(buf, 1)
		}
	} else {
		buf.Ref()
		svrs[rand.RandInt(0, int32(len(svrs)-1))].SendMsg(buf, 1)
	}

	buf.Free()
}

func (self *BusServerMgr) SendRouteMsg(destSvr int64, destAll bool,
	msg *network.RawMessage, to int32, svrId int64, all bool) {

	rmsg := NewRouteRawMessageOut(destSvr, destAll, msg, self.cfg.Parser)
	if rmsg != nil {
		self.SendData(svrId, all, rmsg, 1)
	}
}

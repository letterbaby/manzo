package bus

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/network"
	"github.com/letterbaby/manzo/rand"
	"github.com/letterbaby/manzo/utils"
)

type BusRegCall func(svr *NewSvrInfo, flag int64)
type BusNewCall func(id int64) bool
type BusDataCall func(id int64, msg *network.RawMessage) *network.RawMessage

type Config struct {
	//SvrId int64 // 服务器ID
	SvrInfo *NewSvrInfo

	Parser network.IMessage

	BusCfg []*NewSvrInfo

	OnData   BusDataCall
	OnNewBus BusNewCall // 新bus是否可以接入
	OnBusReg BusRegCall
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
	Id int64

	sync.RWMutex

	network.TcpClient

	seqId  uint32
	callc  chan bool //dis
	caller map[uint32]chan *network.RawMessage

	mgr *BusClientMgr

	dest *NewSvrInfo

	Authed int32

	/*不走bus统一管理，维护跟上层业务关系很大，
	不能统一标志,有好的想法可以提出
	*/
	Maintance int32
}

func newBusClient(sinfo *NewSvrInfo, mgr *BusClientMgr) *BusClient {
	client := &BusClient{}

	client.Id = sinfo.Id
	client.mgr = mgr
	client.dest = sinfo

	cfg := &network.Config{}
	cfg.ServerAddress = sinfo.Ip + ":" + sinfo.Port

	cfg.Parser = mgr.parser
	cfg.ReadDeadline = 9999999
	cfg.Rpm = 9999999

	if !client.init(cfg) {
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

	self.OnAuthCheck = self.GetAuthed

	self.caller = make(map[uint32]chan *network.RawMessage, 0)

	self.OnData = self.OnBusData
	self.OnConnect = func() {
		// 断线重连
		self.SetMaintance(false)
		self.callc = make(chan bool, 1)
		self.mgr.OnMount(self)
	}

	self.OnDisconnect = func() {
		self.mgr.UnRegBus(self)
		close(self.callc)
		self.SetMaintance(true)
	}

	self.OnPing = func() {
		self.SendMsg(pingMsg, 1)
	}

	return true
}

func (self *BusClient) SetAuthed(a bool) {
	if a {
		atomic.StoreInt32(&self.Authed, 1)
	} else {
		atomic.StoreInt32(&self.Authed, 0)
	}
}

func (self *BusClient) GetAuthed() bool {
	return atomic.LoadInt32(&self.Authed) > 0
}

func (self *BusClient) SetMaintance(a bool) {
	if a {
		atomic.StoreInt32(&self.Maintance, 1)
	} else {
		atomic.StoreInt32(&self.Maintance, 0)
	}
}

func (self *BusClient) IsMaintance() bool {
	return atomic.LoadInt32(&self.Maintance) > 0
}

func (self *BusClient) RecvRouteMsg(msg *network.RawMessage) {
	rmsg := NewRouteRawMessageIn(msg, self.mgr.cfg.Parser)

	if msg.Seq == 0 {
		self.mgr.RecvRouteMsg(self.Id, rmsg)
		return
	}

	self.callerDone(msg.Seq, rmsg)
}

func (self *BusClient) OnBusData(msg *network.RawMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	if msgdata.Code == Cmd_ROUTE_MSG {
		self.RecvRouteMsg(msg)
	} else {
		if msg.Seq == 0 {
			return self.mgr.OnBusData(msg)
		}

		self.callerDone(msg.Seq, msg)
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
		case <-self.callc:
			logger.Warning("BusClient:SendData conn:%v,msg:%v", self.Conn, msg)
		case <-time.After(time.Second * time.Duration(to)):
			logger.Warning("BusClient:SendData conn:%v,msg:%v,to:%v", self.Conn, msg, to)
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
	buss map[int64]*BusClient

	cfg *Config

	parser network.IMessage
}

func NewBusClientMgr(cfg *Config) *BusClientMgr {
	mgr := &BusClientMgr{}
	mgr.init(cfg)
	return mgr
}

func (self *BusClientMgr) init(cfg *Config) {
	parser := network.NewProtocParser(-1)
	parser.Register(uint16(Cmd_NONE), CommonMessage{})
	self.parser = parser

	self.buss = make(map[int64]*BusClient, 0)

	self.cfg = cfg
	for _, v := range cfg.BusCfg {
		self.RegBus(v)
	}
}

func (self *BusClientMgr) RegBus(sinfo *NewSvrInfo) {
	if !self.cfg.OnNewBus(sinfo.Id) {
		return
	}

	self.Lock()
	defer self.Unlock()

	_, ok := self.buss[sinfo.Id]
	if ok {
		logger.Error("BusClientMgr:NewBusClient id:%v", sinfo.Id)
		return
	}

	clt := newBusClient(sinfo, self)

	self.buss[sinfo.Id] = clt

	// 异步链接
	go func() {
		defer utils.CatchPanic()
		clt.Connect(true)
	}()
}

func (self *BusClientMgr) DelBus(sinfo *DelSvrInfo) {
	self.Lock()
	logger.Info("BusClientMgr:DelBus id:%v,s:%v", sinfo.Id,
		GetServerIdStr(sinfo.Id))

	v, ok := self.buss[sinfo.Id]
	if !ok {
		self.Unlock()
		//logger.Error("BusClientMgr:DelBus id:%v", sinfo.DestId)
		return
	}

	delete(self.buss, sinfo.Id)
	self.Unlock()

	v.Disconnect()
}

func (self *BusClientMgr) UnRegBus(clt *BusClient) {
	logger.Info("BusClientMgr:UnRegBus id:%v,s:%v", clt.Id,
		GetServerIdStr(clt.Id))

	self.RLock()
	_, ok := self.buss[clt.Id]
	self.RUnlock()
	if ok {
		clt.SetAuthed(false)
	}

	if self.cfg.OnBusReg != nil {
		self.cfg.OnBusReg(clt.dest, 0)
	}
}

func (self *BusClientMgr) regBusOk(info *NewSvrInfo) {
	logger.Info("BusClientMgr:regBusOk id:%v,s:%v", info.Id,
		GetServerIdStr(info.Id))

	self.RLock()
	v, ok := self.buss[info.Id]
	self.RUnlock()

	if !ok {
		logger.Error("BusClientMgr:BusOk id:%v", info.Id)
		return
	}
	v.SetAuthed(true)

	if self.cfg.OnBusReg != nil {
		self.cfg.OnBusReg(info, 1)
	}
}

func (self *BusClientMgr) OnMount(clt *BusClient) {
	logger.Info("BusClientMgr:OnMount id:%v,s:%v", clt.Id,
		GetServerIdStr(clt.Id))

	self.RLock()
	_, ok := self.buss[clt.Id]
	self.RUnlock()
	if !ok {
		logger.Error("BusClientMgr:OnMount id:%v", clt.Id)
		return
	}

	// 发送注册消息
	msg := &CommonMessage{}
	msg.Code = Cmd_REG_SVR

	msg.SvrInfo = &RegSvrInfo{}
	msg.SvrInfo.Src = self.cfg.SvrInfo
	msg.SvrInfo.Dest = clt.dest
	rmsg := NewBusRawMessage(msg)
	clt.SendData(rmsg, false, 1)
}

func (self *BusClientMgr) OnBusData(msg *network.RawMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	if msgdata.Code == Cmd_REG_SVR {
		self.regBusOk(msgdata.SvrInfo.Dest)
	} else if msgdata.Code == Cmd_NEW_SVR {
		self.RegBus(msgdata.NewSvrInfo)
	} else if msgdata.Code == Cmd_DEL_SVR {
		self.DelBus(msgdata.DelSvrInfo)
	} else if msgdata.Code == Cmd_PING {
	} else {
		logger.Error("BusClientMgr:OnBusData code:%v", msgdata.Code)
	}

	return nil
}

func (self *BusClientMgr) GetBusClientById(svrId int64) []*BusClient {
	self.RLock()
	defer self.RUnlock()

	t := make([]*BusClient, 0)

	for _, v := range self.buss {
		// 必须认证过的
		if !v.IsMaintance() && v.GetAuthed() &&
			(svrId == 0 || (IsSameWorldFuncId(svrId, v.Id) &&
				(GetServerLogicId(svrId) == 0 ||
					GetServerLogicId(svrId) == GetServerLogicId(v.Id)))) {
			t = append(t, v)
		}
	}
	return t
}

// 轮询\广播\指定
func (self *BusClientMgr) SendData(msg *network.RawMessage,
	sync bool, to int32, svrId int64, st int64) (*network.RawMessage, int64) {
	t := self.GetBusClientById(svrId)

	if len(t) <= 0 {
		logger.Error("BusClientMgr:SendData svrId:%v", svrId)
		return nil, 0
	}

	// LISTMAP 要复制，效率多少提升？
	sort.Slice(t, func(i int, k int) bool {
		return t[i].Id > t[k].Id
	})

	var cliId int64
	var rt *network.RawMessage
	if st > 0 {
		cli := t[st%int64(len(t))]
		cliId = cli.Id
		rt = cli.SendData(msg, sync, to)
	} else if st == 0 {
		for _, v := range t {
			rmsg := msg
			if sync {
				// !!!sync
				rmsg = NewBusRawMessage(msg.MsgData.(*CommonMessage))
			}
			_ = v.SendData(rmsg, sync, to)
		}
	} else if st == -1 {
		cli := t[rand.RandInt(0, int32(len(t)-1))]
		cliId = cli.Id
		rt = cli.SendData(msg, sync, to)
	}
	return rt, cliId
}

func (self *BusClientMgr) RecvRouteMsg(id int64, msg *network.RawMessage) {
	if self.cfg.OnData != nil {
		self.cfg.OnData(id, msg)
	}
}

// svrId要去哪里
// st发送方式0:全部,-1:随机,>0:st_round
// destsvr到wfuncId不是要下车还是继续
// sync是不是rpc
// to发送超时
func (self *BusClientMgr) SendRouteMsg(destSvr int64, destSt int64,
	msg *network.RawMessage, sync bool, to int32, svrId int64, st int64) (*network.RawMessage, int64) {

	rmsg := NewRouteRawMessageOut(destSvr, destSt, msg, self.cfg.Parser)
	if rmsg != nil {
		return self.SendData(rmsg, sync, to, svrId, st)
	}
	return nil, 0
}

func (self *BusClientMgr) Maintance(id int64) {
	self.RLock()
	defer self.RUnlock()

	v, ok := self.buss[id]
	if !ok {
		return
	}

	v.SetMaintance(true)
}

func (self *BusClientMgr) Close() {
	self.RLock()
	for _, v := range self.buss {
		v.Disconnect()
	}
	self.RUnlock()

	utils.ASyncWait(func() bool {
		self.RLock()
		defer self.RUnlock()

		for _, v := range self.buss {
			if v.GetAuthed() {
				return false
			}
		}
		return true
	})
}

//------------------------------------------------------------------------------------

type BusServer struct {
	network.Agent

	Id  int64 // client ID
	Mgr *BusServerMgr

	OnDisconnect func()
	OnData       BusDataCall

	dest *NewSvrInfo
}

func (self *BusServer) Initx(cfg *network.Config, mgr *BusServerMgr) {
	self.Mgr = mgr

	ncfg := &network.Config{}
	ncfg.Parser = mgr.parser
	ncfg.Rpm = 9999999

	self.Init(ncfg)
}

func (self *BusServer) Hand_Close(flag int32) {
	logger.Debug("BusServer:onclose conn:%v,id:%v,s:%v", self.Conn,
		self.Id, GetServerIdStr(self.Id))

	if self.OnDisconnect != nil {
		self.OnDisconnect()
	}

	if self.Id > 0 {
		self.Mgr.DelSvr(self.Id)
	}
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

	logger.Debug("BusServer:RegSvr con:%v,id:%v,s:%v", self.Conn,
		req.Src.Id, GetServerIdStr(req.Src.Id))

	if !self.Mgr.AddSvr(req.Src, self) {
		// !
		self.Close()
		return
	}

	self.dest = req.Src
	// 绑定id
	self.Id = req.Src.Id
	//self.OnAuthCheck = func() bool {
	//	return true
	//}
	// 少序列化点数据
	//msg.SvrInfo = nil

	rmsg := NewBusRawMessage(msg)
	self.SendMsg(rmsg, 1)
}

func (self *BusServer) SendRouteMsg(msg *network.RawMessage) {
	rmsg := NewRouteRawMessageOut(0, -1, msg, self.Mgr.parser)
	if rmsg != nil {
		self.SendMsg(rmsg, 1)
	}
}

func (self *BusServer) RecvRouteMsg(msg *network.RawMessage) {
	msgdata := msg.MsgData.(*CommonMessage)
	req := msgdata.RouteInfo

	if IsSameWorldFuncId(req.DestSvr, self.Mgr.cfg.SvrInfo.Id) {
		if GetServerLogicId(req.DestSvr) == 0 ||
			GetServerLogicId(req.DestSvr) == GetServerLogicId(self.Mgr.cfg.SvrInfo.Id) {
			if self.OnData != nil {
				rmsg := NewRouteRawMessageIn(msg, self.Mgr.cfg.Parser)
				if rmsg != nil {
					self.OnData(self.Id, rmsg)
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
	parser := network.NewProtocParser(-1)
	parser.Register(uint16(Cmd_NONE), CommonMessage{})
	self.parser = parser

	self.cfg = cfg
	self.servers = make(map[int64]*BusServer, 0)
}

func (self *BusServerMgr) AddSvr(info *NewSvrInfo, svr *BusServer) bool {
	self.Lock()

	logger.Info("BusServerMgr:AddSvr id:%v,s:%s", info.Id, GetServerIdStr(info.Id))

	_, ok := self.servers[info.Id]
	if ok {
		logger.Error("BusServerMgr:AddSvr id:%v", info.Id)
		self.Unlock()
		return false
	}

	self.servers[info.Id] = svr
	self.Unlock()

	if self.cfg.OnBusReg != nil {
		self.cfg.OnBusReg(info, 1)
	}

	return true
}

func (self *BusServerMgr) DelSvr(id int64) {
	self.Lock()

	logger.Info("BusServerMgr:DelSvr id:%v,s:%s", id, GetServerIdStr(id))

	svr, ok := self.servers[id]
	if !ok {
		logger.Error("BusServerMgr:DelSvr id:%v", id)
		self.Unlock()
		return
	}

	delete(self.servers, id)
	self.Unlock()

	if self.cfg.OnBusReg != nil {
		self.cfg.OnBusReg(svr.dest, 0)
	}
}

func (self *BusServerMgr) GetServersById(id int64) []*BusServer {
	self.RLock()
	defer self.RUnlock()

	svrs := make([]*BusServer, 0)

	for _, v := range self.servers {
		// 必须认证过的
		if id == 0 || (IsSameWorldFuncId(id, v.Id) &&
			(GetServerLogicId(id) == 0 ||
				GetServerLogicId(id) == GetServerLogicId(v.Id))) {
			svrs = append(svrs, v)
		}
	}

	return svrs
}

//是不是给自己
func (self *BusServerMgr) RecvRouteMsg(msg *network.RawMessage) {
	msgdata := msg.MsgData.(*CommonMessage)
	req := msgdata.RouteInfo

	self.SendData(req.DestSvr, req.DestSt, msg, 1)
}

func (self *BusServerMgr) NewServer(id int64, ip string, port string) {
	msg := &CommonMessage{}
	msg.Code = Cmd_NEW_SVR
	msg.NewSvrInfo = &NewSvrInfo{}
	msg.NewSvrInfo.Id = id
	msg.NewSvrInfo.Ip = ip
	msg.NewSvrInfo.Port = port

	rmsg := NewBusRawMessage(msg)

	self.SendData(0, 0, rmsg, 1)
}

func (self *BusServerMgr) DelServer(id int64) {
	msg := &CommonMessage{}
	msg.Code = Cmd_DEL_SVR
	msg.DelSvrInfo = &DelSvrInfo{}
	msg.DelSvrInfo.Id = id

	rmsg := NewBusRawMessage(msg)

	self.SendData(0, 0, rmsg, 1)
}

// SendData 发送方式0:全部,-1:随机,>0:st_round
func (self *BusServerMgr) SendData(svrId int64, st int64,
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

	// LISTMAP 要复制，效率多少提升？
	sort.Slice(svrs, func(i int, k int) bool {
		return svrs[i].Id > svrs[k].Id
	})

	buf, err := self.parser.Serialize(msg)
	if err != nil {
		logger.Error("BusServerMgr:SendData id:%v", err)
		return
	}

	defer func() {
		buf.Free()
	}()

	if st > 0 {
		buf.Ref()
		svrs[st%int64(len(svrs))].SendMsg(buf, 1)
	} else if st == 0 {
		for _, v := range svrs {
			//增加引用次数
			buf.Ref()
			v.SendMsg(buf, 1)
		}
	} else if st == -1 {
		buf.Ref()
		svrs[rand.RandInt(0, int32(len(svrs)-1))].SendMsg(buf, 1)
	}
}

// st发送方式0:全部,-1:随机,>0:st_round
func (self *BusServerMgr) SendRouteMsg(destSvr int64, destSt int64,
	msg *network.RawMessage, to int32, svrId int64, st int64) {

	rmsg := NewRouteRawMessageOut(destSvr, destSt, msg, self.cfg.Parser)
	if rmsg != nil {
		self.SendData(svrId, st, rmsg, 1)
	}
}

func (self *BusServerMgr) Close() {
	utils.ASyncWait(func() bool {
		self.RLock()
		defer self.RUnlock()
		return len(self.servers) == 0
	})
}

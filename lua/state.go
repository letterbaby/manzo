package lua

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/letterbaby/manzo/logger"

	lua "github.com/yuin/gopher-lua"

	"github.com/letterbaby/manzo/lua/luar"
	"github.com/letterbaby/manzo/rand"

	"github.com/letterbaby/manzo/utils"
)

type LuaMessage struct {
	Id       int32 // key
	PlayerId int32 // 玩家id
	MsgData  interface{}
}

/// ??????
var (
	lualogger = NewModule("logger")
	luarand   = NewModule("rand")
	luabit    = NewModule("bit")
)

func init() {
	lualogger.Func("Debug", logger.Debug)
	lualogger.Func("Info", logger.Info)
	lualogger.Func("Warning", logger.Warning)
	lualogger.Func("Error", logger.Error)
	lualogger.Func("Fatal", logger.Fatal)

	luarand.Func("RandInt", rand.RandInt)

	luabit.Func("Band", func(a int64, b int64) int64 {
		return a & b
	})
	luabit.Func("Bor", func(a int64, b int64) int64 {
		return a | b
	})
}

var (
	luaMsgPool = &sync.Pool{
		New: func() interface{} {
			return &LuaMessage{}
		},
	}
)

type callInfo struct {
	sinfo string
	td    float64
	einfo string
}

type proCall struct {
	bt     int64
	cis    []*callInfo
	fnIdx  int
	fnLast string
}

type proData struct {
	fn   string //地址
	td   float64
	info string
	cis  []*callInfo
}

type proView struct {
	td   float64
	cnt  float64
	max  float64
	min  float64
	info string
}

type proHook struct {
	evt interface{}
	snf interface{}
}

type proViews []*proView

func (self proViews) Len() int {
	return len(self)
}

func (self proViews) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self proViews) Less(i, j int) bool {
	x := self[i].td / self[i].cnt
	y := self[j].td / self[j].cnt
	return x > y
}

type LuaState struct {
	X     int                        //唯一标志
	index int                        // heapidx
	refs  map[int32]chan *LuaMessage // 有那些key共享这个LS
	in    chan *LuaMessage

	state *lua.LState
	Msg   *LuaMessage // POP的消息

	//--------------------------
	pro_fdir  string
	pro_fpath string
	pro_fname string
	pro_flog  *os.File
	pro_run   chan bool
	proCalls  map[string]*proCall
	proDatas  []*proData
	proSaves  []string
	pro_fns   []string
	pro_ch    chan *proHook
	pro_tick  int
	pro_save  int
	//--------------------------

	sync.RWMutex
}

func (self *LuaState) Init(x int, loadlibs func(s *lua.LState)) {
	self.X = x

	self.refs = make(map[int32]chan *LuaMessage)
	self.in = make(chan *LuaMessage, 1024)

	self.state = lua.NewState()

	// 自己
	self.state.SetGlobal("Host", luar.New(self.state, self))
	// 这个是必须1
	lualogger.Load(self.state)
	luarand.Load(self.state)
	luabit.Load(self.state)

	// 应用层2
	loadlibs(self.state)

	err := self.state.DoFile("./script/init.lua")
	if err != nil {
		logger.Fatal("LuaState:init msg:%v", err)
	}

	go self.run()
}

func (self *LuaState) RefCount() int {
	self.RLock()
	defer self.RUnlock()

	return len(self.refs)
}

func (self *LuaState) Close() {
	self.state.Close()

	if self.pro_run != nil {
		self.pro_run <- true // 同步的

		l := len(self.pro_ch)
		for i := 0; i < l; i++ {
			ph := <-self.pro_ch
			self.updateProf(ph)
		}

		self.writeProf()

		self.pro_flog.Close()
	}
}

func (self *LuaState) Ref(id int32, out chan *LuaMessage, msg interface{}) {
	self.Lock()
	_, ok := self.refs[id]
	if ok {
		self.Unlock()
		logger.Error("LuaState:ref s:%v,id:%v", self.X, id)
		return
	}
	self.refs[id] = out
	self.Unlock()

	//投递创建
	self.PushIn(id, -1, msg, 99999)
}

func (self *LuaState) UnRef(id int32) {
	//投递销毁
	self.PushIn(id, -2, nil, 99999)

	self.Lock()
	_, ok := self.refs[id]
	if !ok {
		self.Unlock()
		logger.Error("LuaState:unref s:%v,id:%v", self.X, id)
		return
	}

	delete(self.refs, id)
	self.Unlock()
}

func (self *LuaState) getChan(id int32) chan *LuaMessage {
	self.RLock()
	defer self.RUnlock()

	v, ok := self.refs[id]
	if !ok {
		return nil
	}

	return v
}

func (self *LuaState) PushIn(id int32, playerId int32, msgData interface{}, to int32) bool {
	if self.getChan(id) == nil {
		logger.Error("LuaState:in id:%v,s:%v,u:%v,msg:%v", id, self.X, playerId, msgData)
		return false
	}

	if to <= 0 {
		to = 1
	}

	msg := luaMsgPool.Get().(*LuaMessage)
	msg.Id = id
	msg.PlayerId = playerId
	msg.MsgData = msgData

	select {
	case self.in <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		self.PutLuaMsg(msg)
		logger.Warning("LuaState:in s:%v,msg:%v", self.X, msg)
		return false
	}
	return true
}

func (self *LuaState) PushOut(id int32, playerId int32, msgData interface{}, to int32) {
	out := self.getChan(id)
	if out == nil {
		logger.Error("LuaState:out id:%v,s:%v,u:%v,msg:%v", id, self.X, playerId, msgData)
		return
	}

	if to <= 0 {
		to = 1
	}

	msg := luaMsgPool.Get().(*LuaMessage)
	msg.Id = id
	msg.PlayerId = playerId
	msg.MsgData = msgData

	select {
	case out <- msg:
	case <-time.After(time.Second * time.Duration(to)):
		self.PutLuaMsg(msg)
		logger.Warning("LuaState:out s:%v,msg:%v", self.X, msg)
	}
}

func (self *LuaState) PutLuaMsg(msg *LuaMessage) {
	// proto就不跟着进池了
	msg.MsgData = nil
	luaMsgPool.Put(msg)
}

func (self *LuaState) call(fc string, fn lua.LValue, args ...lua.LValue) {
	now := time.Now()

	err := self.state.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, args...)

	if err != nil {
		logger.Error("LuaState:call s:%v,fc:%v,args:%v,msg:%v", self.X, fc, args, err)
		return
	}

	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("LuaState:call s:%v,fc:%v,args:%v,time:%v", self.X, fc, args, tt)
	}
}

func (self *LuaState) run() {
	defer utils.CatchPanic()

	fticker := time.NewTicker(time.Second * 1)
	defer func() {
		fticker.Stop()
	}()

	//gc := 0

	r := self.state.GetGlobal("recv")
	u := self.state.GetGlobal("update")

	for {
		select {
		case self.Msg = <-self.in:
			self.call("recv", r)
			self.PutLuaMsg(self.Msg)
			self.Msg = nil

		case <-fticker.C:
			if self.RefCount() > 0 {
				//gc = gc + 1
				self.call("update", u)
				//if gc >= 30 {
				//	runtime.GC()
				//	gc = 0
				//}
			}
		}
	}
}

func (self *LuaState) StartProf(dir string, name string, save int) {
	self.pro_fdir = dir
	self.pro_fname = name
	self.pro_run = make(chan bool)
	self.proCalls = make(map[string]*proCall, 0)
	self.proDatas = make([]*proData, 0)
	self.proSaves = make([]string, 0)

	self.pro_save = save
	self.pro_tick = 0
	self.pro_fns = make([]string, 0)
	self.pro_ch = make(chan *proHook, 10240)

	go self.prof()
}

//异步写log
func (self *LuaState) prof() {
	defer utils.CatchPanic()

	fticker := time.NewTicker(time.Second * time.Duration(self.pro_save))
	defer func() {
		fticker.Stop()
	}()
	for {
		select {
		case ph := <-self.pro_ch:
			self.updateProf(ph)
		case <-fticker.C:
			self.writeProf()
		case <-self.pro_run:
			return
		}
	}
}

func (self *LuaState) ProfHook(evt interface{}, snf interface{}) {
	//logger.Debug("LuaState:ProfHook evt:%v,snf:%v", evt, snf)
	ph := &proHook{}
	ph.evt = evt
	ph.snf = snf

	//--??目前看这个chan小了
	select {
	case self.pro_ch <- ph:
	default:
		logger.Warning("LuaState:ProfHook s:%v,msg:%v", self.X, ph)
		//?? 接下来堆栈不匹配,没参考价值
	}
}

func (self *LuaState) updateProf(ph *proHook) {
	dbg, ok := ph.snf.(map[interface{}]interface{})
	if !ok || len(dbg) <= 0 {
		return
	}

	e, _ := ph.evt.(string)
	fn := dbg["func_"].(string)

	if e == "c" {
		pc := self.proCalls[fn]
		if pc == nil {
			pc = &proCall{}
			pc.bt = time.Now().UnixNano()
			pc.cis = make([]*callInfo, 0)
			pc.fnIdx = -1
			pc.fnLast = ""
			fns := len(self.pro_fns)
			if fns > 0 {
				pc.fnLast = self.pro_fns[fns-1]
				al := self.proCalls[pc.fnLast]
				if al != nil {
					pc.fnIdx = len(al.cis) - 1
				}
			}
			self.proCalls[fn] = pc
			self.pro_fns = append(self.pro_fns, fn)
		}
		ci := &callInfo{}
		ci.td = 0
		ci.einfo = "[G] ??"
		ci.sinfo = fmt.Sprintf("[%v]:%v->%v (%v)",
			dbg["source"], dbg["linedefined"],
			dbg["currentline"], dbg["name"])

		pc.cis = append(pc.cis, ci)
	} else {
		pc := self.proCalls[fn]
		if pc == nil {
			return
		}

		pd := &proData{}
		pd.fn = fn
		pd.td = (float64(time.Now().UnixNano()) - float64(pc.bt)) / 1e6
		pd.cis = pc.cis
		pd.info = fmt.Sprintf("[%v]:%v->%v (%v)",
			dbg["source"], dbg["linedefined"],
			dbg["currentline"], dbg["name"])

		self.proDatas = append(self.proDatas, pd)

		if pc.fnLast != "" {
			tpc := self.proCalls[pc.fnLast]
			if tpc != nil && pc.fnIdx >= 0 && pc.fnIdx < len(tpc.cis) {
				tcs := tpc.cis[pc.fnIdx]
				tcs.td = pd.td
				tcs.einfo = pd.info
			} else {
				// G??
			}
		}
		delete(self.proCalls, fn)
		fns := len(self.pro_fns)
		if fns > 0 {
			self.pro_fns = append([]string{}, self.pro_fns[:fns-1]...)
		}

		//!!!
		if len(self.pro_fns) <= 0 {
			self.saveProf()
		}
	}
}

func (self *LuaState) saveProf() {
	str := ""
	ref := make(map[string]*proView)

	for _, v := range self.proDatas {
		str = str + fmt.Sprintf("\n%s\t%.2f", v.info, v.td)
		for _, vv := range v.cis {
			str = str + fmt.Sprintf("\n\t%s\t%s\t%.2f", vv.sinfo, vv.einfo, vv.td)
		}

		x := ref[v.fn]
		if x == nil {
			x = &proView{}
			x.td = 0
			x.cnt = 0
			x.max = 0
			x.min = 0
			x.info = v.info
			ref[v.fn] = x
		}

		x.cnt = x.cnt + 1

		x.td = x.td + v.td
		if v.td < x.min {
			x.min = v.td
		} else if v.td > x.max {
			x.max = v.td
		}
	}
	str = str + "\n#################################################################"
	self.proSaves = append(self.proSaves, str)

	t := make(proViews, 0)
	for _, v := range ref {
		t = append(t, v)
	}

	sort.Sort(t)
	str = ""
	for _, v := range t {
		str = str + fmt.Sprintf("\n\t%s\t%d\t%.2f\t%.2f\t%.2f", v.info, int(v.cnt), v.td/v.cnt, v.max, v.min)
	}
	str = str + "\n-----------------------------------------------------------------"
	self.proSaves = append(self.proSaves, str)

	self.proCalls = make(map[string]*proCall, 0)
	self.proDatas = make([]*proData, 0)
}

func (self *LuaState) writeProf() {
	now := time.Now()

	fp := fmt.Sprintf("%s/%d%02d%02d_%02d_%d_%s",
		self.pro_fdir,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		self.X,
		self.pro_fname)

	if fp != self.pro_fpath {
		if self.pro_flog != nil {
			self.pro_flog.Close()
			self.pro_flog = nil
		}

		self.pro_flog, _ = os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_RDWR,
			os.ModeAppend|os.ModePerm)
		self.pro_fpath = fp
	}

	for _, v := range self.proSaves {
		self.pro_flog.Write([]byte(v))
	}
	self.proSaves = make([]string, 0)
}

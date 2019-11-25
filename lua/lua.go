package lua

import (
	"container/heap"
	"sync"

	"github.com/letterbaby/manzo/logger"

	"github.com/letterbaby/manzo/utils"

	lua "github.com/yuin/gopher-lua"
)


//TODO:兼容普通的Lua模式,这个有点定制

// 负载
type LSHeap []*LuaState

func (self LSHeap) Len() int {
	return len(self)
}

func (self LSHeap) Less(i, j int) bool {
	return self[i].RefCount() < self[j].RefCount()
}

func (self LSHeap) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
	self[i].index = i
	self[j].index = j
}

func (self *LSHeap) Push(s interface{}) {
	s.(*LuaState).index = len(*self)
	*self = append(*self, s.(*LuaState))
}

func (self *LSHeap) Pop() interface{} {
	l := len(*self)
	s := (*self)[l-1]
	s.index = -1
	*self = (*self)[:l-1]
	return s
}

type Config struct {
	MaxCount int `json:"maxcount"` // 虚拟机的数量
}

var (
	LuaConfig = &Config{}
)

type Lua struct {
	Lsh LSHeap
	sync.Mutex
}

func NewLua(loadlibs func(s *lua.LState)) *Lua {
	lua := &Lua{}
	lua.Init(loadlibs)
	return lua
}

func (self *Lua) Init(loadlibs func(s *lua.LState)) {
	if LuaConfig.MaxCount <= 0 {
		LuaConfig.MaxCount = 1
		logger.Warning("Lua:init MaxCount <= 0 defalut 1")
	}

	self.Lsh = make(LSHeap, LuaConfig.MaxCount)

	wg := sync.WaitGroup{}
	wg.Add(LuaConfig.MaxCount)
	for i := 0; i < LuaConfig.MaxCount; i++ {
		go func(x int) {
			defer utils.CatchPanic()

			defer wg.Done()

			self.Lsh[x] = &LuaState{}
			self.Lsh[x].Init(x, loadlibs)
		}(i)
	}
	wg.Wait()
}

// 用到luastate的必须先释放
func (self *Lua) Close() {
	self.Lock()
	defer self.Unlock()

	for _, v := range self.Lsh {
		if v.RefCount() != 0 {
			logger.Warning("Lua:close s:%v", v.X)
		}
		v.Close()
	}
}

// 取第一个相对空闲的
func (self *Lua) Ref(id int32, out chan *LuaMessage, msg interface{}) *LuaState {
	self.Lock()
	ls := self.Lsh[0]
	heap.Fix(&self.Lsh, 0)
	self.Unlock()

	ls.Ref(id, out, msg)

	return ls
}

// 释放
func (self *Lua) UnRef(id int32, ls *LuaState) {
	ls.UnRef(id)

	self.Lock()
	heap.Fix(&self.Lsh, ls.index)
	self.Unlock()
}

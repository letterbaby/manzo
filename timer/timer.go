package timer

import (
	"math"
	"sync"
	"time"

	"github.com/letterbaby/manzo/logger"

	"github.com/letterbaby/manzo/utils"

	"container/heap"
)

type ITimer interface {
	OnTimer(flag int64)
	GetIndex() int
	GetPriority() int64
	SetIndex(idx int)
	SetPriority(p int64)
	GetId() int64
	SetId(id int64)
}

type LSTimerData []ITimer

func (self LSTimerData) Len() int {
	return len(self)
}

func (self LSTimerData) Less(i, j int) bool {
	return self[i].GetPriority() < self[j].GetPriority()
}

func (self LSTimerData) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
	self[i].SetIndex(i)
	self[j].SetIndex(j)
}

func (self *LSTimerData) Push(s interface{}) {
	s.(ITimer).SetIndex(len(*self))
	*self = append(*self, s.(ITimer))
}

func (self *LSTimerData) Pop() interface{} {
	old := *self
	n := len(old)
	item := old[n-1]
	item.SetIndex(-1) // for safety
	*self = old[0 : n-1]
	return item
}

//----------------------------------------------------------
type MyTimer struct {
	Id      int64
	HeapIdx int   //PQ
	HeapRef int64 //PQ
}

func (self *MyTimer) SetId(id int64) {
	self.Id = id
}

func (self *MyTimer) GetId() int64 {
	return self.Id
}

func (self *MyTimer) OnTimer(flag int64) {
}

func (self *MyTimer) GetIndex() int {
	return self.HeapIdx
}

func (self *MyTimer) GetPriority() int64 {
	return self.HeapRef
}

func (self *MyTimer) SetIndex(idx int) {
	self.HeapIdx = idx
}

func (self *MyTimer) SetPriority(p int64) {
	self.HeapRef = p
}

//-----------------------------------------------------------------
// goroutine safe
type TimerMgr struct {
	sync.RWMutex

	datas LSTimerData
	acc   time.Duration // tick sleep

	add chan ITimer
	del chan ITimer

	snakeId int64
	datams  map[int64]ITimer
}

func NewTimerMgr(ac int, size int) *TimerMgr {
	tm := &TimerMgr{}
	tm.init(ac, size)
	return tm
}

func (self *TimerMgr) init(ac int, size int) {
	self.datams = make(map[int64]ITimer)

	self.add = make(chan ITimer, size)
	self.del = make(chan ITimer, size)

	self.acc = time.Duration(ac)
	self.datas = make(LSTimerData, 0)

	go self.run()
}

func (self *TimerMgr) Add(t ITimer, tick int64) {
	t.SetPriority(tick)
	select {
	case self.add <- t:
	default:
		logger.Error("TimerMgr:Add t:%v", t)
	}
}

func (self *TimerMgr) Del(t ITimer) {
	select {
	case self.del <- t:
	default:
		logger.Error("TimerMgr:Del t:%v", t)
	}
}

func (self *TimerMgr) nextId() int64 {
	self.snakeId = self.snakeId + 1
	if self.snakeId > math.MaxInt64 {
		self.snakeId = 0
	}

	return self.snakeId
}

func (self *TimerMgr) push() {
	for {
		select {
		case t := <-self.add:
			err := false

			if t.GetId() > 0 {
				_, err = self.datams[t.GetId()]
			} else {
				t.SetId(self.nextId())
			}

			if !err {
				logger.Debug("TimerMgr:push t:%v", t.GetId())
				self.datams[t.GetId()] = t
				heap.Push(&self.datas, t)
			} else {
				logger.Error("TimerMgr:push t:%v", t)
			}
		default:
			return
		}
	}
}

func (self *TimerMgr) pop() {
	t := heap.Pop(&self.datas).(ITimer)

	now := time.Now().Unix()
	if now < t.GetPriority() {
		heap.Push(&self.datas, t)
		return
	}

	delete(self.datams, t.GetId())
	logger.Debug("TimerMgr:pop t:%v", t.GetId())

	t.OnTimer(t.GetId())
}

func (self *TimerMgr) remove() {
	for {
		select {
		case t := <-self.del:
			ok := true

			if t.GetId() > 0 {
				_, ok = self.datams[t.GetId()]
			} else {
				ok = false
			}

			if ok {
				logger.Debug("TimerMgr:remove t:%v", t.GetId())
				delete(self.datams, t.GetId())
				heap.Remove(&self.datas, t.GetIndex())
			} else {
				logger.Error("TimerMgr:remove t:%v", t)
			}
		default:
			return
		}
	}
}

func (self *TimerMgr) run() {
	defer utils.CatchPanic()

	for {
		// step:1
		self.push()
		// step:2
		self.remove()

		// step:3
		if self.datas.Len() > 0 {
			self.pop()
		} else {
			time.Sleep(time.Millisecond * self.acc)
		}
	}
}

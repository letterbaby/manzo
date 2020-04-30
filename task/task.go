package task

import (
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

type ITask interface {
	GetTaskSn(cnt int32) int8
	OnExcute()
	TimeOut()
}

type Tasker struct {
	sn   int8
	name string
	Msg  chan ITask
}

func (self *Tasker) init(sn int8, name string) {
	self.sn = sn
	self.name = name
	self.Msg = make(chan ITask, 10240)

	go self.run()
}

func (self *Tasker) tryExcute(t ITask) {
	now := time.Now()

	//ok := utils.CallByTimeOut(func() {
	t.OnExcute()
	//}, 10)
	//if !ok {
	//msg.TimeOut()
	//}
	tt := time.Now().Sub(now)
	if tt > time.Millisecond*50 {
		logger.Warning("Tasker:tryExcute sn:%v,n:%v,t:%v,tt:%v",
			self.sn, self.name, t, time.Now().Sub(now))
	}
}

func (self *Tasker) run() {
	defer utils.CatchPanic()

	for {
		select {
		case msg := <-self.Msg:
			self.tryExcute(msg)
		}
	}
}

func (self *Tasker) add(t ITask) bool {
	select {
	case self.Msg <- t:
		return true
	default:
		logger.Warning("Tasker:Add sn:%v,n:%v,t:%v", self.sn, self.name, t)
	}
	return false
}

func (self *Tasker) close() {
	utils.ASyncWait(func() bool {
		return len(self.Msg) == 0
	})
}

//---------------------------------------------------------------------
type TaskerMgr struct {
	name string
	ters map[int8]*Tasker
}

func NewTaskerMgr(name string, size int8) *TaskerMgr {
	term := &TaskerMgr{}
	term.init(name, size)
	return term
}

func (self *TaskerMgr) init(name string, size int8) {
	self.name = name
	self.ters = make(map[int8]*Tasker, 0)
	for i := int8(0); i < size; i++ {
		ter := &Tasker{}
		ter.init(i, name)
		self.ters[i] = ter
	}
}

func (self *TaskerMgr) TaskerCount() int32 {
	return int32(len(self.ters))
}

func (self *TaskerMgr) findTasker(sn int8) *Tasker {
	v, ok := self.ters[sn]
	if ok {
		return v
	}
	return nil
}

func (self *TaskerMgr) AddTask(t ITask) bool {
	sn := t.GetTaskSn(self.TaskerCount())

	ter := self.findTasker(sn)
	if ter == nil {
		logger.Error("TaskerMgr:AddTask sn:%d,n:%v,i:%v", sn, self.name, t)
		return false
	}

	return ter.add(t)
}

func (self *TaskerMgr) Close() {
	for _, v := range self.ters {
		v.close()
	}
}

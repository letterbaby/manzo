package uuid

import (
	"sync"
	"time"

	"github.com/letterbaby/manzo/logger"
)

// 时间戳	| WorldId(256)| FuncId (16) | LogicId(256) |  毫秒(65535)
// ? 	    | 	8        |	    4     |  8            | 16
const (
	worldIdBits = 8
	funcIdBits  = 4
	logicIdBits = 8
	numberBits  = 16

	// 最大值检查
	worldIdMax = -1 ^ (-1 << worldIdBits)
	funcIdMax  = -1 ^ (-1 << funcIdBits)
	logicIdMax = -1 ^ (-1 << logicIdBits)
	numberMax  = -1 ^ (-1 << numberBits)

	timeShift = worldIdBits + funcIdBits + logicIdBits + numberBits

	//时间戳的最大值
	timeMax = -1 ^ (-1 << (64 - timeShift))

	worldShift = funcIdBits + logicIdBits + numberBits
	funcShift  = logicIdBits + numberBits
	logicShift = numberBits

	epoch = 1573717147000 // 开始时间戳
)

type Uuid int64

func (self Uuid) GetWorldId() int64 {
	return int64(byte(self >> worldShift))
}

type UuidMgr struct {
	sync.Mutex

	worldId int64
	funcId  int64
	logicId int64
	number  int64

	timestamp int64
}

func NewUuidMgr(wId int64, fId int64, lId int64) *UuidMgr {
	nid := &UuidMgr{}
	if !nid.init(wId, fId, lId) {
		return nil
	}
	return nid
}

func (self *UuidMgr) init(wId int64, fId int64, lId int64) bool {
	if wId < 0 || wId > worldIdMax ||
		fId < 0 || fId > funcIdMax ||
		lId < 0 || lId > logicIdMax {
		logger.Fatal("UUID:init w:%v,f:%v,l:%v", wId, fId, lId)
		return false
	}

	self.worldId = wId
	self.funcId = fId
	self.logicId = lId
	self.number = 0
	self.timestamp = 0

	return true
}

// 线程安全
func (self *UuidMgr) nextNumber() int64 {
	self.Lock()
	defer self.Unlock()

	now := time.Now().UnixNano() / 1e6
	if self.timestamp == now {
		self.number++

		if self.number > numberMax {
			for now <= self.timestamp {
				now = time.Now().UnixNano() / 1e6
			}
		}
	} else {
		self.number = 0
		self.timestamp = now
	}
	return now
}

func (self *UuidMgr) NextId() Uuid {
	td := (self.nextNumber() - epoch)
	if td > timeMax {
		logger.Error("UuidMgr:NextId td:%v", td)
	}

	return Uuid((td << timeShift) |
		(self.worldId << worldShift) |
		(self.funcId << funcShift) |
		(self.logicId << logicShift) |
		self.number)
}

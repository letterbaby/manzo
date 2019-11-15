package uuid

import (
	"sync"
	"time"

	"github.com/letterbaby/manzo/logger"
)

// 时间戳| seed
// ? 	 | 	32
const (
	seedBits = 32

	// 最大值检查
	seedMax = -1 ^ (-1 << seedBits)

	timeShift = seedBits

	//时间戳的最大值
	timeMax = -1 ^ (-1 << (64 - seedBits))

	//		1573784033611
	epoch = 1573717147000 // 开始时间戳
)

type Uuid int64

func (self Uuid) GetSeed() int64 {
	return int64(uint32(self))
}

type UuidMgr struct {
	sync.Mutex

	seed      int64
	timestamp int64
}

func NewUuidMgr(seed int64) *UuidMgr {
	nid := &UuidMgr{}
	if !nid.init(seed) {
		return nil
	}
	return nid
}

func (self *UuidMgr) init(seed int64) bool {
	if seed < 0 || seed > seedMax {
		logger.Fatal("UUID:init seed:%v", seed)
		return false
	}
	self.seed = seed
	self.timestamp = 0

	return true
}

// 线程安全,一毫秒一个?效率?
func (self *UuidMgr) nextTime() int64 {
	self.Lock()
	defer self.Unlock()

	now := time.Now().UnixNano() / 1e6
	if self.timestamp == now {
		for now <= self.timestamp {
			now = time.Now().UnixNano() / 1e6
		}
	}
	self.timestamp = now
	return now
}

func (self *UuidMgr) NextId() Uuid {
	td := (self.nextTime() - epoch)
	if td > timeMax {
		logger.Error("UuidMgr:NextId td:%v", td)
	}

	return Uuid((td << timeShift) | self.seed)
}

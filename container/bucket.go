package container

import (
	"sync"

	"github.com/letterbaby/manzo/logger"
)

type IGood interface {
	Key() int64
	Cast(interface{}) // 广播
}

type Bucket struct {
	sync.RWMutex
	goods map[int64]IGood
}

func NewBucket() *Bucket {
	bk := &Bucket{}
	bk.init()
	return bk
}

func (self *Bucket) init() {
	self.goods = make(map[int64]IGood, 0)
}

func (self *Bucket) Count() int {
	self.RLock()
	defer self.RUnlock()

	return len(self.goods)
}

func (self *Bucket) Add(good IGood) {
	self.Lock()
	defer self.Unlock()

	self.goods[good.Key()] = good
}

func (self *Bucket) Del(k int64) {
	self.Lock()
	defer self.Unlock()

	_, ok := self.goods[k]
	if !ok {
		logger.Error("Bucket:Del rl:%v", k)
		return
	}
	delete(self.goods, k)
}

func (self *Bucket) Get(gid int64, creator func() IGood) IGood {
	self.Lock()
	defer self.Unlock()

	v, ok := self.goods[gid]
	if ok {
		return v
	}

	if creator == nil {
		return nil
	}

	v = creator()
	self.goods[gid] = v
	return v
}

func (self *Bucket) Cast(msg interface{}) {
	self.RLock()
	defer self.RUnlock()

	// 遍历发送
	for _, v := range self.goods {
		v.Cast(msg)
	}
}

//---------------------------------------------------------------------------------
type BucketMgr struct {
	size    int64
	buckets []*Bucket
}

func NewBcketMgr(size int64) *BucketMgr {
	mgr := &BucketMgr{}
	mgr.init(size)
	return mgr
}

func (self *BucketMgr) init(size int64) {
	self.size = size
	self.buckets = make([]*Bucket, 0)

	for i := int64(0); i < size; i++ {
		bucket := NewBucket()

		self.buckets = append(self.buckets, bucket)
	}
}

func (self *BucketMgr) AddGood(good IGood) {
	idx := good.Key() % self.size

	self.buckets[idx].Add(good)
}

func (self *BucketMgr) DelGood(gid int64) {
	idx := gid % self.size

	self.buckets[idx].Del(gid)
}

// GetGood 通过id找并创建
func (self *BucketMgr) GetGood(gid int64, creator func() IGood) IGood {
	idx := gid % self.size

	return self.buckets[idx].Get(gid, creator)
}

func (self *BucketMgr) BroadCast(msg interface{}) {
	for _, v := range self.buckets {
		v.Cast(msg)
	}
}

func (self *BucketMgr) TotalCount() int {
	ret := 0
	for _, v := range self.buckets {
		ret = ret + v.Count()
	}
	return ret
}

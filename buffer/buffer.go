package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/letterbaby/manzo/logger"
)

type Buffer struct {
	Data   []byte
	bsize  int
	refcnt int32
}

type bufCacheInfo struct {
	maxbody int
	pool    *sync.Pool
}

func newBuffer(sz int) *Buffer {
	m := &Buffer{}
	m.Data = make([]byte, sz)
	m.bsize = sz
	return m
}

var bufCache = []bufCacheInfo{
	{
		maxbody: 64,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(64)
			},
		},
	}, {
		maxbody: 128,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(128)
			},
		},
	}, {
		maxbody: 256,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(256)
			},
		},
	}, {
		maxbody: 512,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(512)
			},
		},
	}, {
		maxbody: 1024,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(1024)
			},
		},
	}, {
		maxbody: 4096,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(4096)
			},
		},
	}, {
		maxbody: 8192,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(8192)
			},
		},
	}, {
		maxbody: 65536,
		pool: &sync.Pool{
			New: func() interface{} {
				return newBuffer(65536)
			},
		},
	},
}

func (self *Buffer) Free() {
	v := atomic.AddInt32(&self.refcnt, -1)
	if v > 0 {
		return
	}

	for i := range bufCache {
		if self.bsize == bufCache[i].maxbody {
			bufCache[i].pool.Put(self)
			return
		}
	}
}

// 增加引用
func (self *Buffer) Ref() {
	atomic.AddInt32(&self.refcnt, 1)
}

func NewBuffer(sz int) *Buffer {
	var buf *Buffer
	for i := range bufCache {
		if sz < bufCache[i].maxbody {
			buf = bufCache[i].pool.Get().(*Buffer)
			break
		}
	}
	if buf == nil {
		buf = newBuffer(sz)
	}

	v := atomic.AddInt32(&buf.refcnt, 1)
	if v != 1 {
		// 减去这一次胡
		atomic.AddInt32(&buf.refcnt, -1)
		logger.Error("NewBuffer buf:%v", buf)
		return nil
	}

	return buf
}

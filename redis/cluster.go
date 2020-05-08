package redis

import (
	"errors"
	"strconv"
	"sync"

	"github.com/gomodule/redigo/redis"

	"github.com/letterbaby/manzo/rand"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"
)

/*
C:\>redis-cli -c -p 7001
127.0.0.1:7001> CLUSTER SLOTS
1) 1) (integer) 10923
   2) (integer) 16383
   3) 1) "127.0.0.1"
      2) (integer) 7002
   4) 1) "127.0.0.1"
      2) (integer) 7005
2) 1) (integer) 0
   2) (integer) 5460
   3) 1) "127.0.0.1"
      2) (integer) 7000
   4) 1) "127.0.0.1"
      2) (integer) 7003
3) 1) (integer) 5461
   2) (integer) 10922
   3) 1) "127.0.0.1"
      2) (integer) 7004
   4) 1) "127.0.0.1"
      2) (integer) 7001
127.0.0.1:7001>
*/

const hashSlots = 16384

var (
	errNoNodeFound = errors.New("no node for cluster")
)

type slotMapping struct {
	start, end int
	nodes      []string // 索引0是主
}

type Cluster struct {
	sync.RWMutex

	startupNodes []string // 保证有节点可以查询到集群信息
	createPool   func(addr string) (*redis.Pool, error)
	pools        map[string]*redis.Pool
	mapping      [hashSlots][]string //槽对应的主从

	refreshing bool // 是不是需要刷新,当节点不可用的时候
}

// 初始化集群
func (self *Cluster) Init() error {
	self.pools = make(map[string]*redis.Pool, 0)
	for _, v := range self.startupNodes {
		self.pools[v] = nil
	}
	return self.refresh()
}

func (self *Cluster) updatePools(m []slotMapping) error {
	self.Lock()
	defer self.Unlock()

	oldpools := make(map[string]bool)
	for k, _ := range self.pools {
		oldpools[k] = false
	}

	for _, sm := range m {
		for _, node := range sm.nodes {
			if node == "" {
				continue
			}

			_, ok := self.pools[node]
			if !ok {
				self.pools[node] = nil
			}

			oldpools[node] = true
		}
		for i := sm.start; i <= sm.end; i++ {
			self.mapping[i] = sm.nodes
		}
	}

	// 删除没有用的
	for k, v := range oldpools {
		if v {
			continue
		}
		pool, ok := self.pools[k]
		if ok {
			if pool != nil {
				pool.Close()
			}
			delete(self.pools, k)
		}
	}
	return nil
}

func (self *Cluster) updateSlots(addr string) error {
	conn, err := self.getConnForAddr(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	vals, err := redis.Values(conn.Do("CLUSTER", "SLOTS"))
	if err != nil {
		return err
	}

	m := make([]slotMapping, 0, len(vals))
	for len(vals) > 0 {
		var slotRange []interface{}
		vals, err = redis.Scan(vals, &slotRange)
		if err != nil {
			return err
		}

		var start, end int
		slotRange, err = redis.Scan(slotRange, &start, &end)
		if err != nil {
			return err
		}

		sm := slotMapping{start: start, end: end}

		for len(slotRange) > 0 {
			var nodes []interface{}
			slotRange, err = redis.Scan(slotRange, &nodes)
			if err != nil {
				return err
			}

			var addr string
			var port int
			if _, err = redis.Scan(nodes, &addr, &port); err != nil {
				return err
			}
			sm.nodes = append(sm.nodes, addr+":"+strconv.Itoa(port))
		}

		m = append(m, sm)
	}

	return self.updatePools(m)
}

func (self *Cluster) refresh() error {
	addrs := make([]string, 0)

	getaddrs := func() {
		self.RLock()
		defer self.RUnlock()
		for k, _ := range self.pools {
			addrs = append(addrs, k)
			if len(addrs) >= 3 {
				break
			}
		}
	}

	// 用来查集群信息的随便取3个,读模式不分主从
	getaddrs()

	var lastErr error
	for _, addr := range addrs {
		err := self.updateSlots(addr)
		if err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func (self *Cluster) needsRefresh() error {
	self.Lock()

	if self.refreshing {
		self.Unlock()

		utils.ASyncWait(func() bool {
			self.RLock()
			defer self.RUnlock()
			return !self.refreshing
		})

		return nil
	}
	self.refreshing = true
	self.Unlock()

	err := self.refresh()

	self.Lock()
	self.refreshing = false
	self.Unlock()

	return err
}

func (self *Cluster) getConnForAddr(addr string) (redis.Conn, error) {
	logger.Debug("Cluster:getConnForAddr addr:%s", addr)

	self.RLock()
	pool, ok := self.pools[addr]
	self.RUnlock()

	if ok && pool != nil {
		return pool.Get(), nil
	}

	newPool, err := self.createPool(addr)
	if err != nil {
		return nil, err
	}

	self.Lock()
	pool, _ = self.pools[addr]
	// 有可能另外一个携程 加过了
	if pool != nil {
		// 释放新的
		self.Unlock()

		newPool.Close()
		return pool.Get(), nil
	}
	self.pools[addr] = newPool
	self.Unlock()

	return newPool.Get(), nil
}

// replicas读优先从库
func (self *Cluster) getConnForSlot(slot int, replicas bool) (redis.Conn, error) {
	self.RLock()

	addrs := self.mapping[slot]

	if len(addrs) == 0 {
		self.RUnlock()
		return nil, errNoNodeFound
	}

	//第0位默认是主, 取从的库
	addr := addrs[0]
	if replicas && len(addrs) > 1 {
		if len(addrs) == 2 {
			addr = addrs[1]
		} else {
			idx := rand.RandInt(1, int32(len(addrs)-1))
			addr = addrs[idx]
		}
	}
	self.RUnlock()

	return self.getConnForAddr(addr)
}

func (self *Cluster) Close() {
	self.Lock()
	defer self.Unlock()

	for _, v := range self.pools {
		if v != nil {
			v.Close()
		}
	}
}

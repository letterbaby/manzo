package redis

import (
	"errors"
	"fmt"
	"time"

	"github.com/letterbaby/manzo/logger"

	"github.com/gomodule/redigo/redis"
)

type Config struct {
	ServerAddress []string `json:"serveraddress"` // []{1,2,3}
	MaxIdle       int      `json:"-"`
	MaxActive     int      `json:"-"`
	IdleTimeOut   int      `json:"-"`
	Password      string   `json:"password"` // 密码
	Dbase         string   `json:dbase`
}

var (
	noArgsFound = errors.New("no args found")
)

type IRedis interface {
	Refresh() // 如果手动增加从了，需要刷新
	Close()   // 关闭
	//Do(cmd string, args ...interface{})(interface{}, error)
	Hset(args ...interface{}) (err error)
	Hget(args ...interface{}) (ret interface{}, err error)
	RegScript(sh string, kc int, sc string) (err error)                 // 注册脚本
	Script(sh string, args ...interface{}) (ret interface{}, err error) // 执行脚本
	Hgetall(args ...interface{}) (ret []interface{}, err error)
	Expire(args ...interface{}) (err error)
	Incr(args ...interface{}) (ret int64, err error)
	Set(args ...interface{}) (err error)
	Get(args ...interface{}) (ret interface{}, err error)
}

type RedisCluster struct {
	cfg     *Config
	cluster *Cluster
	scripts map[string]*redis.Script
}

func NewRedisCluster(cfg *Config) IRedis {
	r := &RedisCluster{}
	r.Init(cfg)
	return r
}

func (self *RedisCluster) Init(cfg *Config) {
	self.cfg = cfg

	self.scripts = make(map[string]*redis.Script)

	self.cluster = &Cluster{}
	self.cluster.startupNodes = self.cfg.ServerAddress

	if self.cfg.MaxIdle <= 0 {
		self.cfg.MaxIdle = 1024
		logger.Warning("RedisCluster:init MaxIdle <= 0 defalut 1024")
	}

	if self.cfg.MaxActive <= 0 {
		self.cfg.MaxActive = 10240
		logger.Warning("RedisCluster:init MaxActive <= 0 defalut 10240")
	}

	self.cluster.createPool = func(addr string) (*redis.Pool, error) {
		return &redis.Pool{
			MaxIdle:     self.cfg.MaxIdle,
			MaxActive:   self.cfg.MaxActive, // 0代表无限大
			IdleTimeout: time.Minute,
			Wait:        true,
			Dial: func() (redis.Conn, error) {

				return redis.Dial("tcp", addr,
					redis.DialPassword(self.cfg.Password),
					//集群模式下不支持selet
					//redis.DialDatabase(self.cfg.Dbase),
					redis.DialConnectTimeout(1*time.Second),
					redis.DialReadTimeout(1*time.Second),
					redis.DialWriteTimeout(1*time.Second))
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}, nil
	}

	err := self.cluster.Init()
	if err != nil {
		logger.Fatal("RedisCluster:init msg:%v", err)
	}
}

func (self *RedisCluster) Refresh() {
	if self.cluster != nil {
		self.cluster.needsRefresh()
	}
}

func (self *RedisCluster) Close() {
	if self.cluster != nil {
		self.cluster.Close()
	}
}

func (self *RedisCluster) Do(cmd string, replicas bool, args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, noArgsFound
	}

	var lastErr error
	for i := 0; i < 2; i++ {
		key := args[0].(string)
		conn, err := self.cluster.getConnForSlot(slot(key), replicas)
		if err != nil {
			return nil, err
		}

		// 链接池分开？？？？？
		// 1、允许从从库读取的需求一般都是不及时的
		// 2、当从切换到主的时候pool要清一次，因为当前地址上的链接都是readonly的
		// 3、一个地址上绑定两个池，needsRefresh参数传入addr，将这个地址上的pool清一次
		if replicas {
			conn.Do("READONLY")
		}
		v, err := conn.Do(cmd, args...)

		if replicas {
			conn.Do("READWRITE")
		}

		conn.Close()

		lastErr = err
		if err != nil && err != redis.ErrNil {
			// 如果是MOVED也可以用新的地址在试一下
			// 有错都刷新一下吧??
			self.cluster.needsRefresh()
			time.Sleep(time.Second * 1)
			continue
		}
		return v, nil
	}
	return nil, lastErr
}

// 取唯一id
func (self *RedisCluster) RegScript(sh string, kc int, sc string) (err error) {
	_, ok := self.scripts[sh]
	if ok {
		return
	}

	//conn, err := self.cluster.getConnForSlot(slot("loadscript"), false)
	//if err != nil {
	//	return err
	//}
	//defer conn.Close()

	nsh := redis.NewScript(kc, sc)
	//err = nsh.Load(conn)
	//if err != nil {
	//	return err
	//}

	self.scripts[sh] = nsh

	return
}

/*
1、第一位必须是key,多个key注意数据迁移,最好同一个key
2、确实有不同key可以用{}解决
*/
func (self *RedisCluster) Script(sh string, args ...interface{}) (ret interface{}, err error) {
	if len(args) != 3 {
		err = noArgsFound
		logger.Error("RedisCluster:script msg:%v", args)
	}

	script, ok := self.scripts[sh]
	if !ok {
		return "", errNoNodeFound
	}

	key := args[0].(string) + self.cfg.Dbase + ":" + args[1].(string)
	conn, err := self.cluster.getConnForSlot(slot(key), false)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ret, err = script.Do(conn, key, args[2])
	if err != nil {
		logger.Error("RedisCluster:script msg:%s,p:%v", err.Error(), args)
	}

	return
}

func (self *RedisCluster) Hgetall(args ...interface{}) (ret []interface{}, err error) {
	if len(args) != 3 {
		err = noArgsFound
		logger.Error("RedisCluster:hgetall msg:%v", args)
	}

	key := args[1].(string) + self.cfg.Dbase + ":" + args[2].(string)
	ret, err = redis.Values(self.Do("HGETALL", args[0].(bool), key))
	if err != nil && err != redis.ErrNil {
		logger.Error("RedisCluster:hgetall msg:%s,p:%v", err.Error(), args)
	}

	return
}

// hset肯定是主
func (self *RedisCluster) Hset(args ...interface{}) (err error) {
	if len(args) != 4 {
		err = noArgsFound
		logger.Error("RedisCluster:hset msg:%v", args)
		return
	}

	key := args[0].(string) + self.cfg.Dbase + ":" + args[1].(string)
	_, err = self.Do("HSET", false, key, args[2], args[3])
	if err != nil {
		logger.Error("RedisCluster:hset msg:%s,p:%v", err.Error(), args)
	}

	return
}

// 重用的数据直接从主获取
func (self *RedisCluster) Hget(args ...interface{}) (ret interface{}, err error) {
	if len(args) != 4 {
		err = noArgsFound
		logger.Error("RedisCluster:hget msg:%v", args)
		return
	}

	key := args[1].(string) + self.cfg.Dbase + ":" + args[2].(string)
	ret, err = self.Do("HGET", args[0].(bool), key, args[3])
	if err != nil && err != redis.ErrNil {
		logger.Error("RedisCluster:hget msg:%s,p:%v", err.Error(), args)
	}

	return
}

func (self *RedisCluster) Expire(args ...interface{}) (err error) {
	if len(args) != 3 {
		err = noArgsFound
		logger.Error("RedisCluster:expire msg:%v", args)
		return
	}

	key := args[0].(string) + self.cfg.Dbase + ":" + args[1].(string)
	_, err = self.Do("EXPIRE", false, key, args[2])
	if err != nil && err != redis.ErrNil {
		logger.Error("RedisCluster:expire msg:%s,p:%v", err.Error(), args)
	}

	return
}

func (self *RedisCluster) Incr(args ...interface{}) (ret int64, err error) {
	if len(args) != 2 {
		err = noArgsFound
		logger.Error("RedisCluster:incr msg:%v", args)
		return
	}

	key := args[0].(string) + self.cfg.Dbase + ":" + args[1].(string)
	ret, err = redis.Int64(self.Do("INCR", false, key))
	if err != nil && err != redis.ErrNil {
		logger.Error("RedisCluster:incr msg:%s,p:%v", err.Error(), args)
	}

	return
}

func (self *RedisCluster) Set(args ...interface{}) (err error) {
	if len(args) != 3 {
		err = noArgsFound
		logger.Error("RedisCluster:set msg:%v", args)
		return
	}

	key := args[0].(string) + self.cfg.Dbase + ":" + args[1].(string)
	_, err = self.Do("SET", false, key, args[2])
	if err != nil {
		logger.Error("RedisCluster:set msg:%s,p:%v", err.Error(), args)
	}

	return
}

func (self *RedisCluster) Get(args ...interface{}) (ret interface{}, err error) {
	if len(args) != 3 {
		err = noArgsFound
		logger.Error("RedisCluster:get msg:%v", args)
		return
	}

	key := args[1].(string) + self.cfg.Dbase + ":" + args[2].(string)
	ret, err = self.Do("HGET", args[0].(bool), key)
	if err != nil && err != redis.ErrNil {
		logger.Error("RedisCluster:get msg:%s,p:%v", err.Error(), args)
	}

	return
}

// 哨兵、常规
//-------------------------------------------------------------------------------------

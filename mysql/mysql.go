package mysql

import (
	"strings"
	"sync"
	"time"

	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/utils"

	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
)

type IDBCmd interface {
	GetDBSn() int8
	//OnExcute() // 异步发送消息用
	OnExcuteSql(clt *DBClient)
	Dump() string

	GetW() chan bool
	NewW()
	Wait() bool
}

type MySyncDBCmd struct {
	w chan bool // 等待W

	Id  interface{} // int, string
	Sql string

	DbMgr *DBMgr
}

func (self *MySyncDBCmd) GetW() chan bool {
	return self.w
}

func (self *MySyncDBCmd) NewW() {
	self.w = make(chan bool, 1)
}

func (self *MySyncDBCmd) Wait() bool {
	select {
	case <-self.w:
		return true
	case <-time.After(time.Second * 2):
		logger.Warning("MySyncDBCmd:Wait id:%v,sql:%v", self.Id, self.Sql)
	}
	return false
}

// mysql连接,重连超时
const (
	dbtimeout time.Duration = 5
	cmd_size                = 10240 // 指令大小
)

// mysql连接配置
type Config struct {
	Connstr string `json:"connstr"`
	User    string `json:"user"`
	Passw   string `json:"passw"`
	Dbase   string `json:"dbase"`
	Count   int32  `json:"count"`
}

type DBClient struct {
	sn    int8       // 连接序号
	conn  mysql.Conn // mymysql连接
	disc  bool       // 连接是否断开
	dbmgr *DBMgr     // 数据库管理器

	pending chan IDBCmd
	dbterm  chan bool      // 数据执行退出信号
	dbwt    sync.WaitGroup // 数据安全等待锁
}

// 客户端初始化
func (self *DBClient) init(sn int8, cfg *Config, dbmgr *DBMgr) {
	self.dbterm = make(chan bool)

	self.sn = sn
	self.dbmgr = dbmgr

	self.pending = make(chan IDBCmd, cmd_size)

	cons := strings.Split(cfg.Connstr, ",")

	self.conn = mysql.New("tcp", "", cons[0], cfg.User, cfg.Passw, cfg.Dbase)
	self.conn.SetTimeout(time.Second * dbtimeout)
	// UTF8
	self.conn.Register("SET NAMES utf8")
	// 尝试连接mysql
	err := self.conn.Connect()
	if err != nil {
		logger.Fatal("DBClient:init cfg:%v", cfg)
	}

	self.dbwt.Add(1)
	go self.run()
}

// 增加数据访问指令
// 1.cmd 指令对象
func (self *DBClient) addReq(cmd IDBCmd) bool {
	select {
	case self.pending <- cmd:
	default:
		logger.Warning("DBClient:addReq d:%v", cmd.Dump())
		return false
	}

	return true
}

// 数据库连接检查+重连
func (self *DBClient) check() error {
	if !self.disc {
		return nil
	}

	logger.Warning("DBClient:check Reconnect...")

	/*
		err := self.conn.Ping()
		if err == nil {
			return nil
		}
	*/

	now := time.Now().Unix()
	err := self.conn.Reconnect()
	if err == nil {
		self.disc = false
		return nil
	}
	logger.Error("DBClient:check sn:%d,i:%v", self.sn, err)

	st := time.Duration((time.Now().Unix() - now))
	time.Sleep(time.Second * (dbtimeout - st))
	return err
}

// 数据执行线程
func (self *DBClient) run() {
	defer utils.CatchPanic()
	defer self.dbwt.Done()

	for {
		// step1:连接检查
		err := self.check()
		if err != nil {
			// 关闭？
			continue
		}

		// step2:被动退出
		select {
		case <-self.dbterm:
			return

		case cmd, ok := <-self.pending:
			if !ok {
				logger.Error("DBClient:check pending <-")
				return
			}

			cmd.OnExcuteSql(self)
			self.dbmgr.addRep(cmd)
		}
	}
}

// 被动关闭, 注意数据完整性
func (self *DBClient) close() {
	// step1:数据完整性
	close(self.dbterm)
	self.dbwt.Wait()

	n := len(self.pending)
	//logger.Debug("DBClient close:%v", n)

	for i := 0; i < n; i++ {

		cmd, ok := <-self.pending
		if !ok {
			break
		}

		cmd.OnExcuteSql(self)
		self.dbmgr.addRep(cmd)
	}
	// step2:关闭连接
	self.conn.Close()
}

// 执行数据库查询,兼容一次重连
func (self *DBClient) ExcuteSql(sql string) ([]mysql.Row, mysql.Result, error) {
	rows, res, err := self.conn.Query(sql)
	if err == nil {
		return rows, res, err
	}

	// 重连一次
	err = self.conn.Reconnect()
	if err != nil {
		self.disc = true
		return nil, nil, err
	}

	rows, res, err = self.conn.Query(sql)
	if err == nil {
		return rows, res, err
	}

	// 连接断开了
	self.disc = true
	return nil, nil, err
}

// mysql_real_escape_string
func (self *DBClient) Escape(v string) string {
	return self.conn.Escape(v)
}

//-------------------------------------
// 数据访问管理器
type DBMgr struct {
	clts map[int8]*DBClient // 数据访问客户端列表

	pending chan IDBCmd    // 指令完成队列
	dbterm  chan bool      // 数据执行退出信号
	dbwt    sync.WaitGroup // 数据安全等待锁
}

func NewDBMgr(cfg *Config) *DBMgr {
	dbmgr := &DBMgr{}
	dbmgr.init(cfg)
	return dbmgr
}

// 数据访问管理器
func (self *DBMgr) init(cfg *Config) {
	self.dbterm = make(chan bool)

	self.clts = make(map[int8]*DBClient)

	for i := int32(0); i < cfg.Count; i++ {
		clt := new(DBClient)
		clt.init(int8(i), cfg, self)
		self.clts[int8(i)] = clt
	}
}

func (self *DBMgr) CltCount() int32 {
	return int32(len(self.clts))
}

// 增加执行完成队列
// 1.cmd 执行指令
func (self *DBMgr) addRep(cmd IDBCmd) {
	w := cmd.GetW()
	if w != nil {
		w <- true
	}
}

// 查找数据访问客户端
// 1.sn 客户端序列号
func (self *DBMgr) findClient(sn int8) *DBClient {
	v, ok := self.clts[sn]
	if ok {
		return v
	}
	return nil
}

// 增加数据访问指令,注意panic异常
// 1.cmd 指令对象
func (self *DBMgr) AddReq(cmd IDBCmd, sync bool) bool {
	if cmd == nil {
		return false
	}

	sn := cmd.GetDBSn()

	clt := self.findClient(sn)
	if clt == nil {
		logger.Error("DBMgr:AddReq sn:%d,i:%v", sn, cmd.Dump())
		return false
	}

	// 如果同步
	if sync {
		cmd.NewW()
	}

	if !clt.addReq(cmd) {
		return false
	}

	//等待
	if sync {
		return cmd.Wait()
	}
	return true
}

// 被动关闭, 注意数据完整性
func (self *DBMgr) Close() {
	// step1:等待数据访问退出
	for _, v := range self.clts {
		v.close()
	}

	close(self.dbterm)
	self.dbwt.Wait()
}

func (self *DBMgr) Escape(v string) string {
	if len(self.clts) > 0 {
		return self.clts[0].Escape(v)
	}
	return ""
}

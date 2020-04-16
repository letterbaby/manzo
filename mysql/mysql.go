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
	GetDBSn(cnt int32) int8
	OnExcute() // 异步发送消息用
	OnExcuteSql(clt *DBClient)
	Dump() string

	GetW() chan bool
	NewW()
	Wait() bool
}

type MySyncDBCmd struct {
	w chan bool // 等待W

	Id  interface{} // int64, string
	Sql string
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

// 取sn
func (self *MySyncDBCmd) GetDBSn(cnt int32) int8 {
	// int64 & string
	switch self.Id.(type) {
	case int64:
		return int8(self.Id.(int64) % int64(cnt))
	case string:
		return int8(int32(len(self.Id.(string))) % cnt)
	default:
		return 0
	}
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
	sn   int8       // 连接序号
	conn mysql.Conn // mymysql连接

	dbmgr *DBMgr // 数据库管理器

	pending chan IDBCmd
	dbterm  chan bool      // 数据执行退出信号
	dbwt    sync.WaitGroup // 数据安全等待锁

	disc bool // 假定有错都是连接断开
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

	now := time.Now().Unix()
	err := self.conn.Reconnect()
	if err != nil {
		logger.Error("DBClient:check sn:%d,i:%v", self.sn, err)

		st := time.Duration((time.Now().Unix() - now))
		time.Sleep(time.Second * (dbtimeout - st))
		return err
	}

	self.disc = false
	return nil
}

func (self *DBClient) tryExcute(cmd IDBCmd) {
	defer utils.CatchPanic()

	excute := func() bool {
		// 执行
		cmd.OnExcuteSql(self)

		if !self.disc {
			return true
		}

		logger.Warning("DBClient:tryExcute Reconnect...")
		err := self.conn.Reconnect()
		if err != nil {
			logger.Error("DBClient:tryExcute sn:%d,i:%v", self.sn, err)
			return true
		}

		self.disc = false
		return false
	}

	for i := 0; i < 2; i++ {
		if excute() {
			break
		}
	}
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

			self.tryExcute(cmd)
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

		self.tryExcute(cmd)
		self.dbmgr.addRep(cmd)
	}
	// step2:关闭连接
	self.conn.Close()
}

// 执行数据库查询,兼容一次重连
func (self *DBClient) ExcuteSql(sql string) ([]mysql.Row, mysql.Result, error) {
	rows, res, err := self.conn.Query(sql)
	if err != nil {
		self.disc = true
		return rows, res, err
	}

	return rows, res, nil
}

func (self *DBClient) ExcuteSqls(sqls []string) error {
	tr, err := self.conn.Begin()
	if err != nil {
		self.disc = true
		return err
	}

	query := func(sql string) error {
		_, _, err := tr.Query(sql)
		if err != nil {
			err2 := tr.Rollback()
			if err2 != nil {
				logger.Error("DBClient:ExcuteSqls s:%v,e:%v", sql, err2)
			}
			return err
		}
		return nil
	}

	for _, v := range sqls {
		err := query(v)
		if err != nil {
			self.disc = true
			return err
		}
	}

	err = tr.Commit()
	if err != nil {
		self.disc = true
		return err
	}
	return nil
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

	// clts * cmd_size?
	self.pending = make(chan IDBCmd, cmd_size*2)
	self.clts = make(map[int8]*DBClient)

	for i := int32(0); i < cfg.Count; i++ {
		clt := new(DBClient)
		clt.init(int8(i), cfg, self)
		self.clts[int8(i)] = clt
	}

	self.dbwt.Add(1)
	go self.run()
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
	} else {
		select {
		case self.pending <- cmd:
		default:
			logger.Warning("DBMgr:addRep d:%v", cmd.Dump())
		}
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

	sn := cmd.GetDBSn(self.CltCount())

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

// 数据执行线程
func (self *DBMgr) run() {
	defer utils.CatchPanic()
	defer self.dbwt.Done()

	for {
		// step1:被动退出
		select {
		case <-self.dbterm:
			return

		case cmd, ok := <-self.pending:
			if !ok {
				logger.Error("DBMgr pending <-")
				return
			}

			cmd.OnExcute()
		}
	}
}

// 被动关闭, 注意数据完整性
func (self *DBMgr) Close() {
	// step1:等待数据访问退出
	for _, v := range self.clts {
		v.close()
	}

	close(self.dbterm)
	self.dbwt.Wait()

	n := len(self.pending)
	//logger.Debug("DBMgr close:%v", n)

	// step2:数据完整性
	for i := 0; i < n; i++ {

		cmd, ok := <-self.pending
		if !ok {
			break
		}

		cmd.OnExcute()
	}
}

func (self *DBMgr) Escape(v string) string {
	if len(self.clts) > 0 {
		return self.clts[0].Escape(v)
	}
	return ""
}

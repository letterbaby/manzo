package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/letterbaby/manzo/logger"

	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
)

var (
	// 没有宏!!只有启动用到if
	ALERT_FIELD = "0000" // 采用 -ldflags "-X mysql.ALERT_FIELD=1111"
)

// 创建表工具
type MyOps struct {
	conn mysql.Conn // mymysql连接
	cfg  *Config    // 当前连接

	tbl  string // 当前使用表
	cols map[string]*MyFiled
}

func NewMyOps(cfg *Config) *MyOps {
	dbops := &MyOps{}
	dbops.init(cfg)
	return dbops
}

// 初始化工具
// 1.cfg 数据库配置
func (self *MyOps) init(cfg *Config) {

	cons := strings.Split(cfg.Connstr, ",")

	self.cfg = cfg
	self.conn = mysql.New("tcp", "", cons[0], cfg.User, cfg.Passw, cfg.Dbase)
	self.conn.SetTimeout(time.Second * dbtimeout)
	// UTF8
	self.conn.Register("SET NAMES utf8")
	// 尝试连接mysql
	err := self.conn.Connect()
	if err != nil {
		logger.Fatal("MyOps:init cfg:%v", cfg)
	}
}

// 去默认字段
func (self *MyOps) GetCols() map[string]*MyFiled {
	return self.cols
}

// 查找列名是否存在
func (self *MyOps) FindCol(col string) bool {
	_, ok := self.cols[col]

	return ok
}

// 指定当前要操作的表
func (self *MyOps) UseTable(table string) {
	self.tbl = table
	self.cols = make(map[string]*MyFiled, 0)
}

// 执行语句
func (self *MyOps) ExcuteSql(sql string) ([]mysql.Row, mysql.Result, error) {
	rows, res, err := self.conn.Query(sql)
	if err == nil {
		return rows, res, err
	}

	// 重连一次
	err = self.conn.Reconnect()
	if err != nil {
		return nil, nil, err
	}

	rows, res, err = self.conn.Query(sql)
	if err == nil {
		return rows, res, err
	}

	return nil, nil, err
}

// 创建表
func (self *MyOps) CreateTable(engine string, autoid int, comment string,
	idlst []string, idxlst []string, prmlst []string) bool {

	strids := ""
	for _, v := range idlst {
		strids = strids + "," + v
	}

	strprm := ""
	for _, v := range prmlst {
		strprm = "," + "`" + v + "`" + strprm
	}
	strids = strids + ", PRIMARY KEY ( `id`" + strprm + ")"

	stridx := ""
	for _, v := range idxlst {
		stridx = "," + "INDEX `idx_" + v + "`(`" + v + "`)" + stridx
	}
	strids = strids + stridx

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (`id` INT UNSIGNED AUTO_INCREMENT %s) ENGINE=%s DEFAULT CHARSET=utf8 COMMENT='%s' AUTO_INCREMENT=%d;", self.tbl, strids, engine, comment, autoid)
	_, _, err := self.ExcuteSql(sql)
	if err != nil {
		logger.Error("MyOps:CreateTable sql:%v,I:%v", sql, err)
		return false
	}

	sql = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s'", self.cfg.Dbase, self.tbl)
	rows, res, err := self.ExcuteSql(sql)
	if err != nil {
		logger.Fatal("MyOps:CreateTable sql:%v,i:%v", sql, err)
		return false
	}

	name := res.Map("column_name")
	for _, v := range rows {
		self.cols[v.Str(name)] = &MyFiled{Data: 0}
	}

	return true
}

// 增加表字段
func (self *MyOps) AlertField(field string, ftype string, comment string, dv interface{}, bin bool) bool {
	f, ok := self.cols[field]

	if ALERT_FIELD == "1111" {
		mod := ""
		if ok {
			mod = " MODIFY "
		} else {
			mod = " ADD COLUMN "
		}

		sql := fmt.Sprintf("ALTER TABLE `%s` %s %s %s COMMENT '%s'", self.tbl, mod, field, ftype, comment)
		_, _, err := self.ExcuteSql(sql)
		if err != nil {
			logger.Fatal("MyOps:AlertField i:%v", err)
		}

		self.cols[field] = &MyFiled{Data: dv, Bin: bin}

		return false
	} else if !ok {
		logger.Fatal("MyOps:AlertField field:%v,cmt:%v", field, comment)
		return false
	}

	// TODO: 验证数据库字段类型
	f.Data = dv
	f.Bin = bin
	return true
}

// 增加字段索引
func (self *MyOps) AddIndexKey(ftype string) {
	if ALERT_FIELD == "1111" {
		sql := fmt.Sprintf("ALTER TABLE `%s` %s", self.tbl, ftype)
		_, _, err := self.ExcuteSql(sql)
		if err != nil {
			//logger.Error("MyOps:AddIndexKey %v", err)
			//logger.Info("AddIndexKey sql:%v", sql)
		}
	}
}

// 加载数据
func (self *MyOps) LoadData(sql string) []map[string]interface{} {
	rows, res, err := self.ExcuteSql(sql)
	if err != nil {
		logger.Error("MyOps:LoadData sql:%v,i:%v", sql, err)
		return nil
	}

	fld := res.Fields()
	rl := make([]map[string]interface{}, 0)
	for _, v1 := range rows {

		rr := make(map[string]interface{})

		for _, v := range fld {
			idx := res.Map(v.Name)
			rr[v.Name] = v1[idx]
		}

		rl = append(rl, rr)
	}

	return rl
}

// 释放数据库连接
func (self *MyOps) Close() {
	self.conn.Close()
}

func (self *MyOps) Escape(v string) string {
	return self.conn.Escape(v)
}

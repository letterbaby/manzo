package mysql

import (
	"fmt"
	"strconv"

	//"github.com/letterbaby/manzo/utils"
	"github.com/letterbaby/manzo/logger"
)

// 支持int32, int64, string(bin)
type MyFiled struct {
	Data  interface{}
	Bin   bool // 是不是2进制字段 TODO:[]byte{}
	Dirty bool // 是不是有更新
}

type MyData struct {
	Id        interface{} // int64, string
	IdName    string
	TableName string

	// InitTbl初始化的
	Cols map[string]*MyFiled

	Dirty bool

	DbMgr *DBMgr
}

// update + insert
type uiCmd struct {
	MySyncDBCmd

	ok bool
}

const (
	// 结果
	Ret_nodata = 1 + iota
	Ret_data
	Ret_err
)

type loadCmd struct {
	MySyncDBCmd

	cols map[string]*MyFiled
	ret  int32
}

type delCmd struct {
	MySyncDBCmd

	ok bool
}

func (self *uiCmd) Dump() string {
	return fmt.Sprintf("uiCmd:Dump i:%v", self)
}

// DB执行完成
func (self *uiCmd) OnExcute() {
}

// 取信息
func (self *uiCmd) OnExcuteSql(clt *DBClient) {
	self.ok = false

	_, res, err := clt.ExcuteSql(self.Sql)
	if err != nil {
		logger.Error("uiCmd:OnExcuteSql sql:%s,i:%v", self.Sql, err)
		return
	}

	if res.AffectedRows() > 0 {
		self.ok = true
	} else {
		logger.Warning("uiCmd:OnExcuteSql sql:%s", self.Sql)
	}
}

func (self *loadCmd) Dump() string {
	return ""
}

// DB执行完成
func (self *loadCmd) OnExcute() {
}

// 取信息
func (self *loadCmd) OnExcuteSql(clt *DBClient) {
	rows, res, err := clt.ExcuteSql(self.Sql)
	if err != nil {
		logger.Error("loadCmd:OnExcuteSql sql:%s,i:%v", self.Sql, err)
		return
	}

	if len(rows) > 0 {
		// 赋值v
		for k, v := range self.cols {
			col := res.Map(k)
			// int32 & int64 & string
			switch v.Data.(type) {
			case int32:
				self.cols[k].Data = int32(rows[0].Int(col))
			case int64:
				self.cols[k].Data = rows[0].Int64(col)
			case string:
				if v.Bin {
					self.cols[k].Data = string(rows[0].Bin(col))
				} else {
					self.cols[k].Data = rows[0].Str(col)
				}
			default:
				logger.Error("loadCmd:OnExcuteSql i:Unknow deal data type")
			}
		}
		self.ret = Ret_data
	} else {
		self.ret = Ret_nodata
	}

	//logger.Debug("Cmd data:%v", self.lret)
}

func (self *delCmd) Dump() string {
	return fmt.Sprintf("delCmd:Dump i:%v", self)
}

// DB执行完成
func (self *delCmd) OnExcute() {
}

// 取信息
func (self *delCmd) OnExcuteSql(clt *DBClient) {
	self.ok = false
	_, res, err := clt.ExcuteSql(self.Sql)
	if err != nil {
		logger.Error("delCmd:OnExcuteSql sql:%s,i:%v", self.Sql, err)
		return
	}

	if res.AffectedRows() > 0 {
		self.ok = true
	} else {
		logger.Warning("delCmd:OnExcuteSql sql:%s", self.Sql)
	}
}

func (self *MyData) NewUiCmd(sql string) *uiCmd {
	uc := &uiCmd{}
	uc.Id = self.Id
	uc.Sql = sql
	return uc
}

func (self *MyData) NewDelCmd(sql string) *delCmd {
	dc := &delCmd{}
	dc.Id = self.Id
	dc.Sql = sql
	return dc
}

func (self *MyData) NewLoadCmd(sql string) *loadCmd {
	lc := &loadCmd{}
	lc.Id = self.Id
	lc.Sql = sql
	lc.cols = self.Cols
	return lc
}

// update
func (self *MyData) Save(sync bool, force bool) {
	if !self.Dirty {
		return
	}

	var str string

	for k, v := range self.Cols {

		if k == "id" || k == self.IdName {
			continue
		}

		if !force && !v.Dirty {
			continue
		}
		v.Dirty = false

		if len(str) > 0 {
			str = str + ","
		}

		str = str + "`" + k + "`"

		// int32 & int64 & string
		switch v.Data.(type) {
		case int32:
			// FormatInt
			str = str + "=" + strconv.Itoa(int(v.Data.(int32)))
		case int64:
			str = str + "=" + strconv.Itoa(int(v.Data.(int64)))
		case string:
			vv := v.Data.(string)
			if v.Bin {
				vv = self.DbMgr.Escape(vv)
			}
			str = str + "='" + vv + "'"
		default:
			logger.Error("MyData:Save i:Unknow deal data type")
		}
	}

	if len(str) == 0 {
		// TODO:多线程Dirty
		return
	}

	ssql := ""

	// int64 & string
	switch self.Id.(type) {
	case int64:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = %d", self.TableName, str, self.IdName, self.Id.(int64))
	case string:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = '%s'", self.TableName, str, self.IdName, self.Id.(string))
	default:
		logger.Error("MyData:Save ????%v", self.Id)
	}

	uc := self.NewUiCmd(ssql)
	if !self.DbMgr.AddReq(uc, sync) {
		logger.Error("MyData:Save sql:%s", ssql)
		return
	}

	//!!
	if sync && !uc.ok {
		return
	}
	self.Dirty = false
}

func (self *MyData) SaveFiled(field string, sync bool) {
	if !self.Dirty {
		return
	}

	if field == "id" || field == self.IdName {
		return
	}

	v, ok := self.Cols[field]
	if !ok {
		return
	}

	if !v.Dirty {
		return
	}
	v.Dirty = false

	var str string

	str = str + "`" + field + "`"

	// int32 & int64 & string
	switch v.Data.(type) {
	case int32:
		// FormatInt
		str = str + "=" + strconv.Itoa(int(v.Data.(int32)))
	case int64:
		str = str + "=" + strconv.Itoa(int(v.Data.(int64)))
	case string:
		vv := v.Data.(string)
		if v.Bin {
			vv = self.DbMgr.Escape(vv)
		}
		str = str + "='" + vv + "'"
	default:
		logger.Error("MyData:SaveFiled i:Unknow deal data type")
	}

	if len(str) == 0 {
		// TODO:多线程Dirty
		return
	}

	ssql := ""
	// int64 & string
	switch self.Id.(type) {
	case int64:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = %d", self.TableName, str, self.IdName, self.Id.(int64))
	case string:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = '%s'", self.TableName, str, self.IdName, self.Id.(string))
	default:
		logger.Error("MyData:SaveFiled ????%v", self.Id)
	}

	uc := self.NewUiCmd(ssql)
	if !self.DbMgr.AddReq(uc, sync) {
		logger.Error("MyData:SaveFiled sql:%s", ssql)
		return
	}

	//!!
	if sync && !uc.ok {
		return
	}
	self.Dirty = false
}

// create
func (self *MyData) Create() bool {
	ssql := ""

	// int64 & string
	switch self.Id.(type) {
	case int64:
		ssql = fmt.Sprintf("insert into `%s`(`%s`) values(%d)", self.TableName, self.IdName, self.Id.(int64))
	case string:
		ssql = fmt.Sprintf("insert into `%s`(`%s`) values('%s')", self.TableName, self.IdName, self.Id.(string))
	default:
		logger.Error("MyData:Create ????%v", self.Id)
	}

	uc := self.NewUiCmd(ssql)
	if !self.DbMgr.AddReq(uc, true) {
		logger.Error("MyData:Create sql:%s", ssql)
		return false
	}

	if uc.ok {
		self.Cols[self.IdName].Data = self.Id
	}
	return uc.ok
}

func (self *MyData) Delete(sync bool) bool {
	ssql := ""
	// int64 & string
	switch self.Id.(type) {
	case int64:
		ssql = fmt.Sprintf("delete from `%s` where `%s`=%d", self.TableName, self.IdName, self.Id.(int64))
	case string:
		ssql = fmt.Sprintf("delete from `%s` where `%s`='%s'", self.TableName, self.IdName, self.Id.(string))
	default:
		logger.Error("MyData:Delete ????%v", self.Id)
	}

	dc := self.NewDelCmd(ssql)
	if !self.DbMgr.AddReq(dc, sync) {
		logger.Error("MyData:Delete sql:%s", ssql)
		return false
	}

	if sync {
		return dc.ok
	}
	return true
}

func (self *MyData) Insert(sync bool) bool {
	if !self.Dirty {
		return false
	}

	var key string
	var val string

	for k, v := range self.Cols {

		// || k == self.IdName
		if k == "id" {
			continue
		}

		if !v.Dirty {
			continue
		}

		v.Dirty = false

		if len(key) > 0 {
			key = key + ","
		}
		key = key + "`" + k + "`"

		if len(val) > 0 {
			val = val + ","
		}

		// int32 & int64 & string
		switch v.Data.(type) {
		case int32:
			// FormatInt
			val = val + strconv.Itoa(int(v.Data.(int32)))
		case int64:
			val = val + strconv.Itoa(int(v.Data.(int64)))
		case string:
			vv := v.Data.(string)
			if v.Bin {
				vv = self.DbMgr.Escape(vv)
			}
			val = val + "'" + vv + "'"
		default:
			logger.Error("MyData:Insert i:Unknow deal data type")
		}
	}

	ssql := fmt.Sprintf("insert into `%s`(%s) values(%s)", self.TableName, key, val)

	//logger.Debug("Insert sql:%v", ssql)
	//aid := int32(self.GetInt(self.IdName))
	// autoId
	//aid := tools.Rand(0, self.DbMgr.CltCount())

	//logger.Debug("Insert aid:%v", aid)

	uc := self.NewUiCmd(ssql)
	if !self.DbMgr.AddReq(uc, sync) {
		logger.Error("MyData:Insert sql:%s", ssql)
		return false
	}

	if sync && !uc.ok {
		return false
	}

	self.Dirty = false
	return true
}

// 有可能数据库断开，不代表没有数据
func (self *MyData) Load() int32 {
	ssql := ""
	// int64 & string
	switch self.Id.(type) {
	case int64:
		ssql = fmt.Sprintf("select * from `%s` where `%s`=%d", self.TableName, self.IdName, self.Id.(int64))
	case string:
		ssql = fmt.Sprintf("select * from `%s` where `%s`='%s'", self.TableName, self.IdName, self.Id.(string))
	default:
		logger.Error("MyData:Load ????%v", self.Id)
	}

	lc := self.NewLoadCmd(ssql)
	if !self.DbMgr.AddReq(lc, true) {
		logger.Error("MyData:Load sql:%s", ssql)
		return Ret_err
	}

	//logger.Debug("Load data:%v", lret)
	return lc.ret
}

func (self *MyData) GetInt32(field string) int32 {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:GetInt32 field:%v", field)
		return 0
	}

	return v.Data.(int32)
}

func (self *MyData) SetInt32(field string, nv int32) {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:SetInt32 field:%v", field)
		return
	}

	if v.Data.(int32) == nv {
		return
	}

	//logger.Debug("User id:%v, Dirty field:%s, old:%v, new:%v", self.Id, field, v, nv)

	v.Dirty = true
	self.Cols[field].Data = nv
	self.Dirty = true
}

func (self *MyData) GetInt64(field string) int64 {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:GetInt64 field:%v", field)
		return 0
	}

	return v.Data.(int64)
}

func (self *MyData) SetInt64(field string, nv int64) {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:SetInt64 field:%v", field)
		return
	}

	if v.Data.(int64) == nv {
		return
	}

	//logger.Debug("User id:%v, Dirty field:%s, old:%v, new:%v", self.Id, field, v, nv)

	v.Dirty = true
	self.Cols[field].Data = nv
	self.Dirty = true
}

func (self *MyData) GetStr(field string) string {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:GetStr field:%v", field)
		return ""
	}

	return v.Data.(string)
}

func (self *MyData) SetStr(field string, nv string) {
	v, ok := self.Cols[field]
	if !ok {
		logger.Error("MyData:SetStr field:%v", field)
		return
	}

	if len(v.Data.(string)) == len(nv) && v.Data.(string) == nv {
		return
	}

	//logger.Debug("User id:%v, Dirty field:%s, old:%v, new:%v", self.Id, field, v, nv)
	v.Dirty = true
	self.Cols[field].Data = nv
	self.Dirty = true
}

package mysql

import (
	"fmt"
	"strconv"

	//"github.com/letterbaby/manzo/utils"
	"github.com/letterbaby/manzo/logger"
)

// 支持int,string(bin)
type MyFiled struct {
	Data  interface{}
	Bin   bool // 是不是2进制字段 TODO:[]byte{}
	Dirty bool // 是不是有更新
}

type MyData struct {
	Id        interface{} // int, string
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

// 取sn
func (self *uiCmd) GetDBSn() int8 {
	// int & string
	switch self.id.(type) {
	case int:
		return int8(int32(self.id.(int)) % self.DbMgr.CltCount())
	case string:
		return int8(int32(len(self.id.(string))) % self.DbMgr.CltCount())
	default:
		return 0
	}
}

// 取信息
func (self *uiCmd) OnExcuteSql(clt *DBClient) {
	self.ok = false

	_, res, err := clt.ExcuteSql(self.sql)
	if err != nil {
		logger.Error("uiCmd:OnExcuteSql sql:%s,i:%v", self.sql, err)
		return
	}

	if res.AffectedRows() > 0 {
		self.ok = true
	} else {
		logger.Warning("uiCmd:OnExcuteSql sql:%s", self.sql)
	}
}

func (self *loadCmd) Dump() string {
	return ""
}

// 取sn
func (self *loadCmd) GetDBSn() int8 {
	// int & string
	switch self.id.(type) {
	case int:
		return int8(int32(self.id.(int)) % self.DbMgr.CltCount())
	case string:
		return int8(int32(len(self.id.(string))) % self.DbMgr.CltCount())
	default:
		return 0
	}
}

// 取信息
func (self *loadCmd) OnExcuteSql(clt *DBClient) {
	rows, res, err := clt.ExcuteSql(self.sql)
	if err != nil {
		logger.Error("loadCmd:OnExcuteSql sql:%s,i:%v", self.sql, err)
		return
	}

	if len(rows) > 0 {
		// 赋值v
		for k, v := range self.cols {
			col := res.Map(k)

			// int & string
			switch v.Data.(type) {
			case int:
				self.cols[k].Data = rows[0].Int(col)
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

// 取sn
func (self *delCmd) GetDBSn() int8 {
	// int & string
	switch self.id.(type) {
	case int:
		return int8(int32(self.id.(int)) % self.DbMgr.CltCount())
	case string:
		return int8(int32(len(self.id.(string))) % self.DbMgr.CltCount())
	default:
		return 0
	}
}

// 取信息
func (self *delCmd) OnExcuteSql(clt *DBClient) {
	self.ok = false
	_, res, err := clt.ExcuteSql(self.sql)
	if err != nil {
		logger.Error("delCmd:OnExcuteSql sql:%s,i:%v", self.sql, err)
		return
	}

	if res.AffectedRows() > 0 {
		self.ok = true
	} else {
		logger.Warning("delCmd:OnExcuteSql sql:%s", self.sql)
	}
}

func (self *MyData) NewUiCmd(sql string) *uiCmd {
	uc := &uiCmd{}
	uc.id = self.Id
	uc.sql = sql
	uc.DbMgr = self.DbMgr
	return uc
}

func (self *MyData) NewDelCmd(sql string) *delCmd {
	dc := &delCmd{}
	dc.id = self.Id
	dc.sql = sql
	dc.DbMgr = self.DbMgr
	return dc
}

func (self *MyData) NewLoadCmd(sql string) *loadCmd {
	lc := &loadCmd{}
	lc.id = self.Id
	lc.sql = sql
	lc.cols = self.Cols
	lc.DbMgr = self.DbMgr
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

		// int & string
		switch v.Data.(type) {
		case int:
			str = str + "=" + strconv.Itoa(v.Data.(int))
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

	// int & string
	switch self.Id.(type) {
	case int:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = %d", self.TableName, str, self.IdName, self.Id.(int))
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

func (self *MyData) SaveFiled(filed string, sync bool) {
	if !self.Dirty {
		return
	}

	if filed == "id" || filed == self.IdName {
		return
	}

	v, ok := self.Cols[filed]
	if !ok {
		return
	}

	if !v.Dirty {
		return
	}
	v.Dirty = false

	var str string

	str = str + "`" + filed + "`"

	// int & string
	switch v.Data.(type) {
	case int:
		str = str + "=" + strconv.Itoa(v.Data.(int))
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
	// int & string
	switch self.Id.(type) {
	case int:
		ssql = fmt.Sprintf("update `%s` set %s where `%s` = %d", self.TableName, str, self.IdName, self.Id.(int))
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
	// int & string
	switch self.Id.(type) {
	case int:
		ssql = fmt.Sprintf("insert into `%s`(`%s`) values(%d)", self.TableName, self.IdName, self.Id.(int))
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
	// int & string
	switch self.Id.(type) {
	case int:
		ssql = fmt.Sprintf("delete from `%s` where `%s`=%d", self.TableName, self.IdName, self.Id.(int))
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

		// int & string
		switch v.Data.(type) {
		case int:
			val = val + strconv.Itoa(v.Data.(int))
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
	// int & string
	switch self.Id.(type) {
	case int:
		ssql = fmt.Sprintf("select * from `%s` where `%s`=%d", self.TableName, self.IdName, self.Id.(int))
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

func (self *MyData) GetInt(filed string) int {
	v, ok := self.Cols[filed]
	if !ok {
		logger.Error("MyData:GetInt filed:%v", filed)
		return 0
	}

	return v.Data.(int)
}

func (self *MyData) SetInt(filed string, nv int) {
	v, ok := self.Cols[filed]
	if !ok {
		logger.Error("MyData:SetInt filed:%v", filed)
		return
	}

	if v.Data.(int) == nv {
		return
	}

	//logger.Debug("User id:%v, Dirty filed:%s, old:%v, new:%v", self.Id, filed, v, nv)

	v.Dirty = true
	self.Cols[filed].Data = nv
	self.Dirty = true
}

func (self *MyData) GetStr(filed string) string {
	v, ok := self.Cols[filed]
	if !ok {
		logger.Error("MyData:GetStr filed:%v", filed)
		return ""
	}

	return v.Data.(string)
}

func (self *MyData) SetStr(filed string, nv string) {
	v, ok := self.Cols[filed]
	if !ok {
		logger.Error("MyData:SetStr filed:%v", filed)
		return
	}

	if len(v.Data.(string)) == len(nv) && v.Data.(string) == nv {
		return
	}

	//logger.Debug("User id:%v, Dirty filed:%s, old:%v, new:%v", self.Id, filed, v, nv)
	v.Dirty = true
	self.Cols[filed].Data = nv
	self.Dirty = true
}

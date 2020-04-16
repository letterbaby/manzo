package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/letterbaby/manzo/logger"

	"github.com/urfave/cli"

	"github.com/letterbaby/manzo/utils"
)

type (
	tbl_info struct {
		DB     string
		Name   string //表名
		IdName string // 关联ID
		Des    string // 描述
		Date   string // 日期
	}
	field_info struct {
		Id       string // 编号
		Name     string //字段名
		Protot   string // Proto类型
		Dbt      string // 数据库类型
		Unsigned string // 无符号
		Null     string //允许为NULL
		Default  string //默认值
		Protorw  string //Proto读写
		Index    string // 索引
		Des      string //描述

		Primary string // 是不是主键
	}
	struct_info struct {
		Tbl    *tbl_info
		Fields []*field_info
	}
)

const (
	TK_FIELD = iota
	TK_TABLE
)

type token struct {
	typ  int
	data interface{}
}

type Lexer struct {
	lines  []string
	lineno int
}

func (self *Lexer) init(r io.Reader) {
	bts, err := ioutil.ReadAll(r)
	if err != nil {
		logger.Error("%v", err)
	}

	// 清除注释
	re := regexp.MustCompile("(?m:^#(.*)$)")
	bts = re.ReplaceAllLiteral(bts, nil)

	// 按行读入源码
	scanner := bufio.NewScanner(bytes.NewBuffer(bts))
	for scanner.Scan() {
		self.lines = append(self.lines, scanner.Text())
	}

	self.lineno = 0

	//logger.Debug("%v", self.lines)
}

func (self *Lexer) next(tbl int) (*token, bool) {

	if self.lineno >= len(self.lines) {
		return nil, false
	}

	str := self.lines[self.lineno]

	//logger.Debug("%v", str)

	// 空行
	if len(str) == 0 {
		return nil, true
	}

	// 表开始
	if str == "---" {
		t := &token{}
		t.typ = TK_TABLE
		return t, true
	}

	//logger.Debug("!!%v, tbl:%v", str, tbl)
	// 不可能
	if tbl == 0 {
		return nil, true
	}

	//str = strings.Replace(str, "\t", "", 0)

	// 第一个表
	if tbl == 1 {
		t := &token{}
		t.typ = TK_FIELD
		tbl := &tbl_info{}
		t.data = tbl
		v, err := utils.SplitToStringArray(str, "|")
		if err != nil {
			logger.Error("%v", err)
			return nil, false
		}

		tbl.DB = v[0]
		tbl.Name = v[1]
		tbl.IdName = v[2]
		tbl.Des = v[3]
		tbl.Date = v[4]

		return t, true
	}

	// 第二表
	if tbl == 2 {
		t := &token{}
		t.typ = TK_FIELD
		fld := &field_info{}
		t.data = fld
		v, err := utils.SplitToStringArray(str, "|")
		if err != nil {
			logger.Error("%v", err)
			return nil, false
		}

		fld.Id = v[0]
		fld.Name = v[1]
		fld.Protot = v[2]
		fld.Dbt = v[3]
		fld.Unsigned = v[4]
		fld.Null = v[5]
		fld.Default = v[6]
		fld.Protorw = v[7]
		fld.Index = v[8]
		fld.Primary = v[9]
		fld.Des = v[10]

		return t, true
	}

	// 空行
	return nil, true
}

type Parser struct {
	lexer *Lexer
	info  struct_info
}

func (self *Parser) init(lex *Lexer) {
	self.lexer = lex
}

func (self *Parser) expr() {

	// 解析规则
	tbl := 0
	for {
		t, b := self.lexer.next(tbl)
		self.lexer.lineno = self.lexer.lineno + 1

		if !b {
			break
		}

		if t == nil {
			continue
		}

		if t.typ == TK_TABLE {
			for i := 0; i < 2; i++ {
				self.lexer.lineno = self.lexer.lineno + 1
			}
			tbl = tbl + 1
		} else if t.typ == TK_FIELD {
			if tbl == 1 {
				self.info.Tbl = t.data.(*tbl_info)
			} else if tbl == 2 {
				self.info.Fields = append(self.info.Fields, t.data.(*field_info))
			}
		}
	}

	//logger.Debug("!!%v", self.info)
}

func upperfc(str string) string {

	strs := []byte(str)
	if len(str) > 0 {
		strs[0] = strings.ToUpper(str)[0]
	}
	return string(strs)
}

func ftype(fld *field_info) string {
	str := fld.Dbt
	if fld.Unsigned == "y" {
		str = str + " UNSIGNED"
	}

	if fld.Null == "-" {
		str = str + " NOT NULL"
	}

	if fld.Default != "-" {
		str = str + " DEFAULT " + fld.Default
	}

	return str
}

func intx(str string) bool {
	if str == "int32" || str == "int64" {
		return true
	}
	return false
}

func int32x(str string) bool {
	if str == "int32" {
		return true
	}
	return false
}

func int64x(str string) bool {
	if str == "int64" {
		return true
	}
	return false
}

func protor(str string) bool {
	return strings.ContainsAny(str, "r")
}

func blob(str string) bool {
	return strings.ContainsAny(str, "BLOB")
}

func protow(str string) bool {
	return strings.ContainsAny(str, "w")
}

func fmtf(f *field_info) string {
	return fmt.Sprintf("`%s` %s COMMENT '%s'", f.Name, ftype(f), f.Des)
}

func isindex(f *field_info) bool {
	if f.Index == "y" {
		return true
	}

	return false
}

func isprm(f *field_info) bool {
	if f.Primary == "y" {
		return true
	}
	return false
}

func main() {
	logger.Start("")

	app := cli.NewApp()
	app.Name = "Protocol Data Structure Generator"
	app.Usage = "tbltool -f=*.md -po=./*.proto -do=./*.go"
	app.Authors = []cli.Author{{Name: "walter.xu"}}
	app.Version = "1.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "file,f", Value: "", Usage: "input *.md file"},

		cli.StringFlag{Name: "proto,p", Value: "./templates/proto.tmpl", Usage: "template file"},
		cli.StringFlag{Name: "pout,po", Value: "", Usage: "pout *.proto file"},

		cli.StringFlag{Name: "data,d", Value: "./templates/data.tmpl", Usage: "template file"},
		cli.StringFlag{Name: "dout,do", Value: "", Usage: "dout *.go file"},

		cli.StringFlag{Name: "sql,s", Value: "./templates/sql.tmpl", Usage: "template file"},
		cli.StringFlag{Name: "sout,so", Value: "", Usage: "sout *.sql file"},
	}
	app.Action = func(c *cli.Context) error {

		pout, err := os.Create(c.String("pout"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		defer pout.Close()

		dout, err := os.Create(c.String("dout"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		defer dout.Close()

		sout, err := os.Create(c.String("sout"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		defer sout.Close()
		// parse
		file, err := os.Open(c.String("file"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		defer file.Close()
		lexer := Lexer{}
		lexer.init(file)

		p := Parser{}
		p.init(&lexer)
		p.expr()

		// use template to generate final output
		funcMap := template.FuncMap{
			"upperfc": upperfc,
			"ftype":   ftype,
			"intx":    intx,
			"int32x":  int32x,
			"int64x":  int64x,
			"protor":  protor,
			"protow":  protow,
			"blob":    blob,
			"fmtf":    fmtf,
			"isindex": isindex,
			"isprm":   isprm,
		}

		fileName := filepath.Base(c.String("proto"))

		tmpl, err := template.New(fileName).Funcs(funcMap).ParseFiles(c.String("proto"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}

		err = tmpl.Execute(pout, p.info)
		if err != nil {
			logger.Error("%v", err)
			return nil
		}

		fileName = filepath.Base(c.String("data"))
		tmpl, err = template.New(fileName).Funcs(funcMap).ParseFiles(c.String("data"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		err = tmpl.Execute(dout, p.info)
		if err != nil {
			logger.Error("%v", err)
			return nil
		}

		fileName = filepath.Base(c.String("sql"))
		tmpl, err = template.New(fileName).Funcs(funcMap).ParseFiles(c.String("sql"))
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		err = tmpl.Execute(sout, p.info)
		if err != nil {
			logger.Error("%v", err)
			return nil
		}
		return nil
	}
	app.Run(os.Args)
}

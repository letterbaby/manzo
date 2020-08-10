package logger

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"
)

type Level int32

var Level_name = map[Level]string{
	1: "debug",
	2: "info",
	3: "warning",
	4: "error",
	5: "fatal",
}

func (self Level) String() string {
	return Level_name[self]
}

func getFileNameAndExt(fullName string) (string, string) {
	filenameWithSuffix := path.Base(fullName)
	fileSuffix := path.Ext(filenameWithSuffix)
	filenameOnly := strings.TrimSuffix(filenameWithSuffix, fileSuffix)
	return filenameOnly, fileSuffix
}

const (
	DEBUG Level = iota + 1
	INFO
	WARNING
	ERROR
	FATAL
	XEND
)

type Config struct {
	Console  bool   `json:"console"`  //是不是输出到控制台
	File     bool   `json:"file"`     // 是不是开启文件日志
	Rotating bool   `json:"rotating"` // 文件日志是不是要按大小
	Classify bool   `json:"classify"` // 是不是开启level分类
	NameRule int32  `json:"namerule"` // 名字规则
	MaxSize  int64  `json:"maxsize"`
	Dir      string `json:"dir"`
	Name     string `json:"name"`
	Level    int32  `json:"level"` // 日志等级
}

type Handler interface {
	//init(classify Level, cfg *Config)
	LogMsg(lvl Level, msg string)
	close()
}

type OnLenCall func(len int64) bool

// 用golang的log库
type LogHandler struct {
	lg *log.Logger

	msgs   chan string
	closed chan bool

	cfg *Config

	classifyLvl Level // 分类等级

	totalLen int64

	OnLen OnLenCall
}

func (self *LogHandler) path(dir string, name string, rule int32) string {
	now := time.Now()

	fp := fmt.Sprintf("%s/%s", dir, name)

	if rule == 1 {
		fp = fmt.Sprintf("%s/%d%02d%02d_%02d_%02d_%02d_%s",
			dir,
			now.Year(),
			now.Month(),
			now.Day(),
			now.Hour(),
			now.Minute(),
			now.Second(), name)
	} else {
		_, err := os.Stat(fp)
		if err == nil || !os.IsNotExist(err) {
			for i := 1; i < 9999999; i++ {
				np := fmt.Sprintf("%s.%d", fp, i)
				_, err = os.Stat(np)
				if err != nil && os.IsNotExist(err) {
					err = os.Rename(fp, np)
					if err == nil {
						break
					}
				}
			}
		}
	}
	return fp
}

func (self *LogHandler) async() {
	self.closed = make(chan bool, 0)
	self.msgs = make(chan string, 10240)

	go self.run()
}

func (self *LogHandler) run() {
	// ???
	for {
		select {
		case msg := <-self.msgs:
			self.lg.Output(0, msg)
			self.totalLen = self.totalLen + int64(len(msg))
			if self.OnLen != nil {
				if self.OnLen(self.totalLen) {
					self.totalLen = 0
				}
			}
		case <-self.closed:
			return
		}
	}
}

func (self *LogHandler) log(msg string) {
	select {
	case self.msgs <- msg:
	default:
		//???
	}
}

func (self *LogHandler) LogMsg(lvl Level, msg string) {
	if self.cfg.Classify && self.classifyLvl != lvl {
		return
	}

	self.log(msg)
}

func (self *LogHandler) fin() {
	self.closed <- true

	l := len(self.msgs)
	for i := 0; i < l; i++ {
		msg := <-self.msgs
		self.lg.Output(0, msg)
	}
}

func (self *LogHandler) close() {
	self.fin()
}

//---------------------------------------------------------------------------
type ConsoleHander struct {
	LogHandler
}

func NewConsoleHandler(cfg *Config) *ConsoleHander {
	h := &ConsoleHander{}
	h.init(cfg)
	return h
}

func (self *ConsoleHander) init(cfg *Config) {
	self.cfg = cfg
	self.lg = log.New(os.Stdout, "", log.LstdFlags)

	self.async()
}

type FileHandler struct {
	LogHandler
	logfile *os.File
}

//---------------------------------------------------------------------------
func NewFileHandler(classifyLvl Level, cfg *Config) *FileHandler {
	h := &FileHandler{}
	h.init(classifyLvl, cfg)
	return h
}

func (self *FileHandler) init(classifyLvl Level, cfg *Config) {
	self.cfg = cfg
	self.classifyLvl = classifyLvl

	na := self.cfg.Name
	if classifyLvl != 0 {
		a, e := getFileNameAndExt(na)
		na = a + "_" + classifyLvl.String() + e
	}

	logfile, _ := os.Create(self.path(self.cfg.Dir, na, self.cfg.NameRule))
	self.lg = log.New(logfile, "", log.LstdFlags)
	self.logfile = logfile

	self.async()
}

func (self *FileHandler) close() {
	self.fin()

	if self.logfile != nil {
		self.logfile.Close()
	}
}

//---------------------------------------------------------------------------
type RotatingHandler struct {
	LogHandler
	logfile *os.File
}

func NewRotatingHandler(classifyLvl Level, cfg *Config) *RotatingHandler {
	h := &RotatingHandler{}
	h.init(classifyLvl, cfg)
	return h
}

func (self *RotatingHandler) init(classifyLvl Level, cfg *Config) {
	self.cfg = cfg
	self.classifyLvl = classifyLvl

	na := self.cfg.Name
	if classifyLvl != 0 {
		a, e := getFileNameAndExt(na)
		na = a + "_" + classifyLvl.String() + e
	}

	logfile, _ := os.Create(self.path(self.cfg.Dir, na, self.cfg.NameRule))
	self.lg = log.New(logfile, "", log.LstdFlags)
	self.logfile = logfile

	self.OnLen = func(len int64) bool {
		if len < self.cfg.MaxSize*1024*1024 {
			return false
		}

		//!!!
		self.logfile.Close()

		self.logfile, _ = os.Create(self.path(self.cfg.Dir, na, self.cfg.NameRule))
		self.lg.SetOutput(self.logfile)
		return true
	}

	self.async()
}

func (self *RotatingHandler) close() {
	self.fin()

	//race1
	if self.logfile != nil {
		self.logfile.Close()
	}
}

type Logger struct {
	level    Level
	handlers []Handler
}

var (
	logger = &Logger{}
)

func loadCfg(cfgPath string) *Config {
	// 解析文件标准文件
	cfg := &Config{}

	fi, err := os.Open(cfgPath)
	if err != nil {
		return cfg
	}
	defer fi.Close()
	data, err := ioutil.ReadAll(fi)
	if err != nil {
		return cfg
	}

	err = json.Unmarshal(data, cfg)
	if err != nil {
		//??
		return cfg
	}

	return cfg
}

func Start(cfgPath string) {
	cfg := loadCfg(cfgPath)

	if cfg != nil {
		StartEx(cfg)
	}
}

func StartEx(cfg *Config) {
	if cfg.Level <= 0 {
		cfg.Level = 1
	}

	if !cfg.Console && !cfg.File {
		cfg.Console = true
	}

	logger.level = Level(cfg.Level)
	logger.handlers = make([]Handler, 0)

	if cfg.Console {
		h := NewConsoleHandler(&Config{})
		logger.handlers = append(logger.handlers, h)
	}

	if cfg.File {
		var h Handler
		if cfg.Classify {
			for i := logger.level; i < XEND; i++ {
				if cfg.Rotating {
					h = NewRotatingHandler(i, cfg)
				} else {
					h = NewFileHandler(i, cfg)
				}
				logger.handlers = append(logger.handlers, h)
			}
		} else {
			if cfg.Rotating {
				h = NewRotatingHandler(Level(0), cfg)
			} else {
				h = NewFileHandler(Level(0), cfg)
			}
			logger.handlers = append(logger.handlers, h)
		}
	}
}

func Close() {
	for i := range logger.handlers {
		logger.handlers[i].close()
	}
}

func Debug(format string, a ...interface{}) {
	if logger.level > DEBUG {
		return
	}

	f := "[DBG]" + format
	msg := fmt.Sprintf(f, a...)
	for i := range logger.handlers {
		logger.handlers[i].LogMsg(DEBUG, msg)
	}
}

func Info(format string, a ...interface{}) {
	if logger.level > INFO {
		return
	}

	f := "[INF]" + format
	msg := fmt.Sprintf(f, a...)
	for i := range logger.handlers {
		logger.handlers[i].LogMsg(INFO, msg)
	}
}

func Warning(format string, a ...interface{}) {
	if logger.level > WARNING {
		return
	}

	f := "[WRN]" + format
	msg := fmt.Sprintf(f, a...)
	for i := range logger.handlers {
		logger.handlers[i].LogMsg(WARNING, msg)
	}
}

func Error(format string, a ...interface{}) {
	if logger.level > ERROR {
		return
	}

	//不打开底层的callstack
	fstr := format + "\n%s"

	p := make([]interface{}, 0)
	p = append(p, a...)

	buf := make([]byte, 4096)
	l := runtime.Stack(buf, false)
	p = append(p, buf[:l])

	f := "[ERR]" + fstr
	msg := fmt.Sprintf(f, p...)
	for i := range logger.handlers {
		logger.handlers[i].LogMsg(ERROR, msg)
	}
}

func Fatal(format string, a ...interface{}) {
	//不打开底层的callstack
	fstr := format + "\n%s"

	p := make([]interface{}, 0)
	p = append(p, a...)

	buf := make([]byte, 4096)
	l := runtime.Stack(buf, false)
	p = append(p, buf[:l])

	f := "[FAT]" + fstr
	msg := fmt.Sprintf(f, p...)
	for i := range logger.handlers {
		logger.handlers[i].LogMsg(FATAL, msg)
		logger.handlers[i].close()
	}

	os.Exit(1)
}

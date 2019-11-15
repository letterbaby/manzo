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

var (
	LoggerConfig = &Config{}
)

type Handler interface {
	//init(classify Level, cfg *Config)

	Debug(format string, a ...interface{})
	Info(format string, a ...interface{})
	Warning(format string, a ...interface{})
	Error(format string, a ...interface{})
	Fatal(format string, a ...interface{})

	close()
}

// 用golang的log库
type LogHandler struct {
	lg *log.Logger

	msgs   chan string
	closed chan bool

	cfg *Config

	classifyLvl Level // 分类等级
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
	for {
		select {
		case msg := <-self.msgs:
			self.lg.Output(0, msg)
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

func (self *LogHandler) Debug(format string, a ...interface{}) {
	if self.cfg.Classify && self.classifyLvl != DEBUG {
		return
	}

	f := "[DBG]" + format
	self.log(fmt.Sprintf(f, a...))
	//self.lg.Output(0, fmt.Sprintf(f, a...))
}

func (self *LogHandler) Info(format string, a ...interface{}) {
	if self.cfg.Classify && self.classifyLvl != INFO {
		return
	}

	f := "[INF]" + format
	self.log(fmt.Sprintf(f, a...))
	//self.lg.Output(0, fmt.Sprintf(f, a...))
}

func (self *LogHandler) Warning(format string, a ...interface{}) {
	if self.cfg.Classify && self.classifyLvl != WARNING {
		return
	}

	f := "[WRN]" + format
	self.log(fmt.Sprintf(f, a...))
	//self.lg.Output(0, fmt.Sprintf(f, a...))
}

func (self *LogHandler) Error(format string, a ...interface{}) {
	if self.cfg.Classify && self.classifyLvl != ERROR {
		return
	}

	f := "[ERR]" + format
	self.log(fmt.Sprintf(f, a...))
	//self.lg.Output(0, fmt.Sprintf(f, a...))
}

func (self *LogHandler) Fatal(format string, a ...interface{}) {
	if self.cfg.Classify && self.classifyLvl != FATAL {
		return
	}

	f := "[FAT]" + format
	self.log(fmt.Sprintf(f, a...))
	//self.lg.Output(0, fmt.Sprintf(f, a...))
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

	self.async()

	go self.tick()
}

func (self *RotatingHandler) tick() {
	//CatchPanic
	timer := time.NewTicker(5 * time.Second)
	//???
	defer func() {
		timer.Stop()
	}()

	for {
		select {
		case <-timer.C:
			self.check()
		}
	}
}

func (self *RotatingHandler) close() {
	self.fin()

	//race1
	if self.logfile != nil {
		self.logfile.Close()
	}
}

func (self *RotatingHandler) check() {
	st, _ := self.logfile.Stat()
	if st.Size() < self.cfg.MaxSize {
		return
	}

	//race1
	lf := self.logfile
	defer func() {
		lf.Close()
	}()

	na := self.cfg.Name
	if self.classifyLvl != 0 {
		a, e := getFileNameAndExt(na)
		na = a + "_" + self.classifyLvl.String() + e
	}

	self.logfile, _ = os.Create(self.path(self.cfg.Dir, na, self.cfg.NameRule))
	self.lg.SetOutput(self.logfile)
}

type Logger struct {
	level    Level
	handlers []Handler
}

var (
	logger = &Logger{}
)

func Start(cfgPath string) {
	// 解析文件标准文件
	fi, err := os.Open(cfgPath)
	if err != nil {

	}
	defer fi.Close()
	data, err := ioutil.ReadAll(fi)
	if err != nil {
	}

	err = json.Unmarshal(data, LoggerConfig)
	if err != nil {
		//??
	}

	if LoggerConfig.Level <= 0 {
		LoggerConfig.Level = 1
	}

	if !LoggerConfig.Console && !LoggerConfig.File {
		LoggerConfig.Console = true
	}

	logger.level = Level(LoggerConfig.Level)
	logger.handlers = make([]Handler, 0)

	if LoggerConfig.Console {
		h := NewConsoleHandler(&Config{})
		logger.handlers = append(logger.handlers, h)
	}

	if LoggerConfig.File {
		var h Handler
		if LoggerConfig.Classify {
			for i := logger.level; i < XEND; i++ {
				if LoggerConfig.Rotating {
					h = NewRotatingHandler(i, LoggerConfig)
				} else {
					h = NewFileHandler(i, LoggerConfig)
				}
				logger.handlers = append(logger.handlers, h)
			}
		} else {
			if LoggerConfig.Rotating {
				h = NewRotatingHandler(Level(0), LoggerConfig)
			} else {
				h = NewFileHandler(Level(0), LoggerConfig)
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

	for i := range logger.handlers {
		logger.handlers[i].Debug(format, a...)
	}
}

func Info(format string, a ...interface{}) {
	if logger.level > INFO {
		return
	}

	for i := range logger.handlers {
		logger.handlers[i].Info(format, a...)
	}
}

func Warning(format string, a ...interface{}) {
	if logger.level > WARNING {
		return
	}

	for i := range logger.handlers {
		logger.handlers[i].Warning(format, a...)
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

	for i := range logger.handlers {
		logger.handlers[i].Error(fstr, p...)
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

	for i := range logger.handlers {
		logger.handlers[i].Fatal(fstr, p...)
		logger.handlers[i].close()
	}

	os.Exit(1)
}

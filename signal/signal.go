package signal

import (
	"os"
	"os/signal"

	"github.com/letterbaby/manzo/logger"
)

type fhandler func(s os.Signal)

type signalHandler struct {
	handlers map[os.Signal]fhandler
}

var (
	sighandler = &signalHandler{}
	c          = make(chan os.Signal)
)

func (self *signalHandler) init(sigs []os.Signal, h fhandler) {
	self.handlers = make(map[os.Signal]fhandler)
	for _, sig := range sigs {
		self.handlers[sig] = h
	}
}

func (self *signalHandler) handle(sig os.Signal) bool {
	_, ok := self.handlers[sig]
	if ok {
		self.handlers[sig](sig)
	}
	return ok
}

func Watch(sigs []os.Signal, h fhandler) {
	c := make(chan os.Signal)
	//接受信号通知
	signal.Notify(c, sigs...)

	// 等待
	for {
		h(<-c)
	}
}

func WatchEx(sigs []os.Signal, h fhandler) {
	sighandler.init(sigs, h)

	//接受信号通知
	signal.Notify(c)

	// 等待
	for {
		sig := <-c
		ok := sighandler.handle(sig)
		if !ok {
			logger.Warning("Unknown handler signal: %v", sig)
		}
	}
}

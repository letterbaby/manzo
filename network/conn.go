package network

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	//"github.com/letterbaby/manzo/logger"
	. "github.com/letterbaby/manzo/buffer"
	"github.com/letterbaby/manzo/logger"
)

var (
	ErrMsg = errors.New("message illegal")
	ErrSeq = errors.New("seq is wrong")
)

// Send做广播优化支持RawMessage跟Buffer
type IConn interface {
	Init(cfg *Config, conn net.Conn)
	RecvMsg() (*RawMessage, error)      // 读取
	SendMsg(interface{}) error          // 写入
	LocalAddr() net.Addr                // 本地地址
	RemoteAddr() net.Addr               // 远程地址
	SetReadDeadline(t time.Time) error  // 读超时
	SetWriteDeadline(t time.Time) error // 写超时
	Close()                             // 关闭
}

type Conn struct {
	conn net.Conn

	cfg *Config
	//packetCount uint32 // 对收到的包进行计数
}

func (self *Conn) Init(cfg *Config, conn net.Conn) {
	self.cfg = cfg
	self.conn = conn
}

func (self *Conn) Close() {
	self.conn.Close()
}

func (self *Conn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *Conn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *Conn) SetReadDeadline(t time.Time) error {
	return self.conn.SetReadDeadline(t)
}

func (self *Conn) SetWriteDeadline(t time.Time) error {
	return self.conn.SetWriteDeadline(t)
}

func (self *Conn) SendMsg(msg interface{}) error {
	var tbuf *Buffer

	now := time.Now()
	buf, ok := msg.(*Buffer)
	if ok {
		tbuf = buf
	} else {
		buf, err := self.cfg.Parser.Serialize(msg.(*RawMessage))
		if err != nil {
			return err
		}
		tbuf = buf
	}

	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("Conn:SendMsg tt:%v", tt)
	}
	//logger.Debug("Conn:sendMsg conn:%v,data:%v,buf:%v", self, len(tbuf.Data), tbuf)
	_, err := self.conn.Write(tbuf.Data)

	tbuf.Free()

	return err
}

func (self *Conn) check(buf *Buffer) error {
	/*if msg.Seq != self.packetCount {
		return nil, ErrSeq
	}*/
	//self.packetCount++

	return nil
}

func (self *Conn) RecvMsg() (*RawMessage, error) {
	var sz uint32
	err := binary.Read(self.conn, binary.BigEndian, &sz)
	if err != nil {
		return nil, err
	}

	if sz <= 0 || sz > 60*1024 {
		return nil, ErrMsg
	}

	buf := NewBuffer(int(sz))
	defer func() {
		buf.Free()
	}()

	buf.Data = buf.Data[0:sz]

	_, err = io.ReadFull(self.conn, buf.Data)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	msg, err := self.cfg.Parser.Deserialize(buf)
	if err != nil {
		return nil, err
	}
	//logger.Debug("Conn:recvMsg conn:%v,data:%v,buf:%v", self, len(buf.Data), buf)

	tt := time.Now().Sub(now)
	if tt > (time.Millisecond * 50) {
		logger.Warning("Conn:RecvMsg tt:%v", tt)
	}

	return msg, nil
}

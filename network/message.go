// 定义了消息的接口
package network

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/golang/protobuf/proto"

	. "github.com/letterbaby/manzo/buffer"
)

// 贯穿的流程太长不建议走回收！！！
// 例如broadcast时候多个conn都发送同一个rawmessage
type RawMessage struct {
	Seq     uint32
	MsgId   uint16
	MsgData interface{}
}

type IMessage interface {
	Serialize(msg *RawMessage) (*Buffer, error)
	Deserialize(buf *Buffer) (*RawMessage, error)
	Register(msgId uint16, msgData interface{}) error
	UnRegister(msgId uint16)
}

type SerializedCall func([]byte, interface{})

//-----------------------------------------------------------------------------
type ProtoMessage struct {
	msgMap map[uint16]reflect.Type // id池：主要用于识别id对应的结构

	zip int // -1:不压0:压,>0条件压
	// Hook
	OnSerialized SerializedCall
}

func NewProtocParser(zip int) IMessage {
	pm := &ProtoMessage{}
	pm.init(zip)
	return pm
}

func (self *ProtoMessage) init(zip int) {
	self.zip = zip
	self.msgMap = make(map[uint16]reflect.Type)
}

func (self *ProtoMessage) Register(msgId uint16, msgData interface{}) error {
	if _, ok := self.msgMap[msgId]; ok {
		return fmt.Errorf("msg has registered:%v", msgId)
	}
	self.msgMap[msgId] = reflect.TypeOf(msgData)
	return nil
}

func (self *ProtoMessage) UnRegister(msgId uint16) {
	delete(self.msgMap, msgId)
}

func (self *ProtoMessage) Serialize(msg *RawMessage) (*Buffer, error) {
	if msg.MsgData == nil {
		return nil, fmt.Errorf("ProtoMessage:ser msg:%v", msg)
	}

	var data []byte
	_, ok := msg.MsgData.(proto.Message)
	if ok {
		pd, err := proto.Marshal(msg.MsgData.(proto.Message))
		if err != nil {
			return nil, err
		}
		data = pd
	} else {
		// Hook透传的[]byte??
		pd, ok := msg.MsgData.([]byte)
		if !ok {
			return nil, fmt.Errorf("ProtoMessage:ser msg:%v", msg)
		}
		data = pd
	}

	if self.OnSerialized != nil {
		self.OnSerialized(data, msg.MsgData)
	}

	zip := byte(0)
	var buff []byte
	azip := func() error {
		var out bytes.Buffer
		zlibw, err := zlib.NewWriterLevel(&out, zlib.BestCompression)
		if err != nil {
			return err
		}

		_, err = zlibw.Write(data)
		if err != nil {
			zlibw.Close()
			return err
		}
		zlibw.Close()
		buff = out.Bytes()

		if len(buff) < len(data) {
			zip = 1
		} else {
			buff = data
		}
		return nil
	}

	if self.zip <= -1 {
		buff = data
	} else if self.zip == 0 {
		err := azip()
		if err != nil {
			return nil, err
		}
	} else if len(data) < self.zip {
		buff = data
	} else {
		err := azip()
		if err != nil {
			return nil, err
		}
	}

	// 长度+消息序号+消息id+压缩标志+包长
	sz := 4 + 4 + 2 + 1 + len(buff)
	buf := NewBuffer(int(sz))
	buf.Data = buf.Data[0:sz]

	binary.BigEndian.PutUint32(buf.Data[0:], uint32(sz-4))
	binary.BigEndian.PutUint32(buf.Data[4:], msg.Seq)
	binary.BigEndian.PutUint16(buf.Data[8:], msg.MsgId)
	buf.Data[10] = zip

	copy(buf.Data[11:], buff)

	return buf, nil
}

func (self *ProtoMessage) Deserialize(buf *Buffer) (*RawMessage, error) {
	msg := &RawMessage{}
	//sz := binary.BigEndian.Uint32(buf.Data)
	msg.Seq = binary.BigEndian.Uint32(buf.Data[0:4])
	msg.MsgId = binary.BigEndian.Uint16(buf.Data[4:6])
	zip := buf.Data[6]
	data := buf.Data[7:]

	var buff []byte
	if zip == 1 {
		in := bytes.NewBuffer(data)
		zlibr, err := zlib.NewReader(in)
		if err != nil {
			return nil, err
		}

		var out bytes.Buffer
		_, err = io.Copy(&out, zlibr)
		if err != nil {
			zlibr.Close()
			return nil, err
		}
		zlibr.Close()

		buff = out.Bytes()
	} else {
		buff = data
	}

	rtype, ok := self.msgMap[msg.MsgId]
	if !ok {
		return nil, fmt.Errorf("ProtoMessage:desc msg:%v", msg)
	}

	msgdata := reflect.New(rtype).Interface()
	err := proto.Unmarshal(buff, msgdata.(proto.Message))
	if err != nil {
		return nil, err
	}
	msg.MsgData = msgdata

	//logger.Debug("ProtoMessage:des msg:%v,buf:%v", msg, buf)

	return msg, nil
}

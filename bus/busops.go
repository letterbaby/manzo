package bus

import (
	"strings"

	"github.com/letterbaby/manzo/buffer"
	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/network"
)

// 服务器id规则  世界ID_功能ID_逻辑ID
func GetServerWorldFuncId(id string) string {
	ids := strings.Split(id, "_")
	if len(ids) != 3 {
		return "-1"
	}
	return ids[0] + "_" + ids[1]
}

func GetServerWorldId(id string) string {
	ids := strings.Split(id, "_")
	if len(ids) != 3 {
		return "-1"
	}
	return ids[0]
}

func GetServerFuncId(id string) string {
	ids := strings.Split(id, "_")
	if len(ids) != 3 {
		return "-1"
	}
	return ids[1]
}

func GetServerLogicId(id string) string {
	ids := strings.Split(id, "_")
	if len(ids) != 3 {
		return "-1"
	}
	return ids[2]
}

func NewBusRawMessage(msg *CommonMessage) *network.RawMessage {
	rmsg := &network.RawMessage{}
	rmsg.MsgId = uint16(Cmd_NONE)
	rmsg.MsgData = msg
	return rmsg
}

func NewRouteRawMessageIn(msg []byte, parser network.IMessage) *network.RawMessage {
	if len(msg) < 4 {
		logger.Error("NewRouteRawMessageIn sz:%v", len(msg))
		return nil
	}

	// 大端!!!!
	sz := uint32(msg[0])<<24 | uint32(msg[1])<<16 | uint32(msg[2])<<8 | uint32(msg[3])
	if len(msg) < int(sz+4) {
		logger.Error("NewRouteRawMessageIn sz:%v", sz)
		return nil
	}

	buf := buffer.NewBuffer(int(sz))
	defer func() {
		buf.Free()
	}()

	buf.Data = buf.Data[0:sz]
	copy(buf.Data, msg[4:])

	rmsg, err := parser.Deserialize(buf)
	if err != nil {
		logger.Error("NewRouteRawMessageIn sz:%v,info:%v", sz, err)
		return nil
	}
	return rmsg
}

func NewRouteRawMessageOut(destId int32, destSvr string,
	msg *network.RawMessage, parser network.IMessage) *network.RawMessage {
	buf, err := parser.Serialize(msg)
	if err != nil {
		logger.Error("NewRouteRawMessageOut i:%v", err)
		return nil
	}
	defer func() {
		buf.Free()
	}()

	r := &CommonMessage{}
	r.Code = Cmd_ROUTE_MSG

	r.RouteInfo = &RouteInfo{}
	r.RouteInfo.DestId = destId
	r.RouteInfo.DestSvr = destSvr
	r.RouteInfo.Msg = make([]byte, len(buf.Data))

	copy(r.RouteInfo.Msg, buf.Data)

	rmsg := NewBusRawMessage(r)
	return rmsg
}

package bus

import (
	fmt "fmt"
	"strconv"
	"strings"

	"github.com/letterbaby/manzo/buffer"
	"github.com/letterbaby/manzo/logger"
	"github.com/letterbaby/manzo/network"
)

/*
type SvrId string

func (self SvrId) Int64() int64 {
	return MakeServerIdByStr(string(self))
}
*/
//WorldId(256)| FuncId (256) | LogicId(65535)
// 	8        |	    8     |  16
// 服务器id规则  世界ID_功能ID_逻辑ID
func MakeServerIdByStr(id string) int64 {
	ids := strings.Split(id, "_")
	if len(ids) != 3 {
		return 0
	}

	wid, err := strconv.Atoi(ids[0])
	if err != nil {
		return 0
	}
	fid, _ := strconv.Atoi(ids[1])
	if err != nil {
		return 0
	}
	lid, _ := strconv.Atoi(ids[2])
	if err != nil {
		return 0
	}
	return MakeServerId(int64(wid), int64(fid), int64(lid))
}

func MakeServerId(wid int64, fid int64, lid int64) int64 {
	if wid < 0 || wid > 255 ||
		fid < 0 || fid > 255 ||
		lid < 0 || lid > 65535 {
		return 0
	}

	return int64(wid<<32 | fid<<24 | lid)
}

func GetServerWorldId(id int64) int64 {
	return int64(byte(id >> 32))
}

func GetServerFuncId(id int64) int64 {
	return int64(byte(id >> 24))
}

func GetServerLogicId(id int64) int64 {
	return int64(uint16(id))
}

func GetServerId(id int64) (int64, int64, int64) {
	return int64(byte(id >> 32)), int64(byte(id >> 24)), int64(uint16(id))
}

func GetServerIdStr(id int64) string {
	rt := fmt.Sprintf("%d_%d_%d",
		int64(byte(id>>32)), int64(byte(id>>24)), int64(uint16(id)))
	return rt
}

func MakeWorldFuncId(wid int64, fid int64) int64 {
	return int64(wid<<32 | fid<<24)
}

func GetWorldFuncId(id int64) int64 {
	wid, fid, _ := GetServerId(id)
	return int64(wid<<32 | fid<<24)
}

func IsSameWorldFuncId(sid int64, did int64) bool {
	sw, sf, _ := GetServerId(sid)
	dw, df, _ := GetServerId(did)
	return (sw == 0 || sw == dw) && (sf == 0 || sf == df)
}

//-------------------------------------------------------------------------------------
func NewBusRawMessage(msg *CommonMessage) *network.RawMessage {
	rmsg := &network.RawMessage{}
	rmsg.MsgId = uint16(Cmd_NONE)
	rmsg.MsgData = msg
	return rmsg
}

func NewRouteRawMessageIn(msg *network.RawMessage, parser network.IMessage) *network.RawMessage {
	msgdata := msg.MsgData.(*CommonMessage)

	d := msgdata.RouteInfo.Msg

	if len(d) < 4 {
		logger.Error("NewRouteRawMessageIn sz:%v", len(d))
		return nil
	}

	// 大端!!!!
	sz := uint32(d[0])<<24 | uint32(d[1])<<16 | uint32(d[2])<<8 | uint32(d[3])
	if len(d) < int(sz+4) {
		logger.Error("NewRouteRawMessageIn sz:%v", sz)
		return nil
	}

	buf := buffer.NewBuffer(int(sz))
	defer func() {
		buf.Free()
	}()

	buf.Data = buf.Data[0:sz]
	copy(buf.Data, d[4:])

	rmsg, err := parser.Deserialize(buf)
	if err != nil {
		logger.Error("NewRouteRawMessageIn sz:%v,info:%v", sz, err)
		return nil
	}

	rmsg.Seq = msg.Seq
	return rmsg
}

func NewRouteRawMessageOut(destSvr int64, destSt int64,
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
	r.RouteInfo.DestSt = destSt
	r.RouteInfo.DestSvr = destSvr
	r.RouteInfo.Msg = make([]byte, len(buf.Data))

	copy(r.RouteInfo.Msg, buf.Data)

	rmsg := NewBusRawMessage(r)

	rmsg.Seq = msg.Seq
	return rmsg
}

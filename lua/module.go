package lua

import (
	lua "github.com/yuin/gopher-lua"

	"github.com/letterbaby/manzo/lua/luar"
)

/*
	logger.Debug
	logger.XXXX
*/

type Module struct {
	name   string
	fields map[string]lua.LValue
	funcs  map[string]interface{}

	types map[string]interface{}
}

func NewModule(n string) *Module {
	return &Module{name: n,
		fields: make(map[string]lua.LValue),
		funcs:  make(map[string]interface{}),
		types:  make(map[string]interface{})}
}

func (self *Module) String(name, value string) {
	self.fields[name] = lua.LString(value)
}

func (self *Module) Number(name string, value float64) {
	self.fields[name] = lua.LNumber(value)
}

func (self *Module) Bool(name string, value bool) {
	self.fields[name] = lua.LBool(value)
}

func (self *Module) Func(name string, value interface{}) {
	self.funcs[name] = value
}

func (self *Module) Type(name string, value interface{}) {
	self.types[name] = value
}

func (self *Module) Load(s *lua.LState) {
	s.PreloadModule(self.name,
		func(state *lua.LState) int {
			// 都放在tbl里面
			tbl := state.NewTable()

			for k, v := range self.funcs {

				f := luar.New(state, v)
				if f != lua.LNil {
					state.SetField(tbl, k, f)
				}
			}

			for k, v := range self.types {
				t := luar.NewType(state, v)
				if t != lua.LNil {
					state.SetField(tbl, k, t)
				}
			}

			for k, v := range self.fields {
				state.SetField(tbl, k, v)
			}
			state.Push(tbl)
			return 1
		})
}

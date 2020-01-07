package container

type Pair struct {
	Key   interface{}
	Value interface{}
}

// goroutine unsafe
type ListMap struct {
	keys  []interface{}
	pairs map[interface{}]*Pair
}

func NewListMap() *ListMap {
	lm := &ListMap{}
	lm.keys = make([]interface{}, 0)
	lm.pairs = make(map[interface{}]*Pair)
	return lm
}

func (self *ListMap) Add(k interface{}, v interface{}) error {
	vv, ok := self.pairs[k]

	if ok {
		vv.Value = v
	} else {
		self.keys = append(self.keys, k)
		self.pairs[k] = &Pair{Key: k, Value: v}
	}

	return nil
}

func (self *ListMap) Del(k interface{}) error {
	delete(self.pairs, k)

	for i, v := range self.keys {
		if v == k {
			self.keys = append(self.keys[:i], self.keys[i+1:]...)
			break
		}
	}

	return nil
}

func (self *ListMap) DelTop() {
	if len(self.keys) < 1 {
		return
	}

	self.Del(self.keys[0])
}

func (self *ListMap) Get(k interface{}) (interface{}, bool, error) {
	if len(self.pairs) == 0 {
		return nil, false, nil
	}

	v, ok := self.pairs[k]
	if !ok {
		return nil, false, nil
	}

	return v.Value, ok, nil
}

func (self *ListMap) GetTop() (*Pair, bool) {
	if len(self.keys) < 1 {
		return nil, false
	}

	v, ok := self.pairs[self.keys[0]]
	return v, ok
}

func (self *ListMap) Iterator() <-chan *Pair {
	ch := make(chan *Pair, len(self.keys))
	go func() {
		defer close(ch)
		for _, v := range self.keys {
			vv, ok := self.pairs[v]
			if ok {
				ch <- vv
			}
		}
	}()
	return ch
}

func (self *ListMap) Len() int {
	return len(self.keys)
}

// list
func (self *ListMap) Values() []interface{} {
	ret := make([]interface{}, 0)
	for _, v := range self.keys {
		vv, ok := self.pairs[v]
		if ok {
			ret = append(ret, vv.Value)
		}
	}
	return ret
}

func (self *ListMap) Keys() []interface{} {
	return self.keys
}

func (self *ListMap) Pairs() map[interface{}]*Pair {
	return self.pairs
}

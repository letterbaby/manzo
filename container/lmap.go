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

func (lm *ListMap) Add(k interface{}, v interface{}) error {

	vv, ok := lm.pairs[k]

	if ok {
		vv.Value = v
	} else {
		lm.keys = append(lm.keys, k)
		lm.pairs[k] = &Pair{Key: k, Value: v}
	}

	return nil
}

func (lm *ListMap) Del(k interface{}) error {

	delete(lm.pairs, k)

	for i, v := range lm.keys {
		if v == k {
			lm.keys = append(lm.keys[:i], lm.keys[i+1:]...)
			break
		}
	}

	return nil
}

func (lm *ListMap) DelTop() {

	if len(lm.keys) < 1 {
		return
	}

	lm.Del(lm.keys[0])
}

func (lm *ListMap) Get(k interface{}) (interface{}, bool, error) {
	if len(lm.pairs) == 0 {
		return nil, false, nil
	}

	v, ok := lm.pairs[k]
	if !ok {
		return nil, false, nil
	}

	return v.Value, ok, nil
}

func (lm *ListMap) GetTop() (*Pair, bool) {
	if len(lm.keys) < 1 {
		return nil, false
	}

	v, ok := lm.pairs[lm.keys[0]]
	return v, ok
}

func (lm *ListMap) Iterator() <-chan *Pair {
	ch := make(chan *Pair, len(lm.keys))
	go func() {
		defer close(ch)
		for _, v := range lm.keys {
			vv, ok := lm.pairs[v]
			if ok {
				ch <- vv
			}
		}
	}()
	return ch
}

func (lm *ListMap) Len() int {
	return len(lm.keys)
}

// list
func (lm *ListMap) Values() []interface{} {
	ret := make([]interface{}, 0)
	for _, v := range lm.keys {
		vv, ok := lm.pairs[v]
		if ok {
			ret = append(ret, vv.Value)
		}
	}
	return ret
}

func (lm *ListMap) Keys() []interface{} {
	return lm.keys
}

func (lm *ListMap) Pairs() map[interface{}]*Pair {
	return lm.pairs
}

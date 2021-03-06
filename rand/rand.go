package rand

import (
	"math/rand"
	"time"
)

const (
	RAND_TEST = 10
)

type IWeight interface {
	Len() int           // 长度
	Weight(i int) int32 // 权重
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandGroup(p ...uint32) int {
	if p == nil {
		panic("args not found")
	}

	r := make([]uint32, len(p))
	for i := 0; i < len(p); i++ {
		if i == 0 {
			r[0] = p[0]
		} else {
			r[i] = r[i-1] + p[i]
		}
	}

	rl := r[len(r)-1]
	if rl == 0 {
		return 0
	}

	rn := uint32(rand.Int63n(int64(rl)))
	for i := 0; i < len(r); i++ {
		if rn < r[i] {
			return i
		}
	}

	panic("bug")
}

// [b1,b2]
func RandInt(b1, b2 int32) int32 {
	if b1 == b2 {
		return b1
	}

	min, max := int64(b1), int64(b2)
	if min > max {
		min, max = max, min
	}
	return int32(rand.Int63n(max-min+1) + min)
}

func RandIntN(b1, b2 int32, n uint32) []int32 {
	if b1 == b2 {
		return []int32{b1}
	}

	min, max := int64(b1), int64(b2)
	if min > max {
		min, max = max, min
	}
	l := max - min + 1
	if int64(n) > l {
		n = uint32(l)
	}

	r := make([]int32, n)
	m := make(map[int32]int32)
	for i := uint32(0); i < n; i++ {
		v := int32(rand.Int63n(l) + min)

		if mv, ok := m[v]; ok {
			r[i] = mv
		} else {
			r[i] = v
		}

		lv := int32(l - 1 + min)
		if v != lv {
			if mv, ok := m[lv]; ok {
				m[v] = mv
			} else {
				m[v] = lv
			}
		}

		l--
	}

	return r
}

// 返回的data索引
func RandWeight(data IWeight, c int) []int {
	total := int32(0)
	temp := make(map[int][]int32, 0)

	for i := 0; i < data.Len(); i++ {
		temp[i] = []int32{total, total + data.Weight(i)}
		total = total + data.Weight(i)
	}

	tst := 0
	rt := make([]int, 0)
	for len(rt) < c && tst < RAND_TEST {
		rv := RandInt(0, total)
		for k, v := range temp {
			if v[0] <= rv && rv < v[1] {
				rt = append(rt, k)
				break
			}
		}
		tst = tst + 1
	}
	return rt
}

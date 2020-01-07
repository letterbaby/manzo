package container

import (
	"fmt"
	"unicode/utf8"
)

// 线程不安全
type Trie struct {
	Root *TrieNode
}

type TrieNode struct {
	Children map[rune]*TrieNode // 子节点
	End      bool               // 是不是结束字符
	C        rune               // 当前字符
	HasSelf  bool               // 是否包含自己
}

func NewTrie() *Trie {
	r := &Trie{}
	r.init()
	return r
}

func (self *Trie) init() {
	self.Root = self.newNode(-1)
}

func (self *Trie) newNode(c rune) *TrieNode {
	tn := &TrieNode{}
	tn.Children = make(map[rune]*TrieNode)
	tn.C = -1
	return tn
}

func (self *Trie) Print(tn *TrieNode, c int, dep int) {
	if tn == nil {
		tn = self.Root
	}

	c = c + 1
	fmt.Println(tn)

	for _, v := range tn.Children {
		if c >= dep {
			break
		}
		self.Print(v, c, dep)
		c = 0
	}
}

func (self *Trie) Add(str string) {
	if len(str) < 1 {
		return
	}
	node := self.Root
	key := []rune(str)
	for i := 0; i < len(key); i++ {
		_, ok := node.Children[key[i]]
		if !ok {
			node.Children[key[i]] = self.newNode(key[i])
		}
		node = node.Children[key[i]]
	}
	node.End = true

	if len(key) == 1 {
		node.HasSelf = true
	}
}

func (self *Trie) algorithm(src string,
	cb func(s int, e int) bool) bool {

	node := self.Root

	key := []rune(src)
	slen := len(key)

	for i := 0; i < slen; i++ {
		_, ok := node.Children[key[i]]
		if !ok {
			continue
		}
		node = node.Children[key[i]]

		if node.End && node.HasSelf {
			if !cb(i, i) {
				return false
			}
		}

		for j := i + 1; j < slen; j++ {
			_, ok = node.Children[key[j]]
			if !ok {
				break
			}

			node = node.Children[key[j]]
			if node.End {
				if !cb(i, j) {
					return false
				}
				break
			}
		}
		node = self.Root
	}
	return true
}

func (self *Trie) Check(src string) bool {
	if len(src) < 1 {
		return true
	}

	return self.algorithm(src, func(s int, e int) bool {
		return false
	})
}

func (self *Trie) Replace(src string, ns string) string {
	if len(src) < 1 {
		return src
	}

	chars := []rune(src)
	c, _ := utf8.DecodeRuneInString(ns)

	self.algorithm(src, func(s int, e int) bool {
		for t := s; t <= e; t++ {
			chars[t] = c
		}
		return true
	})
	return string(chars)
}

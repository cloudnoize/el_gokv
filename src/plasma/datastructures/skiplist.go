package datastructures

import (
	"bytes"
	"math"

	"github.com/cloudnoize/el_gokv/src/plasma/probability"
	"github.com/cloudnoize/el_gokv/src/plasma/utils"
)

// return to user
type KV struct {
	Key     []byte
	Value   []byte
	Version uint64
}

type verValue struct {
	value   []byte
	version uint64
}

type node struct {
	levels    []*node
	key       []byte
	verValues Stack[verValue]
}

func newNode(height uint16) *node {
	return &node{
		levels: make([]*node, height),
	}
}

type SkipList struct {
	head         *node
	maxHeight    uint16
	estimatedCap uint64
	size         uint64
	p            float64
}

func NewSkipList(estimateCap uint64, p float64) *SkipList {
	utils.Assert(utils.IsPowerOf2(estimateCap), "Not a power of two")
	mh := uint16(math.Log2(float64(estimateCap)))
	return &SkipList{
		head:         newNode(mh),
		maxHeight:    mh,
		estimatedCap: estimateCap,
		p:            p,
	}
}

func (s SkipList) Size() uint64 {
	return s.size
}

func (s *SkipList) Put(key []byte, val []byte, version uint64) {
	ptrsToNewNode := make([]*node, s.maxHeight)
	ptrsFromNewNode := make([]*node, s.maxHeight)
	curr := s.head
	for lvl := int(s.maxHeight) - 1; lvl >= 0; lvl-- {
		next := curr.levels[lvl]
		for {
			if next == nil {
				//end of current lvl, all keys are smaller
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = nil
				break
			}
			res := bytes.Compare(key, next.key)
			if res == 0 {
				// 0 is equals, and equals means it's an update to the value
				utils.Assert(next.verValues.Top().version < version, "Input version is not higher than current version")
				next.verValues.Push(verValue{val, version})
				return
			}
			if res == -1 {
				//next key is bigger, this is the insertion point
				ptrsToNewNode[lvl] = curr
				ptrsFromNewNode[lvl] = next
				break
			}
			//next key is smaller, keep going
			curr = next
			next = curr.levels[lvl]
		}
	}
	defer func() { s.size++ }()
	nodeHeight := probability.Geometric(s.p) + 1
	node := newNode(uint16(nodeHeight))
	node.key = key
	node.verValues.Push(verValue{val, version})
	// insert the new node, make each layer poit to it and from it
	for i := 0; i < int(nodeHeight); i++ {
		ptrsToNewNode[i].levels[i] = node
		node.levels[i] = ptrsFromNewNode[i]
	}
}

// TODO also version
func (s SkipList) Get(key []byte) ([]byte, bool) {
	curr := s.head
	for lvl := int(s.maxHeight) - 1; lvl >= 0; lvl-- {
		next := curr.levels[lvl]
		for {
			if next == nil {
				//end of current lvl, all keys are smaller
				break
			}
			res := bytes.Compare(key, next.key)
			if res == 0 {
				return next.verValues.Top().value, true
			}
			if res == -1 {
				// next key is bigger,need to go down a level
				break
			}
			//next key is smaller, keep going
			curr = next
			next = curr.levels[lvl]
		}
	}
	return nil, false
}

type Iterator struct {
	curr *node
}

func (s *SkipList) Iterator() *Iterator {
	return &Iterator{curr: s.head.levels[0]}
}

func (it *Iterator) Next() {
	if it.curr != nil {
		it.curr = it.curr.levels[0]
	}
}

func (it *Iterator) Dref() *KV {
	if it.curr == nil {
		return nil
	}
	return &KV{Key: it.curr.key, Value: it.curr.verValues.Top().value, Version: it.curr.verValues.Top().version}
}

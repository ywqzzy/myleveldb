package skiplist

import (
	"math/rand"
	"mylevelDB/utils"
	"sync"
)

const (
	kMaxHeight = 12
	kBranching = 4
)

type SkipList struct {
	maxHeight  int
	head       *Node
	comparator utils.Comparator
	mu         sync.RWMutex
}

func New(comp utils.Comparator) *SkipList {
	skipList := &SkipList{
		maxHeight:  1,
		head:       newNode(nil, kMaxHeight),
		comparator: comp,
	}
	return skipList
}

func (sl *SkipList) Insert(key interface{}) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	_, prev := sl.findGreaterOrEqual(key)
	height := sl.randomHeight()
	if height > sl.maxHeight {
		for i := sl.maxHeight; i < height; i++ {
			prev[i] = sl.head
		}
		sl.maxHeight = height
	}
	x := newNode(key, height)
	for i := 0; i < height; i++ {
		x.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, x)
	}
}

func (sl *SkipList) Contains(key interface{}) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	x, _ := sl.findGreaterOrEqual(key)
	if x != nil && sl.comparator(x.key, key) == 0 {
		return true
	}
	return false
}

func (sl *SkipList) NewIterator() *Iterator {
	var it Iterator
	it.list = sl
	return &it
}

func (sl *SkipList) randomHeight() int {
	height := 1
	for height < kMaxHeight && (rand.Intn(kBranching) == 0) {
		height++
	}
	return height
}

func (sl *SkipList) findGreaterOrEqual(key interface{}) (*Node, [kMaxHeight]*Node) {
	var prev [kMaxHeight]*Node

	x := sl.head
	level := sl.maxHeight - 1
	for true {
		next := x.getNext(level)
		if sl.keyIsAfterNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				level--
			}
		}
	}
	return nil, prev
}

func (sl *SkipList) findLessThan(key interface{}) *Node {
	x := sl.head
	level := sl.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil || sl.comparator(next.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}

func (sl *SkipList) findLast() *Node {
	x := sl.head

	level := sl.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}

func (sl *SkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	return (n != nil) && (sl.comparator(n.key, key) < 0)
}

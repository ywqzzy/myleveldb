package skiplist

type Iterator struct {
	list *SkipList
	node *Node
}

func (it *Iterator) Valid() bool {
	return it.node != nil
}

func (it *Iterator) Key() interface{} {
	return it.node.key
}

func (it *Iterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.getNext(0)
}

func (it *Iterator) Prev() {
	it.list.mu.RLock()

	defer it.list.mu.RUnlock()

	it.node = it.list.findLessThan(it.node.key)
	if it.node == it.list.head {
		it.node = nil
	}
}

// advance to the first entry with a key >= target
func (it *Iterator) Seek(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
}

func (it *Iterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.getNext(0)
}

func (it *Iterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findLast()
	if it.node == it.list.head {
		it.node = nil
	}
}

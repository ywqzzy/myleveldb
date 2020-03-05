package sstable

import (
	"mylevelDB/internal"
	"mylevelDB/sstable/block"
)

type Iterator struct {
	table           *SStable
	dataBlockHandle BlockHandle
	dataIter        *block.Iterator
	indexIter       *block.Iterator
}

func (it *Iterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *Iterator) InternalKey() *internal.InternalKey {
	return it.dataIter.InternalKey()
}

func (it *Iterator) Next() {
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

func (it *Iterator) Prev() {
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

func (it *Iterator) Seek(target interface{}) {
	it.indexIter.Seek(target)
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

func (it *Iterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

func (it *Iterator) SeekToLast() {
	it.indexIter.SeekToLast()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

func (it *Iterator) initDataBlock() {
	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		var index IndexBlockHandle
		index.InternalKey = it.indexIter.InternalKey()
		tmpBlockHandle := index.GetBlockHandle()

		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {
			// no need to change anything
		} else {
			it.dataIter = it.table.readBlock(tmpBlockHandle).NewIterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}

		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *Iterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}

		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}

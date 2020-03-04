package memtable

import (
	"errors"
	"mylevelDB/internal"
	"mylevelDB/skiplist"
)

type MemTable struct {
	table *skiplist.SkipList
}

func New() *MemTable {
	return &MemTable{
		table: skiplist.New(internal.InternalKeyComparator),
	}
}

func (memTable *MemTable) NewIterator() *skiplist.Iterator {
	return memTable.table.NewIterator()
}

func (memTable *MemTable) Add(seq int64, valueType internal.ValueType, key, value []byte) {
	internalKey := internal.NewInternalKey(seq, valueType, key, value)
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) (bool, []byte, error) {
	lookupKey := internal.LookupKey(key)

	it := memTable.table.NewIterator()

	it.Seek(lookupKey)
	if it.Valid() {
		internalKey := it.Key().(*internal.InternalKey)

		if internal.UserKeyComparator(key, internalKey.UserKey()) == 0 {
			// check valueType
			if internalKey.ValueType() == internal.TypeValue {
				return true, internalKey.UserValue(), nil
			} else {
				return true, nil, errors.New("not found")
			}
		}
	}
	return false, nil, errors.New("not found")
}

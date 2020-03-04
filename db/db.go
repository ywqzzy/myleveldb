package db

import (
	"mylevelDB/internal"
	"mylevelDB/memtable"
	"sync/atomic"
)

type DB struct {
	seq int64
	mem *memtable.MemTable
}

func Open() *DB {
	return &DB{
		seq: 0,
		mem: memtable.New(),
	}
}

func (db *DB) Put(key, value []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, internal.TypeValue, key, value)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	found, value, err := db.mem.Get(key)

	if !found {
		//TODO
	}
	return value, err
}

func (db *DB) Delete(key []byte) error {
	seq := atomic.AddInt64(&db.seq, 1)
	db.mem.Add(seq, internal.TypeDeletion, key, nil)
	return nil
}

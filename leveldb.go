package mylevelDB

import "mylevelDB/db"

type LevelDB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

type Iterator interface {
	Valid() bool
	Key() []byte
	Next()
	Prev()
	// Advance to the first entry with a key >= target
	Seek(target []byte)

	SeekToFirst()

	SeekToLast()
}

func Open() LevelDB {
	return db.Open()
}

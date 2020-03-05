package sstable

import (
	"errors"
	"io"
	"mylevelDB/sstable/block"
	"os"
)

type SStable struct {
	index  *block.Block
	footer Footer
	file   *os.File
}

func Open(fileName string) (*SStable, error) {
	var table SStable

	var err error

	table.file, err = os.Open(fileName)

	if err != nil {
		return nil, err
	}

	stat, _ := table.file.Stat()

	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		return nil, errors.New("file is too short to be sstable")
	}

	_, err = table.file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	err = table.footer.DecodeFrom(table.file)
	if err != nil {
		return nil, err
	}
	table.index = table.readBlock(table.footer.IndexHandle)
	return &table, nil
}

func (table *SStable) NewIterator() *Iterator {
	var it Iterator
	it.table = table
	it.indexIter = table.index.NewIterator()

	return &it
}

func Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (table *SStable) readBlock(blockHandle BlockHandle) *block.Block {
	p := make([]byte, blockHandle.Size)

	n, err := table.file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}

	return block.New(p)
}

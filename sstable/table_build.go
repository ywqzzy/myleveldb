package sstable

import (
	"mylevelDB/internal"
	"mylevelDB/sstable/block"
	"os"
)

const (
	MAX_BLOCK_SIZE = 4 * 1024
)

type TableBuilder struct {
	file               *os.File
	offset             uint32
	numEntries         int32
	dataBlockBuilder   block.BlockBuilder
	indexBlockBuilder  block.BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	status             error
	//TODO  METABLOCK
}

func NewTableBuilder(fileName string) *TableBuilder {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil
	}
	builder.pendingIndexEntry = false
	return &builder
}

func (builder *TableBuilder) Add(internalKey *internal.InternalKey) {
	if builder.status != nil {
		return
	}

	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// TODO filter block  bloom filter
	builder.pendingIndexHandle.InternalKey = internalKey

	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MAX_BLOCK_SIZE {
		builder.flush()
	}
}

func (builder *TableBuilder) flush() {
	if builder.dataBlockBuilder.Empty() {
		return
	}

	builder.pendingIndexHandle.SetBlockHandle(builder.writeBlock(&builder.dataBlockBuilder))
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()

	// TODO filter block

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}

	var footer Footer

	footer.IndexHandle = builder.writeBlock(&builder.indexBlockBuilder)

	// write footer block, 40 byte
	footer.EncodeTo(builder.file)
	builder.file.Close()
	return nil
}

func (builder *TableBuilder) writeBlock(blockBuilder *block.BlockBuilder) BlockHandle {
	content := blockBuilder.Finish()

	// TODO  compress, crc

	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	builder.offset += uint32(len(content))
	_, builder.status = builder.file.Write(content)
	builder.file.Sync()
	blockBuilder.Reset()

	return blockHandle
}

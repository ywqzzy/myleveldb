package sstable

import (
	"bytes"
	"encoding/gob"
	"io"
	"mylevelDB/internal"
)

const (
	MAX_BLOCK_SIZE = 4 * 1024
)

type TableBuilder struct {
	writer             io.Writer
	offset             int
	numEntries         int
	dataBlock          *BlockBuilder
	indexBlock         *BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	status             error
	//TODO  METABLOCK
}

func NewTableBuilder(writer io.Writer) *TableBuilder {
	var builder TableBuilder
	builder.writer = writer
	builder.dataBlock = newBlockBuilder()
	builder.indexBlock = newBlockBuilder()
	return &builder
}

func (builder *TableBuilder) Add(internalKey *internal.InternalKey) {
	if builder.status != nil {
		return
	}

	if builder.pendingIndexEntry {
		builder.indexBlock.add(builder.pendingIndexHandle)
		builder.pendingIndexEntry = false
	}
	// TODO filter block  bloom filter
	builder.pendingIndexHandle.LastKey = internalKey.UserKey
	builder.numEntries++
	builder.dataBlock.add(internalKey)
	if builder.dataBlock.currentSizeEstimate() > MAX_BLOCK_SIZE {
		builder.flush()
	}
}

func (builder *TableBuilder) flush() {
	if builder.dataBlock.empty() {
		return
	}

	builder.pendingIndexHandle.BlockHandle = builder.writeBlock(builder.dataBlock)
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()

	// TODO filter block

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlock.add(builder.pendingIndexHandle)
		builder.pendingIndexEntry = false
	}

	var footer Footer

	footer.IndexHandle = builder.writeBlock(builder.indexBlock)

	// write footer block, 40 byte
	footerRaw := make([]byte, 40)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(footer)
	copy(footerRaw, buf.Bytes())

	builder.writer.Write(footerRaw)
	return nil
}

func (builder *TableBuilder) writeBlock(block *BlockBuilder) BlockHandle {
	content := block.finish()

	// TODO  compress, crc
	builder.writer.Write(content)

	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = len(content)
	builder.offset += len(content)
	_, builder.status = builder.writer.Write(content) // why 2 次调用

	block.reset()

	return blockHandle
}

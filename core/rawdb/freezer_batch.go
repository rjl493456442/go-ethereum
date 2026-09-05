// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
)

const (
	// This is the maximum amount of data that will be buffered in memory
	// for a single freezer table batch.
	freezerBatchBufferLimit = 2 * 1024 * 1024

	// freezerBatchPendingLimit is the maximum amount of uncompressed data that
	// is gathered in a compressed table batch before being compressed (in
	// parallel) and appended to the buffered output.
	freezerBatchPendingLimit = 32 * 1024 * 1024

	// freezerTableFlushThreshold defines the threshold for triggering a freezer
	// table sync operation. If the number of accumulated uncommitted items exceeds
	// this value, a sync will be scheduled.
	freezerTableFlushThreshold = 512
)

// freezerBatch is a write operation of multiple items on a freezer.
type freezerBatch struct {
	tables map[string]*freezerTableBatch
}

func newFreezerBatch(f *Freezer) *freezerBatch {
	batch := &freezerBatch{tables: make(map[string]*freezerTableBatch, len(f.tables))}
	for kind, table := range f.tables {
		batch.tables[kind] = table.newBatch()
	}
	return batch
}

// Append adds an RLP-encoded item of the given kind.
func (batch *freezerBatch) Append(kind string, num uint64, item interface{}) error {
	if table := batch.tables[kind]; table != nil {
		return table.Append(num, item)
	}
	return errUnknownTable
}

// AppendRaw adds an item of the given kind.
func (batch *freezerBatch) AppendRaw(kind string, num uint64, item []byte) error {
	if table := batch.tables[kind]; table != nil {
		return table.AppendRaw(num, item)
	}
	return errUnknownTable
}

// reset initializes the batch.
func (batch *freezerBatch) reset() {
	for _, tb := range batch.tables {
		tb.reset()
	}
}

// commit is called at the end of a write operation and
// writes all remaining data to tables.
func (batch *freezerBatch) commit() (item uint64, writeSize int64, err error) {
	// Check that count agrees on all batches.
	item = uint64(math.MaxUint64)
	for name, tb := range batch.tables {
		if item < math.MaxUint64 && tb.nextItem != item {
			return 0, 0, fmt.Errorf("table %s is at item %d, want %d", name, tb.nextItem, item)
		}
		item = tb.nextItem
	}

	// Commit all table batches.
	for _, tb := range batch.tables {
		if err := tb.commit(); err != nil {
			return 0, 0, err
		}
		writeSize += tb.totalBytes
	}
	return item, writeSize, nil
}

// freezerTableBatch is a batch for a freezer table.
type freezerTableBatch struct {
	t *freezerTable

	encBuffer   writeBuffer
	dataBuffer  []byte
	indexBuffer []byte
	curItem     uint64 // expected index of next item to be appended to the buffers
	nextItem    uint64 // expected index of next item to be added to the batch
	totalBytes  int64  // counts written bytes since reset

	// Items of compressed tables are not appended right away: they are gathered
	// and compressed in parallel once enough of them piled up, or when the batch
	// is flushed. Compression dominates the cost of writing chain data and would
	// otherwise serialize the writer on a single core.
	compress     bool
	pendingData  []byte // Concatenated uncompressed items awaiting compression
	pendingSizes []int  // Sizes of the pending items
}

// newBatch creates a new batch for the freezer table.
func (t *freezerTable) newBatch() *freezerTableBatch {
	batch := &freezerTableBatch{t: t, compress: !t.config.noSnappy}
	batch.reset()
	return batch
}

// reset clears the batch for reuse.
func (batch *freezerTableBatch) reset() {
	batch.dataBuffer = batch.dataBuffer[:0]
	batch.indexBuffer = batch.indexBuffer[:0]
	batch.pendingData = batch.pendingData[:0]
	batch.pendingSizes = batch.pendingSizes[:0]
	batch.curItem = batch.t.items.Load()
	batch.nextItem = batch.curItem
	batch.totalBytes = 0
}

// Append rlp-encodes and adds data at the end of the freezer table. The item number is a
// precautionary parameter to ensure data correctness, but the table will reject already
// existing data.
func (batch *freezerTableBatch) Append(item uint64, data interface{}) error {
	if item != batch.nextItem {
		return fmt.Errorf("%w: have %d want %d", errOutOrderInsertion, item, batch.nextItem)
	}
	batch.nextItem++

	// Encode the item.
	batch.encBuffer.Reset()
	if err := rlp.Encode(&batch.encBuffer, data); err != nil {
		return err
	}
	return batch.add(batch.encBuffer.data)
}

// AppendRaw injects a binary blob at the end of the freezer table. The item number is a
// precautionary parameter to ensure data correctness, but the table will reject already
// existing data.
func (batch *freezerTableBatch) AppendRaw(item uint64, blob []byte) error {
	if item != batch.nextItem {
		return fmt.Errorf("%w: have %d want %d", errOutOrderInsertion, item, batch.nextItem)
	}
	batch.nextItem++

	return batch.add(blob)
}

// add appends an encoded item to the output buffers of an uncompressed table,
// or gathers it for compression otherwise. The item is copied, callers are
// free to reuse the buffer.
func (batch *freezerTableBatch) add(data []byte) error {
	if !batch.compress {
		return batch.appendItem(data)
	}
	batch.pendingData = append(batch.pendingData, data...)
	batch.pendingSizes = append(batch.pendingSizes, len(data))

	if len(batch.pendingData) > freezerBatchPendingLimit {
		return batch.compressPending()
	}
	return nil
}

// compressPending compresses the gathered items in parallel and appends them
// to the output buffers in their original order.
func (batch *freezerTableBatch) compressPending() error {
	if len(batch.pendingSizes) == 0 {
		return nil
	}
	// Detach the pending items, appending them may commit the output buffers
	// which must not pick up the pending items again
	var (
		data  = batch.pendingData
		sizes = batch.pendingSizes
	)
	batch.pendingData = batch.pendingData[:0]
	batch.pendingSizes = batch.pendingSizes[:0]

	// Split the items into contiguous chunks and compress each on its own
	// goroutine, retaining the output per item to append them in order
	var (
		workers = min(len(sizes), runtime.NumCPU())
		chunk   = (len(sizes) + workers - 1) / workers
		outputs = make([][][]byte, workers)
		offsets = make([]int, len(sizes)+1)
		pend    sync.WaitGroup
	)
	for i, size := range sizes {
		offsets[i+1] = offsets[i] + size
	}
	for w := 0; w < workers; w++ {
		start, end := w*chunk, min((w+1)*chunk, len(sizes))
		if start >= end {
			break
		}
		pend.Add(1)
		go func(w, start, end int) {
			defer pend.Done()

			compressed := make([][]byte, 0, end-start)
			for i := start; i < end; i++ {
				compressed = append(compressed, snappy.Encode(nil, data[offsets[i]:offsets[i+1]]))
			}
			outputs[w] = compressed
		}(w, start, end)
	}
	pend.Wait()

	for _, compressed := range outputs {
		for _, item := range compressed {
			if err := batch.appendItem(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (batch *freezerTableBatch) appendItem(data []byte) error {
	// Check if item fits into current data file.
	itemSize := int64(len(data))
	itemOffset := batch.t.headBytes + int64(len(batch.dataBuffer))
	if itemOffset+itemSize > int64(batch.t.maxFileSize) {
		// It doesn't fit, go to next file first.
		if err := batch.write(); err != nil {
			return err
		}
		if err := batch.t.advanceHead(); err != nil {
			return err
		}
		itemOffset = 0
	}

	// Put data to buffer.
	batch.dataBuffer = append(batch.dataBuffer, data...)
	batch.totalBytes += itemSize

	// Put index entry to buffer.
	entry := indexEntry{filenum: batch.t.headId, offset: uint32(itemOffset + itemSize)}
	batch.indexBuffer = entry.append(batch.indexBuffer)
	batch.curItem++

	return batch.maybeCommit()
}

// maybeCommit writes the buffered data if the buffer is full enough.
func (batch *freezerTableBatch) maybeCommit() error {
	if len(batch.dataBuffer) > freezerBatchBufferLimit {
		return batch.write()
	}
	return nil
}

// commit compresses and appends any pending items and writes all batched items
// to the backing freezerTable.
func (batch *freezerTableBatch) commit() error {
	if err := batch.compressPending(); err != nil {
		return err
	}
	return batch.write()
}

// write writes the buffered items to the backing freezerTable. Note index
// file isn't fsync'd after the file write, the recent write can be lost
// after the power failure.
func (batch *freezerTableBatch) write() error {
	_, err := batch.t.head.Write(batch.dataBuffer)
	if err != nil {
		return err
	}
	dataSize := int64(len(batch.dataBuffer))
	batch.dataBuffer = batch.dataBuffer[:0]

	_, err = batch.t.index.Write(batch.indexBuffer)
	if err != nil {
		return err
	}
	indexSize := int64(len(batch.indexBuffer))
	batch.indexBuffer = batch.indexBuffer[:0]

	// Update headBytes of table.
	batch.t.headBytes += dataSize
	items := batch.curItem - batch.t.items.Load()
	batch.t.items.Store(batch.curItem)

	// Update metrics.
	batch.t.sizeGauge.Inc(dataSize + indexSize)
	batch.t.writeMeter.Mark(dataSize + indexSize)

	// Periodically sync the table, todo (rjl493456442) make it configurable?
	batch.t.uncommitted += items
	if batch.t.uncommitted > freezerTableFlushThreshold && time.Since(batch.t.lastSync) > 30*time.Second {
		batch.t.uncommitted = 0
		batch.t.lastSync = time.Now()
		return batch.t.Sync()
	}
	return nil
}

// compress snappy-compresses the data.
// writeBuffer implements io.Writer for a byte slice.
type writeBuffer struct {
	data []byte
}

func (wb *writeBuffer) Write(data []byte) (int, error) {
	wb.data = append(wb.data, data...)
	return len(data), nil
}

func (wb *writeBuffer) Reset() {
	wb.data = wb.data[:0]
}

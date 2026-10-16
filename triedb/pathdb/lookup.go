// Copyright 2024 The go-ethereum Authors
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

package pathdb

import (
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// indexSlots is the number of the slots in the layer index, one per state
	// id, reused in a ring. It must be a power of two above the maximum number
	// of the diff layers.
	indexSlots = 256

	// indexMinRows and indexMaxRows bound the number of the rows in the layer
	// index, each holding a bit per slot.
	indexMinRows = 1 << 14
	indexMaxRows = 1 << 20

	// indexRowsPerKey is the number of the rows reserved per bloom key of a
	// single layer, yielding ~0.5% false positive rate per slot with 3 bits.
	indexRowsPerKey = 16
)

// indexRows is the transposed bloom filter of the layer index: the bloom filter
// of a slot is a column of the rows, so the slots possibly holding a key are
// resolved by intersecting the 3 rows of the key.
type indexRows struct {
	rows [][4]uint64
}

// positions returns the rows of the given bloom key, picked by the disjoint
// bit ranges of the key.
func (r *indexRows) positions(key uint64) (uint64, uint64, uint64) {
	mask := uint64(len(r.rows) - 1)
	return key & mask, (key >> 21) & mask, (key >> 42) & mask
}

// set marks the key in the column of the given slot, in a rows that isn't
// probed yet.
func (r *indexRows) set(key uint64, word uint64, bit uint64) {
	p0, p1, p2 := r.positions(key)
	r.rows[p0][word] |= bit
	r.rows[p1][word] |= bit
	r.rows[p2][word] |= bit
}

// taskQueue runs the scheduled tasks one by one in a background thread, in the
// order of scheduling. The thread terminates once all the tasks are done.
type taskQueue struct {
	lock    sync.Mutex
	cond    sync.Cond // signaled when the thread terminates
	tasks   []func()
	running bool
}

// schedule appends the task, launching the thread if it's not active.
func (q *taskQueue) schedule(task func()) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.tasks = append(q.tasks, task)
	if !q.running {
		q.running = true
		go q.run()
	}
}

// run executes the scheduled tasks in order, terminating once all are done.
func (q *taskQueue) run() {
	for {
		q.lock.Lock()
		tasks := q.tasks
		q.tasks = nil
		if len(tasks) == 0 {
			q.running = false
			q.cond.Broadcast()
			q.lock.Unlock()
			return
		}
		q.lock.Unlock()

		for _, task := range tasks {
			task()
		}
	}
}

// wait blocks until the scheduled tasks are done.
func (q *taskQueue) wait() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.cond.L = &q.lock
	for q.running {
		q.cond.Wait()
	}
}

// indexEntry is a layer indexed in a slot.
type indexEntry struct {
	id   uint64
	keys [][]uint64
}

// indexTask is a task of the layer indexer: either indexing the layer, or the
// slots at or below the state id of the disk layer being cleared.
type indexTask struct {
	layer *diffLayer
	base  uint64
}

// layerIndex locates the diff layers possibly holding an entry by its bloom key,
// avoiding probing the layers one by one. The layers are indexed by a background
// thread, strictly in the order of creation, thus the layers not indexed yet are
// always on top of the indexed ones in any chain, probed by their own filters.
type layerIndex struct {
	rows  atomic.Pointer[indexRows] // nil if no layer is indexed yet
	queue taskQueue                 // the indexer thread

	// The states below are only accessed by the indexer thread
	slots   [indexSlots][]indexEntry // the layers indexed in each slot
	cleared uint64                   // the state id up to which the slots are cleared
}

// newLayerIndex constructs the index on top of the disk layer with given state id.
func newLayerIndex(base uint64) *layerIndex {
	return &layerIndex{cleared: base}
}

// schedule appends the task to the indexer.
func (x *layerIndex) schedule(task indexTask) {
	x.queue.schedule(func() {
		if task.layer != nil {
			x.insert(task.layer)
		} else {
			x.clear(task.base)
		}
	})
}

// wait blocks until the scheduled tasks are done.
func (x *layerIndex) wait() {
	x.queue.wait()
}

// slotBit returns the word and the bit of the slot hosting the given state id.
func slotBit(id uint64) (uint64, uint64, uint64) {
	slot := id % indexSlots
	return slot, slot / 64, 1 << (slot % 64)
}

// insert indexes the layer, growing the rows if they're too few for the keys of
// the layer. The layer is marked as indexed afterwards.
func (x *layerIndex) insert(dl *diffLayer) {
	dl.prepare()
	start := time.Now()

	var n int
	for _, part := range dl.keys {
		n += len(part)
	}
	rows := x.rows.Load()
	if want := min(max(1<<bits.Len(uint(n*indexRowsPerKey)), indexMinRows), indexMaxRows); rows == nil || len(rows.rows) < want {
		rows = x.grow(want)
	}
	slot, word, bit := slotBit(dl.id)
	for _, part := range dl.keys {
		for _, key := range part {
			p0, p1, p2 := rows.positions(key)
			atomic.OrUint64(&rows.rows[p0][word], bit)
			atomic.OrUint64(&rows.rows[p1][word], bit)
			atomic.OrUint64(&rows.rows[p2][word], bit)
		}
	}
	x.slots[slot] = append(x.slots[slot], indexEntry{id: dl.id, keys: dl.keys})
	dl.indexed.Store(x)
	layerIndexTimer.UpdateSince(start)
}

// grow rebuilds the rows with the given number of rows, with all the indexed
// layers reinserted. The readers loading the rows after observing the indexed
// layers will always find them, either in the old rows or the new ones.
func (x *layerIndex) grow(n int) *indexRows {
	rows := &indexRows{rows: make([][4]uint64, n)}
	for slot := range x.slots {
		_, word, bit := slotBit(uint64(slot))
		for _, entry := range x.slots[slot] {
			for _, part := range entry.keys {
				for _, key := range part {
					rows.set(key, word, bit)
				}
			}
		}
	}
	x.rows.Store(rows)
	return rows
}

// clear clears the slots of the state ids at or below the given disk layer,
// whose layers are all persisted. The clearing of a slot is deferred if it's
// already reused by a layer above the disk layer (possible only if the layers
// span more state ids than the slots), the stale bits are false positives.
func (x *layerIndex) clear(base uint64) {
	if base <= x.cleared {
		return
	}
	from := x.cleared + 1
	if base-x.cleared > indexSlots {
		from = base - indexSlots + 1
	}
	x.cleared = base

	rows := x.rows.Load()
	for id := from; id <= base; id++ {
		slot, word, bit := slotBit(id)
		entries := x.slots[slot]
		if len(entries) == 0 || slices.ContainsFunc(entries, func(e indexEntry) bool { return e.id > base }) {
			continue
		}
		for _, entry := range entries {
			for _, part := range entry.keys {
				for _, key := range part {
					p0, p1, p2 := rows.positions(key)
					atomic.AndUint64(&rows.rows[p0][word], ^bit)
					atomic.AndUint64(&rows.rows[p1][word], ^bit)
					atomic.AndUint64(&rows.rows[p2][word], ^bit)
				}
			}
		}
		x.slots[slot] = nil
	}
}

// candidates returns the bitmap of the slots possibly holding the given key.
func (x *layerIndex) candidates(key uint64) (c [4]uint64) {
	rows := x.rows.Load()
	if rows == nil {
		return c
	}
	p0, p1, p2 := rows.positions(key)
	for i := range c {
		c[i] = atomic.LoadUint64(&rows.rows[p0][i]) & atomic.LoadUint64(&rows.rows[p1][i]) & atomic.LoadUint64(&rows.rows[p2][i])
	}
	return c
}

// nextCandidate pops the candidate slot nearest to the slot of the given state
// id, at or below it in the ring, returning its distance from the state id.
func nextCandidate(c *[4]uint64, id uint64) (uint64, bool) {
	pos := id % indexSlots
	below := ^uint64(0) >> (63 - pos%64) // the bits at or below pos in its word
	for i := uint64(0); i <= 4; i++ {
		w := (pos/64 + 4 - i%4) % 4
		word := c[w]
		switch i {
		case 0:
			word &= below
		case 4:
			word &^= below
		}
		if word == 0 {
			continue
		}
		b := uint64(63 - bits.LeadingZeros64(word))
		c[w] &^= 1 << b
		return (pos - (w*64 + b)) % indexSlots, true
	}
	return 0, false
}

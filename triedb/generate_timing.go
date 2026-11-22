// Copyright 2026 The go-ethereum Authors
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

package triedb

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

// GenerateTimings reports elapsed time summed across partition workers. Finalize
// is measured separately on the calling goroutine after all workers have exited.
type GenerateTimings struct {
	FlatIteration time.Duration // opening, advancing, fetching values and releasing source iterators
	TrieBuild     time.Duration // remaining worker time, including hashing, RLP and bookkeeping
	NodeWrite     time.Duration // buffering nodes in the destination batch and calling Write
	NodeCommit    time.Duration // portion of NodeWrite spent inside Batch.Write, including stalls
	Finalize      time.Duration // assembling, writing and verifying the root, then syncing the destination
}

type generationTimers struct {
	mu    sync.Mutex
	total GenerateTimings
}

func (g *generationTimers) snapshot() GenerateTimings {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.total
}

// A partition records timings locally and publishes deltas after each batch
// write and at exit, so iterating a slot doesn't touch a shared timer.
type partitionTimer struct {
	all    *generationTimers
	start  time.Time
	flat   time.Duration
	buffer time.Duration
	commit time.Duration
}

func (p *partitionTimer) publish() {
	now := time.Now()
	write := p.buffer + p.commit
	p.all.mu.Lock()
	p.all.total.FlatIteration += p.flat
	p.all.total.NodeWrite += write
	p.all.total.NodeCommit += p.commit
	p.all.total.TrieBuild += now.Sub(p.start) - p.flat - write
	p.all.mu.Unlock()
	p.start, p.flat, p.buffer, p.commit = now, 0, 0, 0
}

type timedFlatIterator struct {
	ethdb.Iterator
	timer *partitionTimer
}

func (it *timedFlatIterator) Next() bool {
	start := time.Now()
	ok := it.Iterator.Next()
	it.timer.flat += time.Since(start)
	return ok
}

func (it *timedFlatIterator) Value() []byte {
	start := time.Now()
	val := it.Iterator.Value()
	it.timer.flat += time.Since(start)
	return val
}

func (it *timedFlatIterator) Release() {
	start := time.Now()
	it.Iterator.Release()
	it.timer.flat += time.Since(start)
}

type timedNodeBatch struct {
	ethdb.Batch
	timer *partitionTimer
}

func (p *partitionTimer) wrapBatch(b ethdb.Batch) ethdb.Batch {
	if p == nil {
		return b
	}
	return &timedNodeBatch{Batch: b, timer: p}
}

func (b *timedNodeBatch) Put(key, value []byte) error {
	start := time.Now()
	err := b.Batch.Put(key, value)
	b.timer.buffer += time.Since(start)
	return err
}

func (b *timedNodeBatch) Write() error {
	start := time.Now()
	err := b.Batch.Write()
	b.timer.commit += time.Since(start)
	b.timer.publish()
	return err
}

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
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	storageProbeSlots = 4096
	storageProbeBytes = 256 * 1024
	maxStorageJobs    = 64

	// Splitting adds seeks and SST boundaries, so reserve it for large accounts.
	minStorageSlotsForSplit = 4 * 1024 * 1024

	// A job should have enough nodes to fill several SST windows.
	targetSlotsPerJob = 1024 * 1024
)

// Estimate the work from the keyspace covered by the probe. Use fewer
// jobs for moderate accounts so their SSTs can fill across adjacent prefixes.
// The estimate only affects scheduling: the ranges always cover every slot.
func storageJobCount(slot []byte, reverse bool) int {
	position := binary.BigEndian.Uint32(slot)
	if reverse {
		position = ^position
	}
	estimatedSlots := (uint64(storageProbeSlots) << 32) / (uint64(position) + 1)
	if estimatedSlots < minStorageSlotsForSplit {
		return 1
	}
	jobs := min(maxStorageJobs, estimatedSlots/targetSlotsPerJob)
	return 1 << (bits.Len64(jobs) - 1)
}

// storageProbe owns copies of the first slots of one account. No node for
// that account may be written until the probe chooses serial or split generation.
type storageProbe struct {
	slots  []probedSlot
	values []byte
}

type probedSlot struct {
	hash common.Hash
	end  int
}

func (p *storageProbe) add(key, value []byte) bool {
	if len(value) > storageProbeBytes-len(p.values) {
		return false
	}
	if p.slots == nil {
		p.slots = make([]probedSlot, 0, storageProbeSlots)
		p.values = make([]byte, 0, storageProbeBytes)
	}
	p.values = append(p.values, value...)
	p.slots = append(p.slots, probedSlot{hash: common.Hash(key), end: len(p.values)})
	return true
}

func (p *storageProbe) reset() {
	p.slots = p.slots[:0]
	p.values = p.values[:0]
}

func (p *storageProbe) finish(t *trie.StackTrie, c *genCounters) (int64, error) {
	if len(p.slots) == 0 {
		return 0, nil
	}
	start := 0
	for _, slot := range p.slots {
		if err := t.Update(slot.hash[:], p.values[start:slot.end]); err != nil {
			return 0, err
		}
		start = slot.end
	}
	count := int64(len(p.slots))
	c.slots.Add(count)
	p.reset()
	return count, nil
}

type storageSubtries struct {
	owner     common.Hash
	roots     [numPartitions][]byte
	slots     atomic.Int64
	remaining atomic.Int32
	done      chan struct{}
}

type storageJob struct {
	tree *storageSubtries
	lo   int
	hi   int // exclusive
}

// storageQueue lets active partition workers help while waiting for their
// own subtree roots, and lets idle workers wait for new storage jobs.
type storageQueue struct {
	mu      sync.Mutex
	jobs    []storageJob
	ready   chan struct{}
	allDone chan struct{}
}

func newStorageQueue() *storageQueue {
	return &storageQueue{ready: make(chan struct{}), allDone: make(chan struct{})}
}

func (q *storageQueue) add(tree *storageSubtries, count int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	tree.remaining.Store(int32(count))
	if len(q.jobs) == 0 {
		close(q.ready)
	}
	width := numPartitions / count
	for i := 0; i < numPartitions; i += width {
		q.jobs = append(q.jobs, storageJob{tree: tree, lo: i, hi: i + width})
	}
}

func (q *storageQueue) take() (storageJob, <-chan struct{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return storageJob{}, q.ready
	}
	last := len(q.jobs) - 1
	job := q.jobs[last]
	q.jobs[last] = storageJob{}
	q.jobs = q.jobs[:last]
	if last == 0 {
		q.ready = make(chan struct{})
	}
	return job, nil
}

type generationWorker struct {
	batches *partitionBatches
	timer   *partitionTimer
	probe   storageProbe
}

func (w *generationWorker) waitForStorage(ctx context.Context, cancel, done, ready <-chan struct{}) error {
	if w.timer != nil {
		w.timer.publish()
		defer func() { w.timer.start = time.Now() }()
	}
	select {
	case <-cancel:
		return ErrCancelled
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	case <-ready:
	}
	return nil
}

func (g *trieGenerator) helpStorage(ctx context.Context, cancel, done <-chan struct{}, scheme string, c *genCounters, w *generationWorker) error {
	for {
		select {
		case <-done:
			return nil
		case <-cancel:
			return ErrCancelled
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		job, ready := c.storage.take()
		if job.tree == nil {
			if err := w.waitForStorage(ctx, cancel, done, ready); err != nil {
				return err
			}
			continue
		}
		if err := g.generateStorageJob(ctx, cancel, scheme, c, w, job); err != nil {
			return err
		}
	}
}

// generateStorageJob flushes its own contiguous slot range before the worker
// returns to account work or starts a different range.
func (g *trieGenerator) generateStorageJob(ctx context.Context, cancel <-chan struct{}, scheme string, c *genCounters, w *generationWorker, job storageJob) error {
	owner := job.tree.owner
	r := g.rangeIterators(w.timer)
	prefix := slices.Concat(rawdb.SnapshotStoragePrefix, owner[:])
	start := common.Hash{byte(job.lo)}
	if g.ingester != nil {
		start = common.MaxHash
		start[0] = byte(job.hi - 1)
	}
	it := r.open(prefix, start[:], common.HashLength)
	defer func() { it.Release() }()

	var (
		t        *trie.PartialStackTrie
		previous = -1
		slots    int64
	)
	for it.Next() {
		select {
		case <-cancel:
			return ErrCancelled
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		key := it.Key()[len(prefix):]
		partition := int(key[0])
		if partition < job.lo || partition >= job.hi {
			break
		}
		if partition != previous {
			if t != nil {
				t.Hash()
			}
			t = trie.NewBytePartitionStackTrie(byte(partition), g.ingester != nil, func(path []byte, hash common.Hash, blob []byte) {
				if len(path) == 2 {
					job.tree.roots[partition] = common.CopyBytes(blob)
					return
				}
				c.writeStorageNode(w.batches.storage, scheme, owner, path, hash, blob)
			})
			previous = partition
		}
		if err := t.Update(key, it.Value()); err != nil {
			return fmt.Errorf("storage account %x prefix %02x: %w", owner, partition, err)
		}
		slots++
		c.slots.Add(1)
		flushed, err := w.batches.flush(false)
		if err != nil {
			return err
		}
		if flushed && g.inPlace {
			it, err = r.reopen(it, prefix, common.HashLength)
			if err != nil {
				return err
			}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if t != nil {
		t.Hash()
	}
	if _, err := w.batches.flush(true); err != nil {
		return fmt.Errorf("flush storage subtree: %w", err)
	}
	job.tree.slots.Add(slots)
	if job.tree.remaining.Add(-1) == 0 {
		close(job.tree.done)
	}
	return nil
}

func (g *trieGenerator) splitStorage(ctx context.Context, cancel <-chan struct{}, scheme string, c *genCounters, w *generationWorker, r *rangeIterators, owner common.Hash, jobs int) (common.Hash, int64, error) {
	// Release the main iterators while children run. On resumption, skip
	// this owner's entire storage range and resume at the next account.
	var acctStart []byte
	if r.acct.Next() {
		acctStart = common.CopyBytes(r.acct.Key()[len(rawdb.SnapshotAccountPrefix):])
	}
	if err := r.acct.Error(); err != nil {
		return common.Hash{}, 0, err
	}
	r.release()
	r.acct, r.stor = nil, nil
	if _, err := w.batches.flush(true); err != nil {
		return common.Hash{}, 0, err
	}
	start := time.Now()
	tree := &storageSubtries{owner: owner, done: make(chan struct{})}
	c.storage.add(tree, jobs)
	if err := g.helpStorage(ctx, cancel, tree.done, scheme, c, w); err != nil {
		return common.Hash{}, 0, err
	}
	// The stitching keys can sort between different child SSTs. Commit them
	// through the ordinary batch after all of this owner's SSTs are installed.
	root, err := assemblePartitionRoots(&tree.roots, func(path []byte, hash common.Hash, blob []byte) {
		c.writeStorageNode(w.batches.flat, scheme, owner, path, hash, blob)
	})
	if err != nil {
		return common.Hash{}, 0, err
	}
	if acctStart == nil {
		r.acct = emptyRangeIterator()
	} else {
		r.acct = r.open(rawdb.SnapshotAccountPrefix, acctStart, common.HashLength)
	}
	storageStart := owner[:]
	if g.ingester == nil {
		storageStart = slices.Concat(owner[:], common.MaxHash[:], []byte{0})
	}
	r.stor = r.open(rawdb.SnapshotStoragePrefix, storageStart, 2*common.HashLength)
	slots := tree.slots.Load()
	log.Info("Split storage trie done", "owner", owner, "slots", slots, "jobs", jobs, "elapsed", common.PrettyDuration(time.Since(start)))
	return root, slots, nil
}

func (c *genCounters) writeStorageNode(dst ethdb.KeyValueWriter, scheme string, owner common.Hash, path []byte, hash common.Hash, blob []byte) {
	c.storageTrieNodes.Add(1)
	if scheme == rawdb.PathScheme {
		c.storageTrieBytes.Add(int64(len(path) + common.HashLength + len(blob)))
		rawdb.WriteStorageTrieNode(dst, owner, path, blob)
	} else {
		c.storageTrieBytes.Add(int64(common.HashLength + len(blob)))
		rawdb.WriteTrieNode(dst, owner, path, hash, blob, scheme)
	}
}

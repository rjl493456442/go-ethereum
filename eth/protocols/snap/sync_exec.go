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

package snap

import (
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// storageExecInflight bounds the jobs buffered in the storage executor.
// Submissions block once the bound is hit, pushing the backpressure onto
// the runloop and further onto the peers.
const storageExecInflight = 512

// storageJob is the execution part of the storage delivery: the flat-state
// writes, the trie generation and the batch flushes.
type storageJob struct {
	account common.Hash
	hashes  []common.Hash
	slots   [][]byte

	subTask  *storageTask // chunked-contract feed if non-nil
	finish   bool         // subtask completed by this feed
	mainTask *accountTask // origin account task
}

// storageJobResult carries a job's verdict back to the runloop.
type storageJobResult struct {
	healSkip *accountTask // task whose account's storage came out complete
	account  common.Hash  // account the heal-skip verdict applies to
}

// storageExecutor runs storage jobs on a bounded worker pool. Jobs sharing a
// key (the same subtask) execute in submission order; different keys run in
// parallel.
type storageExecutor struct {
	s *syncer

	lock   sync.Mutex
	queues map[any][]*storageJob
	wg     sync.WaitGroup

	tokens   chan struct{}
	inflight chan struct{}
	results  chan *storageJobResult
}

func newStorageExecutor(s *syncer) *storageExecutor {
	return &storageExecutor{
		s:        s,
		queues:   make(map[any][]*storageJob),
		tokens:   make(chan struct{}, runtime.GOMAXPROCS(0)),
		inflight: make(chan struct{}, storageExecInflight),
		results:  make(chan *storageJobResult, storageExecInflight),
	}
}

// submit queues a job for execution, blocking when too many are in flight.
func (e *storageExecutor) submit(job *storageJob) {
	e.inflight <- struct{}{}

	var key any = job.account
	if job.subTask != nil {
		key = job.subTask
	}
	e.lock.Lock()
	e.queues[key] = append(e.queues[key], job)
	if len(e.queues[key]) > 1 {
		e.lock.Unlock() // drainer already active on this key
		return
	}
	e.lock.Unlock()

	e.wg.Add(1)
	go e.drain(key)
}

// drain runs the queued jobs of one key in order, exiting when none remain.
func (e *storageExecutor) drain(key any) {
	defer e.wg.Done()
	for {
		e.lock.Lock()
		job := e.queues[key][0]
		e.lock.Unlock()

		e.tokens <- struct{}{}
		e.s.executeStorageJob(job)
		<-e.tokens

		e.lock.Lock()
		e.queues[key] = e.queues[key][1:]
		empty := len(e.queues[key]) == 0
		if empty {
			delete(e.queues, key)
		}
		e.lock.Unlock()

		<-e.inflight
		if empty {
			// Nudge the runloop: heal-phase entry may be gated on the
			// executor draining.
			select {
			case e.s.update <- struct{}{}:
			default:
			}
			return
		}
	}
}

// pending reports whether any jobs are queued or executing.
func (e *storageExecutor) pending() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return len(e.queues) > 0
}

// executeStorageJob runs one storage job on a worker thread.
func (s *syncer) executeStorageJob(job *storageJob) {
	start := time.Now()
	batch := ethdb.HookedBatch{
		Batch: s.db.NewBatch(),
		OnPut: func(key []byte, value []byte) {
			s.storageBytes.Add(int64(len(key) + len(value)))
		},
	}
	// One-shot small contract: reconstruct the complete storage trie
	if job.subTask == nil {
		var tr genTrie
		if s.scheme == rawdb.HashScheme {
			tr = newHashTrie(batch)
		}
		if s.scheme == rawdb.PathScheme {
			// Keep the left boundary as it's complete
			tr = newPathTrie(job.account, false, s.db, batch)
		}
		for j := 0; j < len(job.hashes); j++ {
			tr.update(job.hashes[j][:], job.slots[j])
		}
		tr.commit(true)
	}
	// Persist the received storage segments. These flat state maybe
	// outdated during the sync, but it can be fixed later during the
	// snapshot generation.
	for j := 0; j < len(job.hashes); j++ {
		rawdb.WriteStorageSnapshot(batch, job.account, job.hashes[j], job.slots[j])

		// If we're storing large contracts, generate the trie nodes
		// on the fly to not trash the gluing points
		if job.subTask != nil {
			job.subTask.genTrie.update(job.hashes[j][:], job.slots[j])
		}
	}
	// Large contracts could have generated new trie nodes, flush them to disk
	if job.subTask != nil {
		if job.finish {
			root := job.subTask.genTrie.commit(job.subTask.Last == common.MaxHash)
			gbStart := time.Now()
			if err := job.subTask.genBatch.Write(); err != nil {
				log.Error("Failed to persist stack slots", "err", err)
			}
			s.prof.commit[profStorage].observe(time.Since(gbStart))
			job.subTask.genBatch.Reset()

			// If the chunk's root is an overflown but full delivery, hand the
			// heal-skip verdict back to the runloop.
			if root == job.subTask.root && rawdb.HasTrieNode(s.db, job.account, nil, root, s.scheme) {
				s.storageExec.results <- &storageJobResult{
					healSkip: job.mainTask,
					account:  job.account,
				}
			}
		} else if job.subTask.genBatch.ValueSize() > batchSizeThreshold {
			job.subTask.genTrie.commit(false)
			gbStart := time.Now()
			if err := job.subTask.genBatch.Write(); err != nil {
				log.Error("Failed to persist stack slots", "err", err)
			}
			s.prof.commit[profStorage].observe(time.Since(gbStart))
			job.subTask.genBatch.Reset()
		}
	}
	// Flush the flat state writes
	commitStart := time.Now()
	if err := batch.Write(); err != nil {
		log.Crit("Failed to persist storage slots", "err", err)
	}
	s.prof.commit[profStorage].observe(time.Since(commitStart))
	s.prof.exec.observe(time.Since(start))
}

// applyHealSkip applies a completed storage job's heal-skip verdict on the
// runloop. Best effort: if the task has moved on, the account just stays
// marked for healing.
func (s *syncer) applyHealSkip(r *storageJobResult) {
	if r.healSkip == nil || r.healSkip.res == nil {
		return
	}
	for i, account := range r.healSkip.res.hashes {
		if account == r.account {
			r.healSkip.needHeal[i] = false
			skipStorageHealingGauge.Inc(1)
		}
	}
}

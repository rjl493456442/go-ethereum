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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// storageExecInflight bounds the jobs buffered in the storage executor.
// Submissions block once the bound is hit, pushing the backpressure onto
// the runloop and further onto the peers.
const storageExecInflight = 512

// storageJob is the execution part of the storage delivery: the flat-state
// writes, the trie generation and the batch flushes.
//
// A response's one-shot small contracts are coalesced into one job sharing
// a single batch; the chunked-contract feed (at most one per response) rides
// in its own job so that feeds of the same subtask serialize.
type storageJob struct {
	// One-shot small contracts, complete tries built in one pass
	accounts []common.Hash
	hashes   [][]common.Hash
	slots    [][][]byte

	// Chunked-contract feed
	subTask    *storageTask
	subAccount common.Hash
	subHashes  []common.Hash
	subSlots   [][]byte
	finish     bool // subtask completed by this feed

	mainTask *accountTask // origin account task
}

// storageJobResult carries a job's verdict back to the runloop.
type storageJobResult struct {
	healSkip *accountTask // task whose account's storage came out complete
	account  common.Hash  // account the heal-skip verdict applies to
}

// storageExecutor runs sync jobs (storage deliveries and account forwards)
// on a bounded worker pool. Jobs sharing a key (the same subtask, the same
// account task) execute in submission order; different keys run in parallel.
type storageExecutor struct {
	s *syncer

	lock   sync.Mutex
	queues map[any][]func()
	wg     sync.WaitGroup

	tokens   chan struct{}
	inflight chan struct{}
	results  chan *storageJobResult
}

func newStorageExecutor(s *syncer) *storageExecutor {
	return &storageExecutor{
		s:        s,
		queues:   make(map[any][]func()),
		tokens:   make(chan struct{}, runtime.GOMAXPROCS(0)),
		inflight: make(chan struct{}, storageExecInflight),
		results:  make(chan *storageJobResult, storageExecInflight),
	}
}

// submit queues a job for execution, blocking when too many are in flight.
// Jobs sharing a key execute in submission order.
func (e *storageExecutor) submit(key any, run func()) {
	e.inflight <- struct{}{}

	e.lock.Lock()
	e.queues[key] = append(e.queues[key], run)
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
		run := e.queues[key][0]
		e.lock.Unlock()

		e.tokens <- struct{}{}
		run()
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
	// One-shot small contracts: reconstruct the complete storage tries and
	// persist the received storage segments. The flat state maybe outdated
	// during the sync, but it can be fixed later during the snapshot
	// generation.
	for k, account := range job.accounts {
		var tr genTrie
		if s.scheme == rawdb.HashScheme {
			tr = newHashTrie(batch)
		}
		if s.scheme == rawdb.PathScheme {
			// Keep the left boundary as it's complete
			tr = newPathTrie(account, false, s.db, batch)
		}
		for j := 0; j < len(job.hashes[k]); j++ {
			tr.update(job.hashes[k][j][:], job.slots[k][j])
		}
		tr.commit(true)

		for j := 0; j < len(job.hashes[k]); j++ {
			rawdb.WriteStorageSnapshot(batch, account, job.hashes[k][j], job.slots[k][j])
		}
	}
	// Chunked contract: persist the slots and generate the trie nodes on
	// the fly to not trash the gluing points
	if job.subTask != nil {
		for j := 0; j < len(job.subHashes); j++ {
			rawdb.WriteStorageSnapshot(batch, job.subAccount, job.subHashes[j], job.subSlots[j])
			job.subTask.genTrie.update(job.subHashes[j][:], job.subSlots[j])
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
			if root == job.subTask.root && rawdb.HasTrieNode(s.db, job.subAccount, nil, root, s.scheme) {
				s.storageExec.results <- &storageJobResult{
					healSkip: job.mainTask,
					account:  job.subAccount,
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

// accountJob is the execution part of an account-task forward: the flat
// account writes and the account-trie generation. The needHeal flags are
// snapshotted at packaging time, so late heal-skip verdicts cannot race the
// worker.
type accountJob struct {
	task     *accountTask
	hashes   []common.Hash
	accounts []*types.StateAccount
	needHeal []bool
	finish   bool // task fully done by this forward: commit the trie boundary
}

// executeAccountJob runs one account-forward job on a worker thread.
func (s *syncer) executeAccountJob(job *accountJob) {
	start := time.Now()
	batch := ethdb.HookedBatch{
		Batch: s.db.NewBatch(),
		OnPut: func(key []byte, value []byte) {
			s.accountBytes.Add(int64(len(key) + len(value)))
		},
	}
	for i, hash := range job.hashes {
		slim := types.SlimAccountRLP(*job.accounts[i])
		rawdb.WriteAccountSnapshot(batch, hash, slim)

		if !job.needHeal[i] {
			// If the storage task is complete, drop it into the stack trie
			// to generate account trie nodes for it
			full, err := types.FullAccountRLP(slim) // TODO(karalabe): Slim parsing can be omitted
			if err != nil {
				panic(err) // Really shouldn't ever happen
			}
			job.task.genTrie.update(hash[:], full)
		} else {
			// If the storage task is incomplete, explicitly delete the corresponding
			// account item from the account trie to ensure that all nodes along the
			// path to the incomplete storage trie are cleaned up.
			if err := job.task.genTrie.delete(hash[:]); err != nil {
				panic(err) // Really shouldn't ever happen
			}
		}
	}
	// Flush anything written just now
	commitStart := time.Now()
	if err := batch.Write(); err != nil {
		log.Crit("Failed to persist accounts", "err", err)
	}
	s.prof.commit[profAccount].observe(time.Since(commitStart))

	// Stack trie could have generated trie nodes, push them to disk. It's fine
	// even if we crash and lose this write as it will only cause more data to
	// be downloaded during heal.
	if job.finish {
		job.task.genTrie.commit(job.task.Last == common.MaxHash)
		gbStart := time.Now()
		if err := job.task.genBatch.Write(); err != nil {
			log.Error("Failed to persist stack account", "err", err)
		}
		s.prof.commit[profAccount].observe(time.Since(gbStart))
		job.task.genBatch.Reset()
	} else if job.task.genBatch.ValueSize() > batchSizeThreshold {
		job.task.genTrie.commit(false)
		gbStart := time.Now()
		if err := job.task.genBatch.Write(); err != nil {
			log.Error("Failed to persist stack account", "err", err)
		}
		s.prof.commit[profAccount].observe(time.Since(gbStart))
		job.task.genBatch.Reset()
	}
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

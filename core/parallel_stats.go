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

package core

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// parallelStats accumulates BAL parallel-execution instrumentation across every
// block processed in this process lifetime. Unlike the metrics timers (which are
// interval based and flushed by an exporter), it is cumulative and O(1) in
// memory, making it suitable for an end-of-run summary such as geth import.
var parallelStats parallelAccumulator

type parallelAccumulator struct {
	blocks atomic.Int64
	txs    atomic.Int64

	txExecNs     atomic.Int64 // summed wall-clock of the transaction phases
	txWorkNs     atomic.Int64 // summed per-tx CPU work (serial-equivalent)
	txMaxNs      atomic.Int64 // largest single-tx execution time observed
	systemNs     atomic.Int64 // summed pre/post system-call time
	gatherNs     atomic.Int64 // summed serial result-gathering time
	stateApplyNs atomic.Int64 // summed ApplyBlockAccessList time
	stateHashNs  atomic.Int64 // summed IntermediateRoot time
	totalNs      atomic.Int64 // summed end-to-end block processing time

	accountHit    atomic.Int64
	accountMiss   atomic.Int64
	accountMissNs atomic.Int64
	storageHit    atomic.Int64
	storageMiss   atomic.Int64
	storageMissNs atomic.Int64

	// Out-of-Process phases, recorded by the insertion/import pipeline. These are
	// the serial per-block costs that run between Process calls and dominate the
	// wall-clock when execution is already parallel.
	commits      atomic.Int64 // number of blocks committed (also gates the summary)
	commitNs     atomic.Int64 // summed statedb.Commit time (trie node collection + triedb insert)
	blockWriteNs atomic.Int64 // summed block/receipt/preimage batch write time
	trieFlushNs  atomic.Int64 // summed triedb Cap/Commit (dirty-node flush to disk) time
	decodeNs     atomic.Int64 // summed RLP decode time (geth import only)
}

// addCommit records one block's out-of-Process persistence costs.
func (a *parallelAccumulator) addCommit(commit, blockWrite, trieFlush time.Duration) {
	a.commits.Add(1)
	a.commitNs.Add(int64(commit))
	a.blockWriteNs.Add(int64(blockWrite))
	a.trieFlushNs.Add(int64(trieFlush))
}

// RecordImportDecode folds RLP block-decode time into the parallel-execution
// summary. It is called by the geth import pipeline, which decodes blocks
// serially between insertions.
func RecordImportDecode(d time.Duration) {
	parallelStats.decodeNs.Add(int64(d))
}

// add folds one block's instrumentation into the running totals.
func (a *parallelAccumulator) add(txs int, txExec, system, gather, stateApply, stateHash, total time.Duration, p parallelProfile) {
	a.blocks.Add(1)
	a.txs.Add(int64(txs))
	a.txExecNs.Add(int64(txExec))
	a.txWorkNs.Add(int64(p.totalTime))
	for { // running max
		cur := a.txMaxNs.Load()
		if int64(p.maxTime) <= cur || a.txMaxNs.CompareAndSwap(cur, int64(p.maxTime)) {
			break
		}
	}
	a.systemNs.Add(int64(system))
	a.gatherNs.Add(int64(gather))
	a.stateApplyNs.Add(int64(stateApply))
	a.stateHashNs.Add(int64(stateHash))
	a.totalNs.Add(int64(total))

	a.accountHit.Add(p.reads.AccountCacheHit)
	a.accountMiss.Add(p.reads.AccountCacheMiss)
	a.accountMissNs.Add(int64(p.reads.AccountMissTime))
	a.storageHit.Add(p.reads.StorageCacheHit)
	a.storageMiss.Add(p.reads.StorageCacheMiss)
	a.storageMissNs.Add(int64(p.reads.StorageMissTime))
}

func hitRate(hit, miss int64) float64 {
	if total := hit + miss; total > 0 {
		return float64(hit) / float64(total) * 100
	}
	return 0
}

// ParallelExecutionSummary returns a human-readable, cumulative summary of the
// BAL parallel-execution performance across every block processed so far. It
// returns the empty string if no block has been executed in parallel (e.g. a
// pre-Amsterdam import), so callers can skip printing it.
func ParallelExecutionSummary() string {
	a := &parallelStats
	blocks := a.blocks.Load()
	if blocks == 0 && a.commits.Load() == 0 {
		return ""
	}
	var (
		txs        = a.txs.Load()
		txExec     = time.Duration(a.txExecNs.Load())
		txWork     = time.Duration(a.txWorkNs.Load())
		efficiency float64
	)
	if txExec > 0 {
		efficiency = float64(txWork) / float64(txExec)
	}
	return fmt.Sprintf(`Parallel execution summary (BAL-driven):
  blocks (parallel):  %d
  transactions:       %d
  tx wall-clock:      %v
  tx CPU work (sum):  %v
  effective parallelism: %.2fx   (work / wall-clock)
  slowest single tx:  %v
  ---- in-Process phases (summed) ----
  system (pre/post):  %v
  gather (serial):    %v
  state apply:        %v   (overlapped)
  state hash:         %v   (overlapped)
  Process total:      %v
  ---- out-of-Process phases (summed, serial) ----
  commit (trie):      %v
  block write:        %v
  trie flush:         %v
  rlp decode:         %v
  ---- shared-cache reads ----
  account: %d hits, %d misses (%.1f%% hit), miss I/O %v
  storage: %d hits, %d misses (%.1f%% hit), miss I/O %v`,
		blocks,
		txs,
		common.PrettyDuration(txExec),
		common.PrettyDuration(txWork),
		efficiency,
		common.PrettyDuration(time.Duration(a.txMaxNs.Load())),
		common.PrettyDuration(time.Duration(a.systemNs.Load())),
		common.PrettyDuration(time.Duration(a.gatherNs.Load())),
		common.PrettyDuration(time.Duration(a.stateApplyNs.Load())),
		common.PrettyDuration(time.Duration(a.stateHashNs.Load())),
		common.PrettyDuration(time.Duration(a.totalNs.Load())),
		common.PrettyDuration(time.Duration(a.commitNs.Load())),
		common.PrettyDuration(time.Duration(a.blockWriteNs.Load())),
		common.PrettyDuration(time.Duration(a.trieFlushNs.Load())),
		common.PrettyDuration(time.Duration(a.decodeNs.Load())),
		a.accountHit.Load(), a.accountMiss.Load(), hitRate(a.accountHit.Load(), a.accountMiss.Load()), common.PrettyDuration(time.Duration(a.accountMissNs.Load())),
		a.storageHit.Load(), a.storageMiss.Load(), hitRate(a.storageHit.Load(), a.storageMiss.Load()), common.PrettyDuration(time.Duration(a.storageMissNs.Load())),
	)
}

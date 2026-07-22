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
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
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

	accountHit      atomic.Int64
	accountMiss     atomic.Int64
	accountMissNs   atomic.Int64
	accountMissRace atomic.Int64 // misses occurred while the BAL prefetcher was still running
	accountMissNoPf atomic.Int64 // misses occurred without an associated prefetcher
	storageHit      atomic.Int64
	storageMiss     atomic.Int64
	storageMissNs   atomic.Int64
	storageMissRace atomic.Int64 // misses occurred while the BAL prefetcher was still running
	storageMissNoPf atomic.Int64 // misses occurred without an associated prefetcher

	// Out-of-Process phases, recorded by the insertion/import pipeline. These are
	// the serial per-block costs that run between Process calls and dominate the
	// wall-clock when execution is already parallel.
	setupNs        atomic.Int64 // summed setupExecutionState + prefetcher start time
	validateNs     atomic.Int64 // summed ValidateState time (receipt root, BAL hash, state root)
	persistNs      atomic.Int64 // summed writeBlockAndSetHead time (superset of the three below)
	prefetchStopNs atomic.Int64 // summed StopPrefetcher time (trie-prefetch join + subtrie merge)
	commits        atomic.Int64 // number of blocks committed (also gates the summary)
	commitNs       atomic.Int64 // summed statedb.Commit time (trie node collection + triedb insert)
	blockWriteNs   atomic.Int64 // summed block/receipt/preimage batch write time
	trieFlushNs    atomic.Int64 // summed triedb Cap/Commit (dirty-node flush to disk) time
	decodeNs       atomic.Int64 // summed RLP decode time (geth import only)
}

// addProcessBlock records the per-block ProcessBlock phases that surround the
// core Process call: state/reader setup, post-execution ValidateState and block
// persistence (writeBlockAndSetHead, a superset of commit/write/flush).
func (a *parallelAccumulator) addProcessBlock(setup, validate, persist time.Duration) {
	a.setupNs.Add(int64(setup))
	a.validateNs.Add(int64(validate))
	a.persistNs.Add(int64(persist))
}

// addPrefetchStop records the trie-prefetcher shutdown time, which runs in a
// deferred call at the end of ProcessBlock.
func (a *parallelAccumulator) addPrefetchStop(d time.Duration) {
	a.prefetchStopNs.Add(int64(d))
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
	a.accountMissRace.Add(p.reads.AccountMissRace)
	a.accountMissNoPf.Add(p.reads.AccountMissNoPrefetch)
	a.storageHit.Add(p.reads.StorageCacheHit)
	a.storageMiss.Add(p.reads.StorageCacheMiss)
	a.storageMissNs.Add(int64(p.reads.StorageMissTime))
	a.storageMissRace.Add(p.reads.StorageMissRace)
	a.storageMissNoPf.Add(p.reads.StorageMissNoPrefetch)
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

		process      = time.Duration(a.totalNs.Load())
		setup        = time.Duration(a.setupNs.Load())
		validate     = time.Duration(a.validateNs.Load())
		persist      = time.Duration(a.persistNs.Load())
		prefetchStop = time.Duration(a.prefetchStopNs.Load())
		decode       = time.Duration(a.decodeNs.Load())
		commit       = time.Duration(a.commitNs.Load())
		blockWrite   = time.Duration(a.blockWriteNs.Load())
		trieFlush    = time.Duration(a.trieFlushNs.Load())
		// head+snapshot+events: whatever writeBlockAndSetHead does beyond the
		// commit/write/flush sub-phases already broken out.
		headSnap = persist - commit - blockWrite - trieFlush
		// Non-overlapping serial segments; their sum is the wall-clock floor that
		// single-threaded work imposes on the import (commit/write/flush are
		// subsets of persist and deliberately excluded from the sum).
		accounted = process + setup + validate + persist + prefetchStop + decode
	)
	if txExec > 0 {
		efficiency = float64(txWork) / float64(txExec)
	}
	summary := fmt.Sprintf(`Parallel execution summary (BAL-driven):
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
  setup (state/rdr):  %v
  validate:           %v
  persist (write):    %v
    commit (trie):    %v
    block write:      %v
    trie flush:       %v
    head+snap+events: %v
  prefetch stop:      %v
  rlp decode:         %v
  ---- accounted serial total ----
  Process+setup+validate+persist+prefetchStop+decode = %v
  ---- shared-cache reads ----
  account: %d hits, %d misses (%.1f%% hit), miss I/O %v
  storage: %d hits, %d misses (%.1f%% hit), miss I/O %v
  miss attribution:   account %d raced / %d uncovered / %d unhinted, storage %d raced / %d uncovered / %d unhinted`,
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
		common.PrettyDuration(process),
		common.PrettyDuration(setup),
		common.PrettyDuration(validate),
		common.PrettyDuration(persist),
		common.PrettyDuration(commit),
		common.PrettyDuration(blockWrite),
		common.PrettyDuration(trieFlush),
		common.PrettyDuration(headSnap),
		common.PrettyDuration(prefetchStop),
		common.PrettyDuration(decode),
		common.PrettyDuration(accounted),
		a.accountHit.Load(), a.accountMiss.Load(), hitRate(a.accountHit.Load(), a.accountMiss.Load()), common.PrettyDuration(time.Duration(a.accountMissNs.Load())),
		a.storageHit.Load(), a.storageMiss.Load(), hitRate(a.storageHit.Load(), a.storageMiss.Load()), common.PrettyDuration(time.Duration(a.storageMissNs.Load())),
		a.accountMissRace.Load(), a.accountMiss.Load()-a.accountMissRace.Load()-a.accountMissNoPf.Load(), a.accountMissNoPf.Load(),
		a.storageMissRace.Load(), a.storageMiss.Load()-a.storageMissRace.Load()-a.storageMissNoPf.Load(), a.storageMissNoPf.Load(),
	)
	// Append a few samples of the uncovered reads (missed after the prefetch
	// completion), identifying the state not covered by the access list hint.
	if accounts, storages := state.ReadUncoveredSamples(); len(accounts) > 0 || len(storages) > 0 {
		summary += "\n  uncovered samples: "
		for _, addr := range accounts {
			summary += fmt.Sprintf(" %x", addr)
		}
		for _, entry := range storages {
			summary += " " + entry
		}
	}
	// Append the triedb commit breakdown if any state commit was performed.
	// The serial part is a subset of the commit (trie) phase above; whatever
	// remains ("outside triedb") is the statedb-side commit cost (trie node
	// collection, account/storage set conversion etc). The background part
	// is overlapped with the block processing and not on the critical path.
	if cs := pathdb.ReadCommitStats(); cs.Updates > 0 {
		var (
			update    = cs.DiffLayerTime + cs.TreeAddTime + cs.TreeCapTime
			capOther  = cs.TreeCapTime - cs.HistoryStateTime - cs.HistoryTrienodeTime - cs.BufferAppendTime - cs.FreezeTime
			outside   = commit - update
			ss        = state.ReadStateCommitStats()
			dbConvert = ss.DBCommitTime - update
		)
		summary += fmt.Sprintf(`
  ---- triedb commit breakdown (inside commit(trie), serial) ----
  triedb update:      %v   (%d updates)
    difflayer build:  %v
    layer link (add): %v   (incl. lookup index %v)
    layer cap:        %v
      history (state):    %v
      history (trienode): %v
      buffer append:      %v
      buffer freeze:      %v   (%d freezes, wait-flush %v)
      cap other:          %v   (incl. lookup unindex %v)
  outside triedb:     %v   (statedb commit minus triedb update)
    root residual:    %v
    destruction:      %v
    trie commit:      %v   (account+storage tries, parallel wall-clock)
    update build:     %v
    db convert:       %v   (db.Commit minus triedb update)
    reader swap:      %v
  ---- triedb background (overlapped) ----
  buffer compaction:  %v   (%d runs)
  buffer flush:       %v   (%d flushes, incl. flatten %v)`,
			common.PrettyDuration(update), cs.Updates,
			common.PrettyDuration(cs.DiffLayerTime),
			common.PrettyDuration(cs.TreeAddTime), common.PrettyDuration(cs.LookupAddTime),
			common.PrettyDuration(cs.TreeCapTime),
			common.PrettyDuration(cs.HistoryStateTime),
			common.PrettyDuration(cs.HistoryTrienodeTime),
			common.PrettyDuration(cs.BufferAppendTime),
			common.PrettyDuration(cs.FreezeTime), cs.Freezes, common.PrettyDuration(cs.WaitFlushTime),
			common.PrettyDuration(capOther), common.PrettyDuration(cs.LookupRemoveTime),
			common.PrettyDuration(outside),
			common.PrettyDuration(ss.RootTime),
			common.PrettyDuration(ss.DestructTime),
			common.PrettyDuration(ss.TrieCommitTime),
			common.PrettyDuration(ss.UpdateBuildTime),
			common.PrettyDuration(dbConvert),
			common.PrettyDuration(ss.ReaderTime),
			common.PrettyDuration(cs.CompactTime), cs.Compactions,
			common.PrettyDuration(cs.FlushTime), cs.Flushes, common.PrettyDuration(cs.FlattenTime),
		)
	}
	// Append the BAL state prefetcher activity if any run was scheduled. All
	// the prefetch runs are overlapped with the transaction execution, thus
	// the numbers below are not part of the serial wall-clock. The lead time
	// (completion ahead of the execution end) along with the interrupt rate
	// reveals whether the prefetching outpaces the execution or not.
	if ps := state.ReadPrefetchStats(); ps.Runs+ps.Interrupted > 0 {
		var (
			avgPrefetch time.Duration
			avgLead     time.Duration
			avgWeight   int64
		)
		if ps.Runs > 0 {
			avgPrefetch = ps.PrefetchTime / time.Duration(ps.Runs)
			avgLead = ps.Lead / time.Duration(ps.Runs)
		}
		if scheduled := ps.Runs + ps.Interrupted; scheduled > 0 {
			avgWeight = ps.Weight / scheduled
		}
		summary += fmt.Sprintf(`
  ---- BAL state prefetch (background, overlapped) ----
  runs:               %d completed, %d interrupted (%.1f%% interrupted)
  task weight:        %d accounts+slots (avg %d /block)
  prefetch time:      %v   (avg %v /block)
  finish lead:        %v   (avg %v ahead of exec end)
  stop wait:          %v   (blocking, part of prefetch stop)`,
			ps.Runs, ps.Interrupted, hitRate(ps.Interrupted, ps.Runs),
			ps.Weight, avgWeight,
			common.PrettyDuration(ps.PrefetchTime), common.PrettyDuration(avgPrefetch),
			common.PrettyDuration(ps.Lead), common.PrettyDuration(avgLead),
			common.PrettyDuration(ps.Wait),
		)
	}
	return summary
}

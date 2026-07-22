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
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"golang.org/x/sync/errgroup"
)

// Per-phase timers for BAL-driven parallel block execution.
var (
	parallelSystemExecTimer = metrics.NewRegisteredResettingTimer("chain/execution/parallel/system", nil)
	parallelTxExecTimer     = metrics.NewRegisteredResettingTimer("chain/execution/parallel/transactions", nil)
	parallelStateApplyTimer  = metrics.NewRegisteredResettingTimer("chain/execution/parallel/stateapply", nil)
	parallelStateHashTimer   = metrics.NewRegisteredResettingTimer("chain/execution/parallel/statehash", nil)
	parallelStateCommitTimer = metrics.NewRegisteredResettingTimer("chain/execution/parallel/statecommit", nil)
	parallelGatherTimer     = metrics.NewRegisteredResettingTimer("chain/execution/parallel/gather", nil)
	parallelTotalTimer      = metrics.NewRegisteredResettingTimer("chain/execution/parallel/total", nil)

	parallelTxWorkTimer = metrics.NewRegisteredResettingTimer("chain/execution/parallel/execution/sum", nil)
	parallelTxMaxTimer  = metrics.NewRegisteredResettingTimer("chain/execution/parallel/execution/max", nil)
	parallelEfficiency  = metrics.NewRegisteredGaugeFloat64("chain/execution/parallel/execution/efficiency", nil)

	parallelAccountCacheHitMeter  = metrics.NewRegisteredMeter("chain/execution/parallel/reads/account/cache/hit", nil)
	parallelAccountCacheMissMeter = metrics.NewRegisteredMeter("chain/execution/parallel/reads/account/cache/miss", nil)
	parallelStorageCacheHitMeter  = metrics.NewRegisteredMeter("chain/execution/parallel/reads/storage/cache/hit", nil)
	parallelStorageCacheMissMeter = metrics.NewRegisteredMeter("chain/execution/parallel/reads/storage/cache/miss", nil)
	parallelAccountMissTimeTimer  = metrics.NewRegisteredResettingTimer("chain/execution/parallel/reads/account/miss/time", nil)
	parallelStorageMissTimeTimer  = metrics.NewRegisteredResettingTimer("chain/execution/parallel/reads/storage/miss/time", nil)
)

// supportsParallelExecution reports whether the block can be executed using the
// BAL-driven parallel processor.
func supportsParallelExecution(accessList *bal.BlockAccessList, config *params.ChainConfig, number *big.Int, time uint64, wantWitness bool, wantTrace bool, disableParallel bool) bool {
	// Parallel execution explicitly disabled via config (e.g. by tests that
	// want to force the sequential path).
	if disableParallel {
		return false
	}
	// Disable the parallel execution if either the Amsterdam hasn't been
	// activated, or the accessList is not accessible.
	if accessList == nil || !config.IsAmsterdam(number, time) {
		return false
	}
	// No tracer is attached (tracing requires the strict sequential
	// ordering of state operations that parallel execution does not
	// preserve).
	if wantTrace {
		return false
	}
	// No witness is being collected (witness building must observe
	// every state access alongside the proof).
	if wantWitness {
		return false
	}
	return true
}

// txExecResult holds the per-transaction outcome of parallel execution.
type txExecResult struct {
	receipt    *types.Receipt
	accessList *bal.ConstructionBlockAccessList

	// regular and state are the EIP-8037 per-transaction
	// gas contributions to the two block-inclusion dimensions.
	regular uint64
	state   uint64
}

// processParallel executes the block's transactions concurrently using the
// block-level access list.
func (p *StateProcessor) processParallel(ctx context.Context, block *types.Block, statedb *state.StateDB, jumpDestCache vm.JumpDestCache, cfg vm.Config) (*ProcessResult, error) {
	var (
		config = p.chainConfig()
		header = block.Header()
		txs    = block.Transactions()
		start  = time.Now()

		signer    = types.MakeSigner(config, header.Number, header.Time)
		context   = NewEVMBlockContext(header, p.chain, nil)
		postIndex = uint32(len(txs) + 1)
		db        = statedb.Database()

		accessList = block.AccessList()
		lookup     = accessList.Lookup()

		// blockAccessList is the access list rebuilt from the actual execution.
		blockAccessList = bal.NewConstructionBlockAccessList()
	)

	// Resolve the parent state root, the point all execution reads from.
	parent := p.chain.GetHeader(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent header %x not found", block.ParentHash())
	}
	parentRoot := parent.Root

	// The base reader: the underlying state reader wrapped with a shared
	// cache and an access-list-hint prefetcher. This reader is shared by
	// all tx-executors.
	base := statedb.Reader()

	// Stats
	var (
		systemExec  time.Duration
		txExec      time.Duration
		stateApply  time.Duration
		stateHash   time.Duration
		stateCommit time.Duration
	)
	// Post-execution state root, computed concurrently with execution.
	var wg errgroup.Group
	wg.Go(func() error {
		start := time.Now()
		if err := statedb.ApplyBlockAccessList(accessList); err != nil {
			return err
		}
		stateApply = time.Since(start)

		start = time.Now()
		statedb.IntermediateRoot(config.IsEIP158(header.Number))
		stateHash = time.Since(start)
		if err := statedb.Error(); err != nil {
			return err
		}
		// Eagerly construct the state update (trie node collection etc.) right
		// after the state root is finalized, overlapping it with the remaining
		// transaction execution and result gathering. The prepared update will
		// be consumed by the database commit in the persistence stage, leaving
		// only the flushing on the serial path.
		//
		// Note this operation invalidates the internal tries of the statedb;
		// IntermediateRoot returns the finalized root from then on.
		start = time.Now()
		if err := statedb.PrepareCommit(header.Number.Uint64(), config.IsEIP158(header.Number), config.IsCancun(header.Number, header.Time)); err != nil {
			return err
		}
		stateCommit = time.Since(start)
		return nil
	})
	// Ensure the root goroutine has stopped mutating the canonical state before
	// returning on any path, including the error paths below. Wait is idempotent,
	// so the explicit join on the happy path remains valid.
	defer func() { _ = wg.Wait() }()

	// Pre-execution system calls, replayed against an ephemeral access-list
	// state at block-access index 0, to contribute their entries to the rebuilt
	// access list.
	//
	// TODO(rjl493456442) both the pre/post execution can be performed alongside
	// the transaction execution. Measure the overhead before making the changes.
	preStart := time.Now()
	preState, err := newAccessListState(db, parentRoot, base, lookup, 0)
	if err != nil {
		return nil, err
	}
	preEVM := vm.NewEVM(context, preState, config, cfg)
	if jumpDestCache != nil {
		preEVM.SetJumpDestCache(jumpDestCache)
	}
	blockAccessList.Merge(PreExecution(ctx, block.BeaconRoot(), parent, config, preEVM, header.Number, header.Time))
	preEVM.Release()
	systemExec += time.Since(preStart)

	// Execute the transactions concurrently. Each transaction runs against its
	// own ephemeral state instance, whose reads are served from the block-level
	// access list overlaid on the parent state.
	txStart := time.Now()
	results, profile, err := p.executeTransactionsParallel(block, parentRoot, db, base, lookup, context, signer, jumpDestCache, cfg)
	if err != nil {
		return nil, err
	}
	txExec = time.Since(txStart)

	var (
		gatherStart = time.Now()
		receipts    = make(types.Receipts, 0, len(txs))
		allLogs     []*types.Log
		gp          = NewGasPool(block.GasLimit())
		logIndex    uint
	)
	for i := range txs {
		receipt := results[i].receipt
		gasLimit := txs[i].Gas()
		if err := gp.CheckGasAmsterdam(min(gasLimit, params.MaxTxGas), gasLimit); err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, txs[i].Hash().Hex(), err)
		}
		if err := gp.ChargeGasAmsterdam(results[i].regular, results[i].state, receipt.GasUsed); err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, txs[i].Hash().Hex(), err)
		}
		// Correct the receipt object with block-level fields
		receipt.CumulativeGasUsed = gp.CumulativeUsed()
		for _, lg := range receipt.Logs {
			lg.Index = logIndex
			logIndex++
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		blockAccessList.Merge(results[i].accessList)
	}
	gather := time.Since(gatherStart)

	// Post-execution system calls against an ephemeral access-list state at
	// index n+1.
	postStart := time.Now()
	postState, err := newAccessListState(db, parentRoot, base, lookup, int(postIndex))
	if err != nil {
		return nil, err
	}
	postEVM := vm.NewEVM(context, postState, config, cfg)
	if jumpDestCache != nil {
		postEVM.SetJumpDestCache(jumpDestCache)
	}
	requests, postBAL, err := PostExecution(ctx, config, header.Number, header.Time, allLogs, postEVM, postIndex)
	postEVM.Release()
	if err != nil {
		return nil, err
	}
	blockAccessList.Merge(postBAL)
	p.chain.Engine().Finalize(p.chain, header, postState, block.Body(), postIndex, blockAccessList)
	systemExec += time.Since(postStart)

	// Join the concurrent root computation.
	if err := wg.Wait(); err != nil {
		return nil, err
	}

	// Publish the execution statistics
	parallelSystemExecTimer.Update(systemExec)
	parallelTxExecTimer.Update(txExec)
	parallelStateApplyTimer.Update(stateApply)
	parallelStateHashTimer.Update(stateHash)
	parallelStateCommitTimer.Update(stateCommit)
	parallelGatherTimer.Update(gather)
	parallelTotalTimer.UpdateSince(start)
	parallelTxWorkTimer.Update(profile.totalTime)
	parallelTxMaxTimer.Update(profile.maxTime)

	var efficiency float64
	if txExec > 0 {
		efficiency = float64(profile.totalTime) / float64(txExec)
	}
	if efficiency > 0 {
		parallelEfficiency.Update(efficiency)
	}
	reportParallelReadStats(header.Number, profile.reads)
	parallelStats.add(len(txs), txExec, systemExec, gather, stateApply, stateHash, stateCommit, time.Since(start), profile)

	log.Debug("Parallel block execution", "number", header.Number, "txs", len(txs),
		"system", common.PrettyDuration(systemExec), "txexec", common.PrettyDuration(txExec),
		"txsum", common.PrettyDuration(profile.totalTime), "txmax", common.PrettyDuration(profile.maxTime),
		"parallelism", fmt.Sprintf("%.2fx/%d", efficiency, profile.workers),
		"gather", common.PrettyDuration(gather),
		"stateapply", common.PrettyDuration(stateApply), "statehash", common.PrettyDuration(stateHash),
		"statecommit", common.PrettyDuration(stateCommit),
		"elapsed", common.PrettyDuration(time.Since(start)),
	)
	return &ProcessResult{
		Receipts: receipts,
		Requests: requests,
		Logs:     allLogs,
		GasUsed:  gp.Used(),
		Bal:      blockAccessList,
	}, nil
}

// newAccessListState constructs an ephemeral state, reading through base, whose
// view reflects the mutations recorded in the access list for all block-access
// indices below index.
func newAccessListState(db state.Database, parentRoot common.Hash, base state.Reader, lookup *bal.Lookup, index int) (*state.StateDB, error) {
	return state.NewWithReader(parentRoot, db, state.NewReaderWithBlockLevelAccessList(base, lookup, index))
}

// parallelProfile captures per-block instrumentation for BAL-driven parallel
// execution.
type parallelProfile struct {
	workers   int                    // number of worker goroutines used
	totalTime time.Duration          // summed per-tx execution time (serial-equivalent work)
	maxTime   time.Duration          // slowest single transaction
	reads     state.StateReaderStats // shared-cache read statistics (hits/misses/miss-time)
}

// executeTransactionsParallel applies all transactions to independent,
// access-list-backed state instances using a pool of workers, and returns
// the per-transaction results in block order.
func (p *StateProcessor) executeTransactionsParallel(block *types.Block, parentRoot common.Hash, db state.Database, base state.Reader, lookup *bal.Lookup, context vm.BlockContext, signer types.Signer, jumpDestCache vm.JumpDestCache, cfg vm.Config) ([]txExecResult, parallelProfile, error) {
	var (
		config      = p.chainConfig()
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		txs         = block.Transactions()
		results     = make([]txExecResult, len(txs))
	)
	workers := runtime.GOMAXPROCS(0)
	if workers > len(txs) {
		workers = len(txs)
	}
	var (
		cursor    atomic.Int64
		group     errgroup.Group
		totalTime atomic.Int64
		maxTime   atomic.Int64
	)
	for w := 0; w < workers; w++ {
		group.Go(func() error {
			evm := vm.NewEVM(context, nil, config, cfg)
			if jumpDestCache != nil {
				evm.SetJumpDestCache(jumpDestCache)
			}
			defer evm.Release()

			for {
				i := int(cursor.Add(1)) - 1
				if i >= len(txs) {
					return nil
				}
				tx := txs[i]
				msg, err := TransactionToMessage(tx, signer, header.BaseFee)
				if err != nil {
					return fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
				}

				// Construct the dedicated pre-tx state with the BAL overlay wrapped.
				reader := state.NewReaderWithBlockLevelAccessList(base, lookup, i+1)
				sdb, err := state.NewWithReader(parentRoot, db, reader)
				if err != nil {
					return err
				}
				sdb.SetTxContext(tx.Hash(), i, uint32(i+1))
				evm.SetStateDB(sdb)

				// A transaction-local gas pool, sized to the transaction's own gas
				// limit: enough to let the state transition run to completion.
				gp := NewGasPool(msg.GasLimit)
				txStart := time.Now()
				receipt, accessList, err := ApplyTransactionWithEVM(msg, gp, sdb, blockNumber, blockHash, context.Time, tx, evm)
				if err != nil {
					return fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
				}
				elapsed := int64(time.Since(txStart))
				totalTime.Add(elapsed)

				for { // lock-free max
					cur := maxTime.Load()
					if elapsed <= cur || maxTime.CompareAndSwap(cur, elapsed) {
						break
					}
				}
				results[i] = txExecResult{
					receipt:    receipt,
					accessList: accessList,
					regular:    gp.CumulativeRegular(),
					state:      gp.CumulativeState(),
				}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return nil, parallelProfile{}, err
	}
	profile := parallelProfile{
		workers:   workers,
		totalTime: time.Duration(totalTime.Load()),
		maxTime:   time.Duration(maxTime.Load()),
		reads:     stateReadStats(base),
	}
	return results, profile, nil
}

// stateReadStats extracts the shared-cache read statistics from the base reader,
// returning the zero value if the reader does not track them.
func stateReadStats(reader state.Reader) state.StateReaderStats {
	stater, ok := reader.(state.ReaderStater)
	if !ok {
		return state.StateReaderStats{}
	}
	return stater.GetStats().StateStats
}

// reportParallelReadStats publishes the shared-cache read statistics to the
// metrics registry and logs a summary. TODO(rjl) integrate it into blockchain
// stats.
func reportParallelReadStats(number *big.Int, stats state.StateReaderStats) {
	parallelAccountCacheHitMeter.Mark(stats.AccountCacheHit)
	parallelAccountCacheMissMeter.Mark(stats.AccountCacheMiss)
	parallelStorageCacheHitMeter.Mark(stats.StorageCacheHit)
	parallelStorageCacheMissMeter.Mark(stats.StorageCacheMiss)
	parallelAccountMissTimeTimer.Update(stats.AccountMissTime)
	parallelStorageMissTimeTimer.Update(stats.StorageMissTime)

	log.Debug("Parallel execution read statistics", "number", number,
		"account.hit", stats.AccountCacheHit, "account.miss", stats.AccountCacheMiss,
		"account.hitrate", stats.AccountCacheHitRate(), "account.misstime", common.PrettyDuration(stats.AccountMissTime),
		"storage.hit", stats.StorageCacheHit, "storage.miss", stats.StorageCacheMiss,
		"storage.hitrate", stats.StorageCacheHitRate(), "storage.misstime", common.PrettyDuration(stats.StorageMissTime))
}

// prefetchHint returns a set of storage slots alongside their account address
// for batch reading.
func prefetchHint(list *bal.BlockAccessList) map[common.Address][]common.Hash {
	hint := make(map[common.Address][]common.Hash, len(*list))
	for i := range *list {
		acc := &(*list)[i]
		slots := make([]common.Hash, 0, len(acc.StorageReads)+len(acc.StorageChanges))
		for _, slot := range acc.StorageReads {
			slots = append(slots, slot.Bytes32())
		}
		for j := range acc.StorageChanges {
			slots = append(slots, acc.StorageChanges[j].Slot.Bytes32())
		}
		hint[acc.Address] = slots
	}
	return hint
}

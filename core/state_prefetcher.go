// Copyright 2019 The go-ethereum Authors
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
	"bytes"
	"runtime"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"golang.org/x/sync/errgroup"
)

// statePrefetcher is a basic Prefetcher that executes transactions from a block
// on top of the parent state, aiming to prefetch potentially useful state data
// from disk. Transactions are executed in parallel to fully leverage the
// SSD's read performance.
type statePrefetcher struct {
	config    *params.ChainConfig // Chain configuration options
	chain     *HeaderChain        // Canonical block chain
	headStart int                 // Number of transactions to prefetch before signaling ready
}

// newStatePrefetcher initialises a new statePrefetcher.
// headStart controls how many transactions to prefetch before signaling ready.
// A value of 0 means signal ready immediately (no head start).
func newStatePrefetcher(config *params.ChainConfig, chain *HeaderChain, headStart int) *statePrefetcher {
	if headStart < 0 {
		headStart = 0
	}
	return &statePrefetcher{
		config:    config,
		chain:     chain,
		headStart: headStart,
	}
}

// Prefetch processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to warm the state caches.
//
// If ready is non-nil, it will be closed after the first few transactions have been
// prefetched, signaling that the executor can start processing.
func (p *statePrefetcher) Prefetch(block *types.Block, statedb *state.StateDB, cfg vm.Config, interrupt *atomic.Bool, ready chan struct{}) {
	var (
		fails   atomic.Int64
		header  = block.Header()
		signer  = types.MakeSigner(p.config, header.Number, header.Time)
		workers errgroup.Group
		reader  = statedb.Reader()
	)
	workers.SetLimit(max(1, 4*runtime.NumCPU()/5)) // Aggressively run the prefetching

	txs := block.Transactions()
	earlyTxs := min(p.headStart, len(txs))

	// If no head start, signal ready immediately (matches original behavior where
	// prefetcher and main executor ran completely in parallel from the start)
	if earlyTxs == 0 && ready != nil {
		close(ready)
		ready = nil // Prevent double-close later
	}

	// prefetchTx executes a single transaction to warm the cache
	prefetchTx := func(i int, tx *types.Transaction, stateCpy *state.StateDB) {
		if interrupt != nil && interrupt.Load() {
			return
		}
		stateCpy.StartPrefetcher("prefetcher", nil, nil)

		sender, err := types.Sender(signer, tx)
		if err != nil {
			fails.Add(1)
			return
		}
		reader.Account(sender)

		if tx.To() != nil {
			account, _ := reader.Account(*tx.To())
			if account != nil && !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
				reader.Code(*tx.To(), common.BytesToHash(account.CodeHash))
			}
		}
		for _, list := range tx.AccessList() {
			reader.Account(list.Address)
			if len(list.StorageKeys) > 0 {
				for _, slot := range list.StorageKeys {
					reader.Storage(list.Address, slot)
				}
			}
		}

		evm := vm.NewEVM(NewEVMBlockContext(header, p.chain, nil), stateCpy, p.config, cfg)
		msg, err := TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			fails.Add(1)
			return
		}
		msg.SkipNonceChecks = true
		stateCpy.SetTxContext(tx.Hash(), i)

		if _, err := ApplyMessage(evm, msg, new(GasPool).AddGas(block.GasLimit())); err != nil {
			fails.Add(1)
		}
		// Emit the prefetching tasks at the end of transaction. Ideally stream them
		// out alongside the execution. TODO(rjl493456442)
		stateCpy.Finalise(true)
		stateCpy.StopPrefetcher() // Block until all the prefetching tasks are completed
	}

	// Start remaining transactions first (they run in background)
	for i := earlyTxs; i < len(txs); i++ {
		idx, tx, stateCpy := i, txs[i], statedb.Copy()
		workers.Go(func() error {
			prefetchTx(idx, tx, stateCpy)
			return nil
		})
	}

	// Prefetch earlyTxs with dedicated workers, then signal ready.
	// This ensures txs 0-earlyTxs are complete before executor starts, while remaining
	// txs are already running in parallel.
	var earlyWorkers errgroup.Group
	earlyWorkers.SetLimit(earlyTxs) // All earlyTxs can run in parallel
	for i := 0; i < earlyTxs; i++ {
		idx, tx, stateCpy := i, txs[i], statedb.Copy()
		earlyWorkers.Go(func() error {
			prefetchTx(idx, tx, stateCpy)
			return nil
		})
	}
	earlyWorkers.Wait() // Wait only for first earlyTxs

	// Signal executor can start
	if ready != nil {
		close(ready)
	}

	// Wait for remaining txs to complete
	workers.Wait()

	blockPrefetchTxsValidMeter.Mark(int64(len(block.Transactions())) - fails.Load())
	blockPrefetchTxsInvalidMeter.Mark(fails.Load())
}

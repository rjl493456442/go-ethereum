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

package state

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
)

// The EIP27928 reader utilizes a hierarchical architecture to optimize state
// access during block execution:
//
// - Base layer: The reader is initialized with the pre-transition state root,
//   providing the access of the state.
//
// - Prefetching Layer: This base reader is wrapped by newPrefetchStateReader.
//   Using an Access List hint, it asynchronously fetches required state data
//   in the background, minimizing I/O blocking during transaction processing.
//
// - Execution Layer: To support parallel transaction execution within the EIP
//   7928 context, readers are wrapped in ReaderWithBlockLevelAccessList.
//   This layer provides a "unified view" by merging the pre-transition state
//   with mutated states from preceding transactions in the block.
//
// - Tracking Layer: Finally, the readerTracker wraps the execution reader to
//   capture all state reads made during a specific transaction. These individual
//   reads are subsequently merged to construct a comprehensive access list
//   for the entire block.
//
// The architecture can be illustrated by the diagram below:
//
//   ┌──────────────┴──────────────┐    ┌──────────────┴──────────────┐
//   │ ReaderWithBlockLevelAL      │    │ ReaderWithBlockLevelAL      │
//   │ (Pre-state + Mutations)     │    │ (Pre-state + Mutations)     │
//   └──────────────┬──────────────┘    └──────────────┬──────────────┘
//                  │                                  │
//                  └────────────────┬─────────────────┘
//                                   │
//                    ┌──────────────┴──────────────┐
//                    │    newPrefetchStateReader   │ (Async I/O)
//                    │  (Access List Hint driven)  │
//                    └──────────────┬──────────────┘
//                                   │
//                    ┌──────────────┴──────────────┐
//                    │        Base Reader          │ (State Root)
//                    │ (State & Contract Code)     │
//                    └─────────────────────────────┘

// Note: The block producer, which is responsible for generating the block
// along with the block-level access list, does not maintain the internal
// hierarchy (e.g., PrefetchStateReader or ReaderWithBlockLevelAL).
// Instead, it directly utilizes the readerTracker, wrapped around the
// base reader, to construct the access list.

// prefetchAccumulator accumulates the BAL prefetcher activity across every
// block processed in this process lifetime. It is cumulative and O(1) in
// memory, making it suitable for an end-of-run summary such as geth import.
var prefetchAccumulator struct {
	runs        atomic.Int64 // number of the fully completed prefetch runs
	interrupted atomic.Int64 // number of the prefetch runs terminated before completion
	prefetchNs  atomic.Int64 // summed duration of the fully completed runs
	leadNs      atomic.Int64 // summed lead time of the completion ahead of the closing
	waitNs      atomic.Int64 // summed close blocking time on the termination
	weight      atomic.Int64 // summed task weight (accounts + slots)
}

// PrefetchStats is a cumulative snapshot of the BAL prefetcher activity.
type PrefetchStats struct {
	Runs         int64         // Number of the fully completed prefetch runs
	Interrupted  int64         // Number of the prefetch runs terminated before completion
	PrefetchTime time.Duration // Summed duration of the fully completed runs
	Lead         time.Duration // Summed lead time of the completion ahead of the closing
	Wait         time.Duration // Summed close blocking time on the termination
	Weight       int64         // Summed task weight (accounts + slots)
}

// ReadPrefetchStats returns a cumulative snapshot of the BAL prefetcher
// activity across every block processed in this process lifetime.
func ReadPrefetchStats() PrefetchStats {
	return PrefetchStats{
		Runs:         prefetchAccumulator.runs.Load(),
		Interrupted:  prefetchAccumulator.interrupted.Load(),
		PrefetchTime: time.Duration(prefetchAccumulator.prefetchNs.Load()),
		Lead:         time.Duration(prefetchAccumulator.leadNs.Load()),
		Wait:         time.Duration(prefetchAccumulator.waitNs.Load()),
		Weight:       prefetchAccumulator.weight.Load(),
	}
}

type fetchTask struct {
	addr  common.Address
	slots []common.Hash
}

func (t *fetchTask) weight() int { return 1 + len(t.slots) }

type prefetchStateReader struct {
	StateReader

	tasks     []*fetchTask
	nThreads  int
	done      chan struct{}
	term      chan struct{}
	closeOnce sync.Once

	interrupted atomic.Bool  // whether any worker was terminated before completion
	finished    atomic.Int64 // nanotime of the natural prefetch completion, 0 if not completed
}

func newPrefetchStateReader(reader StateReader, accessList map[common.Address][]common.Hash, nThreads int) *prefetchStateReader {
	tasks := make([]*fetchTask, 0, len(accessList))
	for addr, slots := range accessList {
		tasks = append(tasks, &fetchTask{
			addr:  addr,
			slots: slots,
		})
	}
	return newPrefetchStateReaderInternal(reader, tasks, nThreads)
}

func newPrefetchStateReaderInternal(reader StateReader, tasks []*fetchTask, nThreads int) *prefetchStateReader {
	r := &prefetchStateReader{
		StateReader: reader,
		tasks:       tasks,
		nThreads:    nThreads,
		done:        make(chan struct{}),
		term:        make(chan struct{}),
	}
	go r.prefetch()
	return r
}

func (r *prefetchStateReader) Close() {
	r.closeOnce.Do(func() {
		select {
		case <-r.done:
			// The prefetch has already been completed naturally, measure how
			// long it finished ahead of the closing (typically the end of the
			// block execution).
			if finished := r.finished.Load(); finished != 0 {
				lead := time.Since(time.Unix(0, finished))
				prefetchLeadTimer.Update(lead)
				prefetchAccumulator.leadNs.Add(int64(lead))
			}
			close(r.term)
		default:
			// The prefetch is still running, terminate it and measure the
			// blocking time on the termination.
			start := time.Now()
			close(r.term)
			<-r.done
			prefetchWaitTimer.UpdateSince(start)
			prefetchAccumulator.waitNs.Add(int64(time.Since(start)))
		}
	})
}

func (r *prefetchStateReader) Wait() error {
	select {
	case <-r.term:
		return nil
	case <-r.done:
		return nil
	}
}

func (r *prefetchStateReader) prefetch() {
	defer close(r.done)

	if len(r.tasks) == 0 {
		return
	}
	var total int
	for _, t := range r.tasks {
		total += t.weight()
	}
	prefetchTaskWeightHist.Update(int64(total))
	prefetchAccumulator.weight.Add(int64(total))

	var (
		begin = time.Now()
		wg    sync.WaitGroup
		unit  = (total + r.nThreads - 1) / r.nThreads // round-up the per worker unit
	)
	for i := 0; i < r.nThreads; i++ {
		start := i * unit
		if start >= total {
			break
		}
		limit := (i + 1) * unit
		if i == r.nThreads-1 {
			limit = total
		}
		// Schedule the worker for prefetching, the items on the range [start, limit)
		// is exclusively assigned for this worker.
		wg.Add(1)
		go func(workerID, startW, endW int) {
			r.process(startW, endW)
			wg.Done()
		}(i, start, limit)
	}
	wg.Wait()

	// Only the fully completed runs are counted for the completion time,
	// the interrupted ones are tracked separately.
	if r.interrupted.Load() {
		prefetchInterruptMeter.Mark(1)
		prefetchAccumulator.interrupted.Add(1)
	} else {
		prefetchTimeTimer.UpdateSince(begin)
		prefetchAccumulator.runs.Add(1)
		prefetchAccumulator.prefetchNs.Add(int64(time.Since(begin)))
		r.finished.Store(time.Now().UnixNano())
	}
}

func (r *prefetchStateReader) process(start, limit int) {
	var total = 0
	for _, t := range r.tasks {
		tw := t.weight()
		if total+tw > start {
			s := 0
			if start > total {
				s = start - total
			}
			l := tw
			if limit < total+tw {
				l = limit - total
			}
			for j := s; j < l; j++ {
				select {
				case <-r.term:
					r.interrupted.Store(true)
					return
				default:
					if j == 0 {
						r.StateReader.Account(t.addr)
					} else {
						r.StateReader.Storage(t.addr, t.slots[j-1])
					}
				}
			}
		}
		total += tw
		if total >= limit {
			return
		}
	}
}

// NewBlockExecutionReader wraps base with a shared, concurrency-safe cache so
// that any state resolved once, whether by the background prefetcher or by
// transaction execution, is not fetched from the underlying reader again.
func NewBlockExecutionReader(base Reader, prefetch map[common.Address][]common.Hash, threads int) (Reader, func()) {
	var (
		cache = newStateReaderWithCache(base)
		stats = newStateReaderWithStats(cache)
		stop  = func() {}
	)
	if len(prefetch) > 0 && threads > 0 {
		pf := newPrefetchStateReader(cache, prefetch, threads)
		stop = pf.Close

		// Attach the probe for attributing the cache misses to either the
		// prefetch race or the prefetch coverage gap.
		stats.prefetchActive = func() bool {
			select {
			case <-pf.done:
				return false
			default:
				return true
			}
		}
	}
	return newReader(base, stats), stop
}

// ReaderWithBlockLevelAccessList provides state access that reflects the
// pre-transition state combined with the mutations made by transactions
// prior to TxIndex.
type ReaderWithBlockLevelAccessList struct {
	Reader
	lookup  *bal.Lookup
	txIndex uint32
}

// NewReaderWithBlockLevelAccessList constructs a reader for accessing states
// with the mutations made by transactions prior to txIndex.
//
// The txIndex refers to the call frame as such:
// - 0 for pre‑execution system contract calls.
// - 1 … n for transactions (in block order).
// - n + 1 for post‑execution system contract calls.
func NewReaderWithBlockLevelAccessList(base Reader, lookup *bal.Lookup, txIndex int) *ReaderWithBlockLevelAccessList {
	return &ReaderWithBlockLevelAccessList{
		Reader:  base,
		lookup:  lookup,
		txIndex: uint32(txIndex),
	}
}

// Account implements Reader, returning the account with the specific address.
//
// The returned account reflects the pre-transition state overlaid with all
// mutations made by call frames prior to the reader's TxIndex.
func (r *ReaderWithBlockLevelAccessList) Account(addr common.Address) (*types.StateAccount, error) {
	base, err := r.Reader.Account(addr)
	if err != nil {
		return nil, err
	}
	balance, nonce, code, hasBalance, hasNonce, hasCode := r.lookup.AccountChanges(addr, r.txIndex)

	// No mutation precedes the current call frame, return the base account as is.
	if !hasBalance && !hasNonce && !hasCode {
		return base, nil
	}
	// Overlay the mutations on top of a copy of the base account. The base
	// account must not be mutated in place: with a shared cache in front of the
	// underlying reader, the same instance is handed to concurrent readers.
	account := types.NewEmptyStateAccount()
	if base != nil {
		account = base.Copy()
	}
	if hasBalance {
		account.Balance = balance.Clone()
	}
	if hasNonce {
		account.Nonce = nonce
	}
	if hasCode {
		if len(code) == 0 {
			account.CodeHash = types.EmptyCodeHash.Bytes()
		} else {
			account.CodeHash = crypto.Keccak256(code)
		}
	}
	return account, nil
}

// Storage implements Reader, returning the storage slot with the specific
// address and slot key.
func (r *ReaderWithBlockLevelAccessList) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	if value, ok := r.lookup.Storage(addr, slot, r.txIndex); ok {
		return value, nil
	}
	return r.Reader.Storage(addr, slot)
}

// Has implements Reader, returning the flag indicating whether the contract
// code with specified address and hash exists or not.
func (r *ReaderWithBlockLevelAccessList) Has(addr common.Address, codeHash common.Hash) bool {
	if _, ok := r.lookup.Code(addr, r.txIndex); ok {
		return true
	}
	return r.Reader.Has(addr, codeHash)
}

// Code implements Reader, returning the contract code with specified address
// and hash. Code created earlier in the block (and therefore absent from the
// pre-transition state) is served directly from the access list.
func (r *ReaderWithBlockLevelAccessList) Code(addr common.Address, codeHash common.Hash) []byte {
	if code, ok := r.lookup.Code(addr, r.txIndex); ok {
		return code
	}
	return r.Reader.Code(addr, codeHash)
}

// CodeSize implements Reader, returning the contract code size with specified
// address and hash.
func (r *ReaderWithBlockLevelAccessList) CodeSize(addr common.Address, codeHash common.Hash) int {
	if code, ok := r.lookup.Code(addr, r.txIndex); ok {
		return len(code)
	}
	return r.Reader.CodeSize(addr, codeHash)
}

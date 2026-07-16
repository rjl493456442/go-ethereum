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
	"github.com/ethereum/go-ethereum/log"
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

	accountStats prefetchReadStats
	storageStats prefetchReadStats
}

type prefetchReadStats struct {
	reads                atomic.Int64
	hits                 atomic.Int64
	misses               atomic.Int64
	lateHits             atomic.Int64
	errors               atomic.Int64
	elapsed              atomic.Int64
	readLockWait         atomic.Int64
	stateRead            atomic.Int64
	writeLockWait        atomic.Int64
	flatReads            atomic.Int64
	flatFallbacks        atomic.Int64
	flatRead             atomic.Int64
	mptReads             atomic.Int64
	mptLockWait          atomic.Int64
	mptGetAccount        atomic.Int64
	otherReads           atomic.Int64
	otherRead            atomic.Int64
	pathdbTreeLockWait   atomic.Int64
	pathdbTreeLookup     atomic.Int64
	pathdbDiffLockWait   atomic.Int64
	pathdbDiffRead       atomic.Int64
	pathdbDiskLockWait   atomic.Int64
	pathdbDiskBufferRead atomic.Int64
	pathdbDiskCacheRead  atomic.Int64
	pathdbDiskRead       atomic.Int64
	pathdbDiskCacheWrite atomic.Int64
	pathdbDecode         atomic.Int64
	pathdbFallbacks      atomic.Int64
}

func (s *prefetchReadStats) add(elapsed time.Duration, detail stateReaderCallStats, err error) {
	s.reads.Add(1)
	s.elapsed.Add(elapsed.Nanoseconds())
	s.readLockWait.Add(detail.readLockWait.Nanoseconds())
	s.stateRead.Add(detail.stateRead.Nanoseconds())
	s.writeLockWait.Add(detail.writeLockWait.Nanoseconds())
	s.flatReads.Add(detail.accountRead.flatReads)
	s.flatFallbacks.Add(detail.accountRead.flatFallbacks)
	s.flatRead.Add(detail.accountRead.flatRead.Nanoseconds())
	s.mptReads.Add(detail.accountRead.mptReads)
	s.mptLockWait.Add(detail.accountRead.mptLockWait.Nanoseconds())
	s.mptGetAccount.Add(detail.accountRead.mptGetAccount.Nanoseconds())
	s.otherReads.Add(detail.accountRead.otherReads)
	s.otherRead.Add(detail.accountRead.otherRead.Nanoseconds())
	s.pathdbTreeLockWait.Add(detail.accountRead.pathdb.TreeLockWait.Nanoseconds())
	s.pathdbTreeLookup.Add(detail.accountRead.pathdb.TreeLookup.Nanoseconds())
	s.pathdbDiffLockWait.Add(detail.accountRead.pathdb.DiffLockWait.Nanoseconds())
	s.pathdbDiffRead.Add(detail.accountRead.pathdb.DiffRead.Nanoseconds())
	s.pathdbDiskLockWait.Add(detail.accountRead.pathdb.DiskLockWait.Nanoseconds())
	s.pathdbDiskBufferRead.Add(detail.accountRead.pathdb.DiskBufferRead.Nanoseconds())
	s.pathdbDiskCacheRead.Add(detail.accountRead.pathdb.DiskCacheRead.Nanoseconds())
	s.pathdbDiskRead.Add(detail.accountRead.pathdb.DiskRead.Nanoseconds())
	s.pathdbDiskCacheWrite.Add(detail.accountRead.pathdb.DiskCacheWrite.Nanoseconds())
	s.pathdbDecode.Add(detail.accountRead.pathdb.Decode.Nanoseconds())
	s.pathdbFallbacks.Add(int64(detail.accountRead.pathdb.Fallbacks))
	s.pathdbTreeLockWait.Add(detail.storageRead.pathdb.TreeLockWait.Nanoseconds())
	s.pathdbTreeLookup.Add(detail.storageRead.pathdb.TreeLookup.Nanoseconds())
	s.pathdbDiffLockWait.Add(detail.storageRead.pathdb.DiffLockWait.Nanoseconds())
	s.pathdbDiffRead.Add(detail.storageRead.pathdb.DiffRead.Nanoseconds())
	s.pathdbDiskLockWait.Add(detail.storageRead.pathdb.DiskLockWait.Nanoseconds())
	s.pathdbDiskBufferRead.Add(detail.storageRead.pathdb.DiskBufferRead.Nanoseconds())
	s.pathdbDiskCacheRead.Add(detail.storageRead.pathdb.DiskCacheRead.Nanoseconds())
	s.pathdbDiskRead.Add(detail.storageRead.pathdb.DiskRead.Nanoseconds())
	s.pathdbDiskCacheWrite.Add(detail.storageRead.pathdb.DiskCacheWrite.Nanoseconds())
	s.pathdbDecode.Add(detail.storageRead.pathdb.Decode.Nanoseconds())
	s.pathdbFallbacks.Add(int64(detail.storageRead.pathdb.Fallbacks))
	if detail.cacheHit {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
	if detail.lateHit {
		s.lateHits.Add(1)
	}
	if err != nil {
		s.errors.Add(1)
	}
}

func (s *prefetchReadStats) values() (reads, hits, misses, lateHits, errors int64, elapsed, readLockWait, stateRead, writeLockWait time.Duration) {
	return s.reads.Load(), s.hits.Load(), s.misses.Load(), s.lateHits.Load(), s.errors.Load(),
		time.Duration(s.elapsed.Load()), time.Duration(s.readLockWait.Load()),
		time.Duration(s.stateRead.Load()), time.Duration(s.writeLockWait.Load())
}

func (s *prefetchReadStats) accountReadValues() (flatReads, flatFallbacks, mptReads, otherReads int64, flatRead, mptLockWait, mptGetAccount, otherRead time.Duration) {
	return s.flatReads.Load(), s.flatFallbacks.Load(), s.mptReads.Load(), s.otherReads.Load(),
		time.Duration(s.flatRead.Load()), time.Duration(s.mptLockWait.Load()),
		time.Duration(s.mptGetAccount.Load()), time.Duration(s.otherRead.Load())
}

func (s *prefetchReadStats) pathDBReadValues() (fallbacks int64, treeLockWait, treeLookup, diffLockWait, diffRead, diskLockWait, diskBufferRead, diskCacheRead, diskRead, diskCacheWrite, decode time.Duration) {
	return s.pathdbFallbacks.Load(),
		time.Duration(s.pathdbTreeLockWait.Load()), time.Duration(s.pathdbTreeLookup.Load()),
		time.Duration(s.pathdbDiffLockWait.Load()), time.Duration(s.pathdbDiffRead.Load()),
		time.Duration(s.pathdbDiskLockWait.Load()), time.Duration(s.pathdbDiskBufferRead.Load()), time.Duration(s.pathdbDiskCacheRead.Load()),
		time.Duration(s.pathdbDiskRead.Load()), time.Duration(s.pathdbDiskCacheWrite.Load()), time.Duration(s.pathdbDecode.Load())
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
		close(r.term)
		<-r.done
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
	var (
		total int
		start = time.Now()
	)
	for _, t := range r.tasks {
		total += t.weight()
	}
	var (
		wg   sync.WaitGroup
		unit = (total + r.nThreads - 1) / r.nThreads // round-up the per worker unit
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
	accountReads, accountHits, accountMisses, accountLateHits, accountErrors, accountElapsed, accountReadLockWait, accountStateRead, accountWriteLockWait := r.accountStats.values()
	accountFlatReads, accountFlatFallbacks, accountMPTReads, accountOtherReads, accountFlatRead, accountMPTLockWait, accountMPTGetAccount, accountOtherRead := r.accountStats.accountReadValues()
	accountPathDBFallbacks, accountPathDBTreeLockWait, accountPathDBTreeLookup, accountPathDBDiffLockWait, accountPathDBDiffRead, accountPathDBDiskLockWait, accountPathDBDiskBufferRead, accountPathDBDiskCacheRead, accountPathDBDiskRead, accountPathDBDiskCacheWrite, accountPathDBDecode := r.accountStats.pathDBReadValues()
	storageReads, storageHits, storageMisses, storageLateHits, storageErrors, storageElapsed, storageReadLockWait, storageStateRead, storageWriteLockWait := r.storageStats.values()
	storagePathDBFallbacks, storagePathDBTreeLockWait, storagePathDBTreeLookup, storagePathDBDiffLockWait, storagePathDBDiffRead, storagePathDBDiskLockWait, storagePathDBDiskBufferRead, storagePathDBDiskCacheRead, storagePathDBDiskRead, storagePathDBDiskCacheWrite, storagePathDBDecode := r.storageStats.pathDBReadValues()
	log.Debug("Prefetched accessList",
		"items", total,
		"elapsed", common.PrettyDuration(time.Since(start)),
		"accountReads", accountReads,
		"accountHits", accountHits,
		"accountMisses", accountMisses,
		"accountLateHits", accountLateHits,
		"accountErrors", accountErrors,
		"accountElapsed", common.PrettyDuration(accountElapsed),
		"accountReadLockWait", common.PrettyDuration(accountReadLockWait),
		"accountStateRead", common.PrettyDuration(accountStateRead),
		"accountWriteLockWait", common.PrettyDuration(accountWriteLockWait),
		"accountFlatReads", accountFlatReads,
		"accountFlatFallbacks", accountFlatFallbacks,
		"accountFlatRead", common.PrettyDuration(accountFlatRead),
		"accountMPTReads", accountMPTReads,
		"accountMPTLockWait", common.PrettyDuration(accountMPTLockWait),
		"accountMPTGetAccount", common.PrettyDuration(accountMPTGetAccount),
		"accountOtherReads", accountOtherReads,
		"accountOtherRead", common.PrettyDuration(accountOtherRead),
		"accountPathDBFallbacks", accountPathDBFallbacks,
		"accountPathDBTreeLockWait", common.PrettyDuration(accountPathDBTreeLockWait),
		"accountPathDBTreeLookup", common.PrettyDuration(accountPathDBTreeLookup),
		"accountPathDBDiffLockWait", common.PrettyDuration(accountPathDBDiffLockWait),
		"accountPathDBDiffRead", common.PrettyDuration(accountPathDBDiffRead),
		"accountPathDBDiskLockWait", common.PrettyDuration(accountPathDBDiskLockWait),
		"accountPathDBDiskBufferRead", common.PrettyDuration(accountPathDBDiskBufferRead),
		"accountPathDBDiskCacheRead", common.PrettyDuration(accountPathDBDiskCacheRead),
		"accountPathDBDiskRead", common.PrettyDuration(accountPathDBDiskRead),
		"accountPathDBDiskCacheWrite", common.PrettyDuration(accountPathDBDiskCacheWrite),
		"accountPathDBDecode", common.PrettyDuration(accountPathDBDecode),
		"storageReads", storageReads,
		"storageHits", storageHits,
		"storageMisses", storageMisses,
		"storageLateHits", storageLateHits,
		"storageErrors", storageErrors,
		"storageElapsed", common.PrettyDuration(storageElapsed),
		"storageReadLockWait", common.PrettyDuration(storageReadLockWait),
		"storageStateRead", common.PrettyDuration(storageStateRead),
		"storageWriteLockWait", common.PrettyDuration(storageWriteLockWait),
		"storagePathDBFallbacks", storagePathDBFallbacks,
		"storagePathDBTreeLockWait", common.PrettyDuration(storagePathDBTreeLockWait),
		"storagePathDBTreeLookup", common.PrettyDuration(storagePathDBTreeLookup),
		"storagePathDBDiffLockWait", common.PrettyDuration(storagePathDBDiffLockWait),
		"storagePathDBDiffRead", common.PrettyDuration(storagePathDBDiffRead),
		"storagePathDBDiskLockWait", common.PrettyDuration(storagePathDBDiskLockWait),
		"storagePathDBDiskBufferRead", common.PrettyDuration(storagePathDBDiskBufferRead),
		"storagePathDBDiskCacheRead", common.PrettyDuration(storagePathDBDiskCacheRead),
		"storagePathDBDiskRead", common.PrettyDuration(storagePathDBDiskRead),
		"storagePathDBDiskCacheWrite", common.PrettyDuration(storagePathDBDiskCacheWrite),
		"storagePathDBDecode", common.PrettyDuration(storagePathDBDecode),
	)
}

func (r *prefetchStateReader) account(addr common.Address) {
	start := time.Now()
	if reader, ok := r.StateReader.(*stateReaderWithCache); ok {
		_, err, stats := reader.accountWithStats(addr)
		r.accountStats.add(time.Since(start), stats, err)
		return
	}
	_, err := r.StateReader.Account(addr)
	r.accountStats.add(time.Since(start), stateReaderCallStats{}, err)
}

func (r *prefetchStateReader) storage(addr common.Address, slot common.Hash) {
	start := time.Now()
	if reader, ok := r.StateReader.(*stateReaderWithCache); ok {
		_, err, stats := reader.storageWithStats(addr, slot)
		r.storageStats.add(time.Since(start), stats, err)
		return
	}
	_, err := r.StateReader.Storage(addr, slot)
	r.storageStats.add(time.Since(start), stateReaderCallStats{}, err)
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
					return
				default:
					if j == 0 {
						r.account(t.addr)
					} else {
						r.storage(t.addr, t.slots[j-1])
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
		stop  = func() {}
	)
	if len(prefetch) > 0 && threads > 0 {
		pf := newPrefetchStateReader(cache, prefetch, threads)
		stop = pf.Close
	}
	return newReader(base, newStateReaderWithStats(cache)), stop
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

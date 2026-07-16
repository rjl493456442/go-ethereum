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
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/sync/errgroup"
)

// ApplyBlockAccessList installs the post-state recorded in a block access list
// directly into the state, without executing any transactions.
func (s *StateDB) ApplyBlockAccessList(list *bal.BlockAccessList) error {
	return s.applyBlockAccessList(list, runtime.GOMAXPROCS(0))
}

type balSlot struct {
	key   common.Hash
	value common.Hash
}

type balAccount struct {
	access *bal.AccountAccess
	slots  []balSlot
	obj    *stateObject // staged mutation, nil if the account ends up untouched
}

// hasMetadataChange reports whether the account has a metadata mutation.
func (a *balAccount) hasMetadataChange() bool {
	return len(a.access.BalanceChanges) != 0 || len(a.access.NonceChanges) != 0 || len(a.access.CodeChanges) != 0
}

// mutated reports whether the block may change the account's trie leaf. It is a
// cheap upper bound, derived purely from the access list, used to skip read-only
// accounts before anything is loaded; whether a storage write truly changes a
// slot is only known once its pre-state value is read.
func (a *balAccount) mutated() bool {
	return a.hasMetadataChange() || len(a.slots) != 0
}

// balReadStats accumulates database read latencies across the concurrent workers.
type balReadStats struct {
	accountReads atomic.Int64
	storageReads atomic.Int64
}

func (s *StateDB) applyBlockAccessList(list *bal.BlockAccessList, threads int) error {
	var (
		accounts  = make([]*balAccount, 0, len(*list))
		addresses = make([]common.Address, 0, len(*list))
	)
	for i := range *list {
		access := &(*list)[i]
		entry := &balAccount{access: access}
		for j := range access.StorageChanges {
			change := &access.StorageChanges[j]
			if n := len(change.SlotChanges); n > 0 {
				entry.slots = append(entry.slots, balSlot{
					key:   change.Slot.Bytes32(),
					value: change.SlotChanges[n-1].PostValue.Bytes32(),
				})
			}
		}
		// Skip the read-only account
		if !entry.mutated() {
			continue
		}
		accounts = append(accounts, entry)
		addresses = append(addresses, access.Address)
	}
	// Warm the account trie and process the accounts concurrently. Prefetching the
	// account-trie leaves and, per account, the storage-trie nodes pulls the reads
	// needed for hashing off the critical path, overlapping them with each other
	// and with the concurrent transaction execution.
	var (
		stats balReadStats
		group errgroup.Group
	)
	group.Go(func() error {
		return s.prefetchAccountTrie(addresses)
	})
	group.Go(func() error {
		return parallelBALApply(len(accounts), threads, func(i int) error {
			return s.prepareBALAccount(accounts[i], &stats)
		})
	})
	if err := group.Wait(); err != nil {
		return err
	}
	var storageLoaded int
	for _, entry := range accounts {
		storageLoaded += len(entry.slots)
		obj := entry.obj
		if obj == nil {
			continue
		}
		if obj.empty() {
			s.markDelete(obj.address)
			s.stateObjectsDestruct[obj.address] = obj
		} else {
			s.markUpdate(obj.address)
			s.setStateObject(obj)
		}
	}
	s.AccountLoaded += len(addresses)
	s.AccountReads += time.Duration(stats.accountReads.Load())
	s.StorageLoaded += storageLoaded
	s.StorageReads += time.Duration(stats.storageReads.Load())
	return nil
}

// prefetchAccountTrie opens the account trie and warms the nodes on the path to
// every given address.
func (s *StateDB) prefetchAccountTrie(addrs []common.Address) error {
	if len(addrs) == 0 {
		return nil
	}
	if s.trie == nil {
		tr, err := s.db.OpenTrie(s.originalRoot)
		if err != nil {
			return err
		}
		s.trie = tr
	}
	return s.trie.PrefetchAccount(addrs)
}

// prepareBALAccount loads one account's pre-state, warms the trie nodes required
// to hash its mutations, and builds the resulting state object. It only touches
// the account's own data, so it is safe to run concurrently for different
// accounts; the object is installed into the shared state by the caller.
func (s *StateDB) prepareBALAccount(entry *balAccount, stats *balReadStats) error {
	// Resolve the account object from the database.
	var (
		addr  = entry.access.Address
		start = time.Now()
	)
	account, err := s.reader.Account(addr)
	stats.accountReads.Add(int64(time.Since(start)))
	if err != nil {
		return fmt.Errorf("load account %x: %w", addr, err)
	}
	obj := newObject(s, addr, account)

	// Apply the final value of each mutated field.
	if n := len(entry.access.BalanceChanges); n > 0 {
		obj.setBalance(entry.access.BalanceChanges[n-1].PostBalance.Clone())
	}
	if n := len(entry.access.NonceChanges); n > 0 {
		obj.setNonce(entry.access.NonceChanges[n-1].PostNonce)
	}
	if n := len(entry.access.CodeChanges); n > 0 {
		code := entry.access.CodeChanges[n-1].NewCode
		obj.setCode(crypto.Keccak256Hash(code), slices.Clone(code))
	}
	if err := s.applyBALStorage(obj, entry.slots, stats); err != nil {
		return err
	}
	// Drop accounts whose writes all reverted to their original value.
	if !entry.hasMetadataChange() && len(obj.pendingStorage) == 0 {
		return nil
	}
	entry.obj = obj
	return nil
}

// applyBALStorage warms the account's storage trie and stages the writes that
// actually change a slot's value.
func (s *StateDB) applyBALStorage(obj *stateObject, slots []balSlot, stats *balReadStats) error {
	if len(slots) == 0 {
		return nil
	}
	addr := obj.address

	if obj.data.Root != types.EmptyRootHash {
		keys := make([][]byte, len(slots))
		for i := range slots {
			keys[i] = slots[i].key.Bytes()
		}
		tr, err := s.db.OpenStorageTrie(s.originalRoot, addr, obj.data.Root, nil)
		if err != nil {
			return err
		}
		if err := tr.PrefetchStorage(addr, keys); err != nil {
			return err
		}
		obj.trie = tr
	}
	// Stage the writes that differ from the slot's pre-state value.
	for i := range slots {
		start := time.Now()
		origin, err := s.reader.Storage(addr, slots[i].key)
		stats.storageReads.Add(int64(time.Since(start)))
		if err != nil {
			return fmt.Errorf("load storage %x/%x: %w", addr, slots[i].key, err)
		}
		if slots[i].value == origin {
			continue // slot ended the block at its original value
		}
		obj.pendingStorage[slots[i].key] = slots[i].value
		obj.uncommittedStorage[slots[i].key] = origin
	}
	return nil
}

// parallelBALApply invokes apply for every index in [0, tasks) across at most
// workers goroutines, returning the first error reported by any of them.
func parallelBALApply(tasks, workers int, apply func(int) error) error {
	if tasks == 0 {
		return nil
	}
	workers = min(max(workers, 1), tasks)

	var (
		next  atomic.Uint64
		group errgroup.Group
	)
	for range workers {
		group.Go(func() error {
			for {
				i := int(next.Add(1)) - 1
				if i >= tasks {
					return nil
				}
				if err := apply(i); err != nil {
					return err
				}
			}
		})
	}
	return group.Wait()
}

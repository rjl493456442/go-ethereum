// Copyright 2025 The go-ethereum Authors
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
	"bytes"
	"errors"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// dirtyIteratee wraps an existing Iteratee and overlays the in-memory mutations
// from a StateDB on top. This makes uncommitted state changes (from preceding
// transactions in the same block, or the current transaction) visible during
// iteration.
type dirtyIteratee struct {
	inner      Iteratee
	db         *StateDB
	destructed map[common.Hash]struct{} // set of destructed account address hashes
}

// newDirtyIteratee creates a dirty iteratee that merges committed state from
// the inner iteratee with in-memory mutations from the StateDB.
func newDirtyIteratee(inner Iteratee, db *StateDB) *dirtyIteratee {
	// Collect destructed accounts for quick lookup. These are accounts whose
	// committed storage should be completely hidden.
	destructed := make(map[common.Hash]struct{})
	for addr := range db.stateObjectsDestruct {
		destructed[crypto.Keccak256Hash(addr.Bytes())] = struct{}{}
	}
	return &dirtyIteratee{
		inner:      inner,
		db:         db,
		destructed: destructed,
	}
}

// NewAccountIterator creates an account iterator that merges committed accounts
// with in-memory dirty accounts. The iteration order is by account address hash.
func (d *dirtyIteratee) NewAccountIterator(start common.Hash) (AccountIterator, error) {
	innerIt, err := d.inner.NewAccountIterator(start)
	if err != nil {
		return nil, err
	}
	// Collect dirty account entries from stateObjects (live modified accounts).
	var entries []dirtyAccountEntry
	deleted := make(map[common.Hash]struct{})

	for addr, obj := range d.db.stateObjects {
		addrHash := crypto.Keccak256Hash(addr.Bytes())
		if bytes.Compare(addrHash.Bytes(), start.Bytes()) < 0 {
			continue
		}
		data, err := rlp.EncodeToBytes(&obj.data)
		if err != nil {
			innerIt.Release()
			return nil, err
		}
		entries = append(entries, dirtyAccountEntry{
			hash:    addrHash,
			address: addr,
			data:    data,
		})
	}
	// Mark destructed accounts (that are not resurrected) as deleted.
	for addr := range d.db.stateObjectsDestruct {
		addrHash := crypto.Keccak256Hash(addr.Bytes())
		if bytes.Compare(addrHash.Bytes(), start.Bytes()) < 0 {
			continue
		}
		if _, resurrected := d.db.stateObjects[addr]; resurrected {
			continue // resurrected account is already in entries above
		}
		deleted[addrHash] = struct{}{}
	}
	// Sort dirty entries by hash for ordered merge.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].hash.Bytes(), entries[j].hash.Bytes()) < 0
	})
	return &dirtyAccountIterator{
		inner:   innerIt,
		dirty:   entries,
		deleted: deleted,
		idx:     0,
		started: false,
	}, nil
}

// NewStorageIterator creates a storage iterator that merges committed storage
// with in-memory dirty storage for the given account. The iteration order is
// by storage key hash.
func (d *dirtyIteratee) NewStorageIterator(addressHash common.Hash, start common.Hash) (StorageIterator, error) {
	// Check if the account was destructed. If so, all committed storage is
	// effectively wiped and only dirty storage (from resurrection) applies.
	_, destructed := d.destructed[addressHash]

	// Find the stateObject matching this addressHash to collect dirty storage.
	var dirtyEntries []dirtyStorageEntry
	for addr, obj := range d.db.stateObjects {
		if crypto.Keccak256Hash(addr.Bytes()) != addressHash {
			continue
		}
		// Merge pendingStorage and dirtyStorage. dirtyStorage (current tx)
		// takes precedence over pendingStorage (prior txs in block).
		merged := make(map[common.Hash]common.Hash)
		for key, value := range obj.pendingStorage {
			merged[key] = value
		}
		for key, value := range obj.dirtyStorage {
			merged[key] = value
		}
		for key, value := range merged {
			keyHash := crypto.Keccak256Hash(key.Bytes())
			if bytes.Compare(keyHash.Bytes(), start.Bytes()) < 0 {
				continue
			}
			dirtyEntries = append(dirtyEntries, dirtyStorageEntry{
				hash:    keyHash,
				key:     key,
				value:   value,
				deleted: value == (common.Hash{}),
			})
		}
		break
	}
	// Sort dirty entries by hash for ordered merge.
	sort.Slice(dirtyEntries, func(i, j int) bool {
		return bytes.Compare(dirtyEntries[i].hash.Bytes(), dirtyEntries[j].hash.Bytes()) < 0
	})
	// If the account was destructed, don't iterate committed storage at all.
	// Only return dirty storage from the (possibly resurrected) account.
	if destructed {
		return &dirtyOnlyStorageIterator{
			dirty:   dirtyEntries,
			idx:     -1,
			started: false,
		}, nil
	}
	innerIt, err := d.inner.NewStorageIterator(addressHash, start)
	if err != nil {
		return nil, err
	}
	return &dirtyStorageIterator{
		inner:   innerIt,
		dirty:   dirtyEntries,
		idx:     0,
		started: false,
	}, nil
}

// dirtyAccountEntry represents a dirty account with precomputed hash.
type dirtyAccountEntry struct {
	hash    common.Hash
	address common.Address
	data    []byte // RLP-encoded full StateAccount
}

// dirtyStorageEntry represents a dirty storage slot with precomputed hash.
type dirtyStorageEntry struct {
	hash    common.Hash
	key     common.Hash    // raw storage key (unhashed)
	value   common.Hash    // storage value
	deleted bool           // true if value is zero (slot deleted)
}

// dirtyAccountIterator merges a committed account iterator with sorted dirty
// account entries. It yields accounts in hash order, with dirty entries
// overriding committed ones.
type dirtyAccountIterator struct {
	inner   AccountIterator
	dirty   []dirtyAccountEntry
	deleted map[common.Hash]struct{} // destructed accounts (not resurrected)

	// Merge state
	idx     int  // next unprocessed dirty entry index
	started bool // whether Next() has been called

	// Current element
	source    byte // 0=inner, 1=dirty
	dirtyIdx  int  // index of current dirty entry (when source=1)
	innerNext bool // whether inner has a valid current element

	err error
}

// Next advances the iterator to the next account in hash order.
func (it *dirtyAccountIterator) Next() bool {
	if it.err != nil {
		return false
	}
	// On first call, prime the inner iterator.
	if !it.started {
		it.started = true
		it.innerNext = it.inner.Next()
	} else if it.source == 0 {
		// Last yielded from inner, advance inner.
		it.innerNext = it.inner.Next()
	}
	// Find the next valid entry from the merge of inner and dirty.
	for {
		hasInner := it.innerNext && it.inner.Error() == nil
		hasDirty := it.idx < len(it.dirty)

		if !hasInner && !hasDirty {
			return false
		}
		if hasInner && !hasDirty {
			// Only inner remains. Check if it's deleted.
			if _, del := it.deleted[it.inner.Hash()]; del {
				it.innerNext = it.inner.Next()
				continue
			}
			it.source = 0
			return true
		}
		if !hasInner && hasDirty {
			// Only dirty remains.
			it.source = 1
			it.dirtyIdx = it.idx
			it.idx++
			return true
		}
		// Both have entries. Compare by hash.
		innerHash := it.inner.Hash()
		dirtyHash := it.dirty[it.idx].hash
		cmp := bytes.Compare(innerHash.Bytes(), dirtyHash.Bytes())

		if cmp < 0 {
			// Inner comes first. Check if deleted.
			if _, del := it.deleted[innerHash]; del {
				it.innerNext = it.inner.Next()
				continue
			}
			it.source = 0
			return true
		}
		if cmp > 0 {
			// Dirty comes first (new account not in committed state).
			it.source = 1
			it.dirtyIdx = it.idx
			it.idx++
			return true
		}
		// Same hash: dirty overrides committed. Advance inner past this entry.
		it.innerNext = it.inner.Next()
		it.source = 1
		it.dirtyIdx = it.idx
		it.idx++
		return true
	}
}

// Error returns any failure that occurred during iteration.
func (it *dirtyAccountIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Error()
}

// Hash returns the hash of the current account.
func (it *dirtyAccountIterator) Hash() common.Hash {
	if it.source == 1 {
		return it.dirty[it.dirtyIdx].hash
	}
	return it.inner.Hash()
}

// Address returns the address of the current account.
func (it *dirtyAccountIterator) Address() (common.Address, error) {
	if it.source == 1 {
		return it.dirty[it.dirtyIdx].address, nil
	}
	return it.inner.Address()
}

// Account returns the RLP-encoded account data of the current account.
func (it *dirtyAccountIterator) Account() []byte {
	if it.source == 1 {
		return it.dirty[it.dirtyIdx].data
	}
	return it.inner.Account()
}

// Release releases associated resources.
func (it *dirtyAccountIterator) Release() {
	it.inner.Release()
}

// dirtyStorageIterator merges a committed storage iterator with sorted dirty
// storage entries. It yields storage slots in hash order.
type dirtyStorageIterator struct {
	inner StorageIterator
	dirty []dirtyStorageEntry

	// Merge state
	idx     int
	started bool

	// Current element
	source    byte // 0=inner, 1=dirty
	dirtyIdx  int
	innerNext bool

	err error
}

// Next advances the iterator to the next storage slot in hash order.
func (it *dirtyStorageIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		it.innerNext = it.inner.Next()
	} else if it.source == 0 {
		it.innerNext = it.inner.Next()
	}
	for {
		hasInner := it.innerNext && it.inner.Error() == nil
		hasDirty := it.idx < len(it.dirty)

		if !hasInner && !hasDirty {
			return false
		}
		if hasInner && !hasDirty {
			it.source = 0
			return true
		}
		if !hasInner && hasDirty {
			// Skip deleted dirty entries.
			if it.dirty[it.idx].deleted {
				it.idx++
				continue
			}
			it.source = 1
			it.dirtyIdx = it.idx
			it.idx++
			return true
		}
		innerHash := it.inner.Hash()
		dirtyHash := it.dirty[it.idx].hash
		cmp := bytes.Compare(innerHash.Bytes(), dirtyHash.Bytes())

		if cmp < 0 {
			it.source = 0
			return true
		}
		if cmp > 0 {
			if it.dirty[it.idx].deleted {
				it.idx++
				continue
			}
			it.source = 1
			it.dirtyIdx = it.idx
			it.idx++
			return true
		}
		// Same hash: dirty overrides. Advance inner.
		it.innerNext = it.inner.Next()
		if it.dirty[it.idx].deleted {
			// Slot was deleted in dirty state, skip it entirely.
			it.idx++
			continue
		}
		it.source = 1
		it.dirtyIdx = it.idx
		it.idx++
		return true
	}
}

// Error returns any failure that occurred during iteration.
func (it *dirtyStorageIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Error()
}

// Hash returns the hash of the current storage slot.
func (it *dirtyStorageIterator) Hash() common.Hash {
	if it.source == 1 {
		return it.dirty[it.dirtyIdx].hash
	}
	return it.inner.Hash()
}

// Key returns the raw storage key of the current slot.
func (it *dirtyStorageIterator) Key() (common.Hash, error) {
	if it.source == 1 {
		return it.dirty[it.dirtyIdx].key, nil
	}
	return it.inner.Key()
}

// Slot returns the RLP-encoded storage slot value.
func (it *dirtyStorageIterator) Slot() []byte {
	if it.source == 1 {
		value := it.dirty[it.dirtyIdx].value
		encoded, err := rlp.EncodeToBytes(common.TrimLeftZeroes(value[:]))
		if err != nil {
			it.err = err
			return nil
		}
		return encoded
	}
	return it.inner.Slot()
}

// Release releases associated resources.
func (it *dirtyStorageIterator) Release() {
	it.inner.Release()
}

// dirtyOnlyStorageIterator iterates only over dirty storage entries. This is
// used for destructed accounts where committed storage is completely wiped.
type dirtyOnlyStorageIterator struct {
	dirty   []dirtyStorageEntry
	idx     int
	started bool
	err     error
}

// Next advances to the next non-deleted dirty storage entry.
func (it *dirtyOnlyStorageIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		it.idx = 0
	} else {
		it.idx++
	}
	// Skip deleted entries.
	for it.idx < len(it.dirty) && it.dirty[it.idx].deleted {
		it.idx++
	}
	return it.idx < len(it.dirty)
}

// Error returns any failure that occurred during iteration.
func (it *dirtyOnlyStorageIterator) Error() error {
	return it.err
}

// Hash returns the hash of the current storage slot.
func (it *dirtyOnlyStorageIterator) Hash() common.Hash {
	if it.idx < len(it.dirty) {
		return it.dirty[it.idx].hash
	}
	return common.Hash{}
}

// Key returns the raw storage key of the current slot.
func (it *dirtyOnlyStorageIterator) Key() (common.Hash, error) {
	if it.idx < len(it.dirty) {
		return it.dirty[it.idx].key, nil
	}
	return common.Hash{}, errors.New("iterator exhausted")
}

// Slot returns the RLP-encoded storage slot value.
func (it *dirtyOnlyStorageIterator) Slot() []byte {
	if it.idx >= len(it.dirty) {
		return nil
	}
	value := it.dirty[it.idx].value
	encoded, err := rlp.EncodeToBytes(common.TrimLeftZeroes(value[:]))
	if err != nil {
		it.err = err
		return nil
	}
	return encoded
}

// Release is a no-op for dirty-only iterators.
func (it *dirtyOnlyStorageIterator) Release() {}

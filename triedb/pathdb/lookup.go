// Copyright 2024 The go-ethereum Authors
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

package pathdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

// storageKey returns a key for uniquely identifying the storage slot.
func storageKey(accountHash common.Hash, slotHash common.Hash) [64]byte {
	var key [64]byte
	copy(key[:32], accountHash[:])
	copy(key[32:], slotHash[:])
	return key
}

// storageKeySlice returns a key for uniquely identifying the storage slot in
// the slice format.
func storageKeySlice(accountHash common.Hash, slotHash common.Hash) []byte {
	key := storageKey(accountHash, slotHash)
	return key[:]
}

// lookupShards is the number of the shards the lookup is split into by the
// account hash, linked and unlinked concurrently.
const lookupShards = 4

// lookupShard returns the lookup shard of the given account and its storage.
func lookupShard(accountHash common.Hash) int {
	return int(accountHash[0]) % lookupShards
}

// lookup is an internal structure used to efficiently determine the layer in
// which a state entry resides.
type lookup struct {
	// accounts represents the mutation history for specific accounts.
	// The key is the account address hash, and the value is a slice
	// of **diff layer** IDs indicating where the account was modified,
	// with the order from oldest to newest. It's split into shards by
	// the account hash.
	accounts [lookupShards]map[common.Hash][]common.Hash

	// storages represents the mutation history for specific storage
	// slot. The key is the account address hash and the storage key
	// hash, the value is a slice of **diff layer** IDs indicating
	// where the slot was modified, with the order from oldest to newest.
	// It's split into shards by the account hash.
	storages [lookupShards]map[[64]byte][]common.Hash

	// descendant is the callback indicating whether the layer with
	// given root is a descendant of the one specified by `ancestor`.
	descendant func(state common.Hash, ancestor common.Hash) bool
}

// newLookup initializes the lookup structure.
func newLookup(head layer, descendant func(state common.Hash, ancestor common.Hash) bool) *lookup {
	var (
		current = head
		layers  []layer
	)
	for current != nil {
		layers = append(layers, current)
		current = current.parentLayer()
	}
	l := &lookup{descendant: descendant}
	for i := range lookupShards {
		l.accounts[i] = make(map[common.Hash][]common.Hash)
		l.storages[i] = make(map[[64]byte][]common.Hash)
	}
	// Apply the diff layers from bottom to top
	for i := len(layers) - 1; i >= 0; i-- {
		switch diff := layers[i].(type) {
		case *diskLayer:
			continue
		case *diffLayer:
			l.addLayer(diff)
		}
	}
	return l
}

// accountTip traverses the layer list associated with the given account in
// reverse order to locate the first entry that either matches the specified
// stateID or is a descendant of it.
//
// If found, the account data corresponding to the supplied stateID resides
// in the layer identified by the returned hash (ok=true). Otherwise,
// (common.Hash{}, false) is returned to signal that the supplied stateID is
// stale.
//
// Note the returned hash may itself be common.Hash{} when the disk layer's
// root is zero — as is the case for a fresh verkle/bintrie database whose
// empty trie hashes to EmptyVerkleHash. Callers must therefore consult the
// boolean rather than comparing the returned hash against common.Hash{}
// directly.
func (l *lookup) accountTip(accountHash common.Hash, stateID common.Hash, base common.Hash) (common.Hash, bool) {
	// Traverse the mutation history from latest to oldest one. Several
	// scenarios are possible:
	//
	// Chain:
	//     D->C1->C2->C3->C4 (HEAD)
	//      ->C1'->C2'->C3'
	// State:
	//     x: [C1, C1', C3', C3]
	//     y: []
	//
	// - (x, C4) => C3
	// - (x, C3) => C3
	// - (x, C2) => C1
	// - (x, C3') => C3'
	// - (x, C2') => C1'
	// - (y, C4) => D
	// - (y, C3') => D
	// - (y, C0) => null
	list := l.accounts[lookupShard(accountHash)][accountHash]
	for i := len(list) - 1; i >= 0; i-- {
		// If the current state matches the stateID, or the requested state is a
		// descendant of it, return the current state as the most recent one
		// containing the modified data. Otherwise, the current state may be ahead
		// of the requested one or belong to a different branch.
		if list[i] == stateID || l.descendant(stateID, list[i]) {
			return list[i], true
		}
	}
	// No layer matching the stateID or its descendants was found. Use the
	// current disk layer as a fallback.
	if base == stateID || l.descendant(stateID, base) {
		return base, true
	}
	// The layer associated with 'stateID' is not the descendant of the current
	// disk layer, it's already stale, return nothing.
	return common.Hash{}, false
}

// storageTip traverses the layer list associated with the given account and
// slot hash in reverse order to locate the first entry that either matches
// the specified stateID or is a descendant of it.
//
// See accountTip for the returned-hash / ok convention — the same
// bintrie-zero-root caveat applies here.
func (l *lookup) storageTip(accountHash common.Hash, slotHash common.Hash, stateID common.Hash, base common.Hash) (common.Hash, bool) {
	list := l.storages[lookupShard(accountHash)][storageKey(accountHash, slotHash)]
	for i := len(list) - 1; i >= 0; i-- {
		// If the current state matches the stateID, or the requested state is a
		// descendant of it, return the current state as the most recent one
		// containing the modified data. Otherwise, the current state may be ahead
		// of the requested one or belong to a different branch.
		if list[i] == stateID || l.descendant(stateID, list[i]) {
			return list[i], true
		}
	}
	// No layer matching the stateID or its descendants was found. Use the
	// current disk layer as a fallback.
	if base == stateID || l.descendant(stateID, base) {
		return base, true
	}
	// The layer associated with 'stateID' is not the descendant of the current
	// disk layer, it's already stale, return nothing.
	return common.Hash{}, false
}

// addLayer traverses the state data retained in the specified diff layer and
// integrates it into the lookup set.
//
// This function assumes that all layers older than the provided one have already
// been processed, ensuring that layers are processed strictly in a bottom-to-top
// order.
func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		wg    sync.WaitGroup
		state = diff.rootHash()
	)
	for shard := range lookupShards {
		wg.Add(1)
		go func() {
			defer wg.Done()

			accounts, storages := l.accounts[shard], l.storages[shard]
			for accountHash := range diff.states.accountData {
				if lookupShard(accountHash) != shard {
					continue
				}
				accounts[accountHash] = append(accounts[accountHash], state)
			}
			for accountHash, slots := range diff.states.storageData {
				if lookupShard(accountHash) != shard {
					continue
				}
				for slotHash := range slots {
					key := storageKey(accountHash, slotHash)
					storages[key] = append(storages[key], state)
				}
			}
		}()
	}
	wg.Wait()
}

// removeFromList removes the specified element from the provided list.
// It returns a flag indicating whether the element was found and removed.
func removeFromList(list []common.Hash, element common.Hash) (bool, []common.Hash) {
	// Traverse the list from oldest to newest to quickly locate the element.
	for i := 0; i < len(list); i++ {
		if list[i] == element {
			if i != 0 {
				list = append(list[:i], list[i+1:]...)
			} else {
				// Remove the first element by shifting the slice forward.
				// Pros: zero-copy.
				// Cons: may retain large backing array, causing memory leaks.
				// Mitigation: release the array if capacity exceeds threshold.
				list = list[1:]
				if cap(list) > 1024 {
					list = append(make([]common.Hash, 0, len(list)), list...)
				}
			}
			return true, list
		}
	}
	return false, nil
}

// removeLayer traverses the state data retained in the specified diff layer and
// unlink them from the lookup set.
func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		eg    errgroup.Group
		state = diff.rootHash()
	)
	for shard := range lookupShards {
		eg.Go(func() error {
			accounts, storages := l.accounts[shard], l.storages[shard]
			for accountHash := range diff.states.accountData {
				if lookupShard(accountHash) != shard {
					continue
				}
				found, list := removeFromList(accounts[accountHash], state)
				if !found {
					return fmt.Errorf("account lookup is not found, %x, state: %x", accountHash, state)
				}
				if len(list) != 0 {
					accounts[accountHash] = list
				} else {
					delete(accounts, accountHash)
				}
			}
			for accountHash, slots := range diff.states.storageData {
				if lookupShard(accountHash) != shard {
					continue
				}
				for slotHash := range slots {
					key := storageKey(accountHash, slotHash)
					found, list := removeFromList(storages[key], state)
					if !found {
						return fmt.Errorf("storage lookup is not found, %x %x, state: %x", accountHash, slotHash, state)
					}
					if len(list) != 0 {
						storages[key] = list
					} else {
						delete(storages, key)
					}
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// Copyright 2023 The go-ethereum Authors
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
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
)

// snapReader is a wrapper over the state snapshot and implements the Reader
// interface. It provides an efficient way to access state.
type snapReader struct {
	snap   snapshot.Snapshot
	hasher crypto.KeccakState
}

// newSnapReader constructs a snap reader based on the specific state root.
func newSnapReader(snaps *snapshot.Tree, root common.Hash) (*snapReader, error) {
	snap := snaps.Snapshot(root)
	if snap == nil {
		return nil, errors.New("snapshot is not available")
	}
	return &snapReader{
		snap:   snap,
		hasher: crypto.NewKeccakState(),
	}, nil
}

// Account implements Reader, retrieving the account specified by the address
// from the associated state.
//
// An error will be returned if the associated snapshot is already stale or
// the snapshot is not fully constructed yet.
//
// The returned account might be nil if it's not existent in the state.
func (r *snapReader) Account(addr common.Address) (*types.StateAccount, error) {
	ret, err := r.snap.Account(crypto.HashData(r.hasher, addr.Bytes()))
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, nil
	}
	acct := &types.StateAccount{
		Nonce:    ret.Nonce,
		Balance:  ret.Balance,
		CodeHash: ret.CodeHash,
		Root:     common.BytesToHash(ret.Root),
	}
	if len(acct.CodeHash) == 0 {
		acct.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if acct.Root == (common.Hash{}) {
		acct.Root = types.EmptyRootHash
	}
	return acct, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key from the associated state.
//
// An error will be returned if the associated snapshot is already stale or
// the snapshot is not fully constructed yet.
//
// The returned storage slot might be empty if it's not existent in the state.
func (r *snapReader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	addrHash := crypto.HashData(r.hasher, addr.Bytes())
	slotHash := crypto.HashData(r.hasher, key.Bytes())
	ret, err := r.snap.Storage(addrHash, slotHash)
	if err != nil {
		return common.Hash{}, err
	}
	if len(ret) == 0 {
		return common.Hash{}, nil
	}
	_, content, _, err := rlp.Split(ret)
	if err != nil {
		return common.Hash{}, err
	}
	var slot common.Hash
	slot.SetBytes(content)
	return slot, nil
}

// trieReader implements the Reader interface, providing functions to access
// state from the referenced trie.
type trieReader struct {
	root     common.Hash             // State root which uniquely represent a state.
	db       *trie.Database          // Database for loading trie
	hasher   crypto.KeccakState      // Hasher for keccak256 hashing.
	mainTrie Trie                    // Main trie, resolved in constructor
	subTries map[common.Address]Trie // group of storage tries, cached when it's resolved.
}

// trieReader constructs a trie reader of the specific state. An error will be returned
// if the associated trie specified by root is not existent.
func newTrieReader(root common.Hash, db *trie.Database) (*trieReader, error) {
	var (
		tr  Trie
		err error
	)
	if !db.IsVerkle() {
		tr, err = trie.NewStateTrie(trie.StateTrieID(root), db)
	} else {
		tr, err = trie.NewVerkleTrie(root, db, utils.NewPointCache(commitmentCacheItems))
	}
	if err != nil {
		return nil, err
	}
	return &trieReader{
		root:     root,
		db:       db,
		hasher:   crypto.NewKeccakState(),
		mainTrie: tr,
		subTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements Reader, retrieving the account specified by the address
// from the associated state.
//
// An error will be returned if the trie state is corrupted. An nil account
// will be returned if it's not existent in the trie.
func (r *trieReader) Account(addr common.Address) (*types.StateAccount, error) {
	return r.mainTrie.GetAccount(addr)
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key from the associated state.
//
// An error will be returned if the trie state is corrupted. An empty storage
// slot will be returned if it's not existent in the trie.
func (r *trieReader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	var (
		tr    Trie
		found bool
		slot  common.Hash
	)
	if r.db.IsVerkle() {
		tr = r.mainTrie
	} else {
		tr, found = r.subTries[addr]
		if !found {
			var err error
			tr, err = trie.NewStateTrie(trie.StorageTrieID(r.root, crypto.HashData(r.hasher, addr.Bytes()), root), r.db)
			if err != nil {
				return common.Hash{}, err
			}
			r.subTries[addr] = tr
		}
	}
	ret, err := tr.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	slot.SetBytes(ret)
	return slot, nil
}

// The reader implements the Reader interface by combining the state snapshot
// along with the trie database, offering methods to efficiently access states
// with the snapshot first and falling back to trie reader if it fails.
type reader struct {
	snap *snapReader // fast reader, might not available
	trie *trieReader // slow reader, always available
}

// newReader constructs a reader of the specific state.
func newReader(root common.Hash, db *trie.Database, snaps *snapshot.Tree) (*reader, error) {
	var snap *snapReader // optional snapshot reader
	if snaps != nil {
		snap, _ = newSnapReader(snaps, root)
	}
	trie, err := newTrieReader(root, db) // mandatory trie reader
	if err != nil {
		return nil, err
	}
	return &reader{
		snap: snap,
		trie: trie,
	}, nil
}

// Account implements Reader, retrieving the account specified by the address
// from the associated state. Nil account is returned if the requested one is
// not existent in the state. Error is only returned if any unexpected error occurs.
func (r *reader) Account(addr common.Address) (*types.StateAccount, error) {
	if r.snap != nil {
		acct, err := r.snap.Account(addr)
		if err == nil {
			return acct, nil
		}
	}
	acct, err := r.trie.Account(addr)
	if err != nil {
		return nil, err
	}
	return acct, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key from the associated state. Null slot is returned if
// the requested one is not existent in the state. Error is only returned if
// any unexpected error occurs.
func (r *reader) Storage(addr common.Address, root common.Hash, key common.Hash) (common.Hash, error) {
	if r.snap != nil {
		slot, err := r.snap.Storage(addr, root, key)
		if err == nil {
			return slot, nil
		}
	}
	slot, err := r.trie.Storage(addr, root, key)
	if err != nil {
		return common.Hash{}, err
	}
	return slot, nil
}

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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/overlay"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/ethereum/go-ethereum/trie/transitiontrie"
	"github.com/ethereum/go-ethereum/triedb"
)

func openBinaryTree(root common.Hash, db *triedb.Database, firstBinary bool) (Trie, error) {
	binTrie, binErr := bintrie.NewBinaryTrie(root, db)
	if binErr != nil {
		return nil, binErr
	}
	// Based on the transition status, determine if the overlay
	// tree needs to be created, or if a single, target tree is
	// to be picked.
	var (
		err error
		tr  Trie
	)
	ts := overlay.LoadTransitionState(db.Disk(), root, true)
	if ts.InTransition() {
		mpt, err := trie.NewStateTrie(trie.StateTrieID(ts.BaseRoot), db)
		if err != nil {
			return nil, err
		}
		tr = transitiontrie.NewTransitionTrie(mpt, binTrie, false)
	} else {
		// HACK: Use TransitionTrie with nil base as a wrapper to make BinaryTrie
		// satisfy the Trie interface. This works around the import cycle between
		// trie and trie/bintrie packages.
		//
		// TODO: In future PRs, refactor the package structure to avoid this hack:
		// - Option 1: Move common interfaces (Trie, NodeIterator) to a separate
		//   package that both trie and trie/bintrie can import
		// - Option 2: Create a factory function in the trie package that returns
		//   BinaryTrie as a Trie interface without direct import
		// - Option 3: Move BinaryTrie to the main trie package
		//
		// The current approach works but adds unnecessary overhead and complexity
		// by using TransitionTrie when there's no actual transition happening.
		tr = transitiontrie.NewTransitionTrie(nil, binTrie, false)
	}
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// BinaryDB is an implementation of Database interface. It leverages both trie and
// state snapshot to provide functionalities for state access in binary manner.
type BinaryDB struct {
	triedb *triedb.Database
	codedb *CodeDB

	// TODO(gballet)
	// Instrument chain context
	firstBinary bool
}

// NewBinaryDB creates a state database with the provided data sources.
func NewBinaryDB(triedb *triedb.Database, codedb *CodeDB, firstBinary bool) *BinaryDB {
	if codedb == nil {
		codedb = NewCodeDB(triedb.Disk())
	}
	return &BinaryDB{
		triedb:      triedb,
		codedb:      codedb,
		firstBinary: firstBinary,
	}
}

// StateReader returns a state reader associated with the specified state root.
func (db *BinaryDB) StateReader(stateRoot common.Hash) (StateReader, error) {
	var readers []StateReader

	reader, err := db.triedb.StateReader(stateRoot)
	if err == nil {
		readers = append(readers, newFlatReader(reader))
	}
	// Configure the trie reader, which is expected to be available as the
	// gatekeeper unless the state is corrupted.
	tr, err := newBinaryTreeReader(stateRoot, db.triedb, db.firstBinary)
	if err != nil {
		return nil, err
	}
	readers = append(readers, tr)

	return newMultiStateReader(readers...)
}

// Reader implements Database, returning a reader associated with the specified
// state root.
func (db *BinaryDB) Reader(stateRoot common.Hash) (Reader, error) {
	sr, err := db.StateReader(stateRoot)
	if err != nil {
		return nil, err
	}
	return newReader(db.codedb.Reader(), sr), nil
}

// ReadersWithCacheStats creates a pair of state readers that share the same
// underlying state reader and internal state cache, while maintaining separate
// statistics respectively.
func (db *BinaryDB) ReadersWithCacheStats(stateRoot common.Hash) (Reader, Reader, error) {
	r, err := db.StateReader(stateRoot)
	if err != nil {
		return nil, nil, err
	}
	sr := newStateReaderWithCache(r)
	ra := newReader(db.codedb.Reader(), newStateReaderWithStats(sr))
	rb := newReader(db.codedb.Reader(), newStateReaderWithStats(sr))
	return ra, rb, nil
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *BinaryDB) OpenTrie(root common.Hash) (Trie, error) {
	return openBinaryTree(root, db.triedb, db.firstBinary)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *BinaryDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	return self, nil
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *BinaryDB) TrieDB() *triedb.Database {
	return db.triedb
}

// Snapshot returns the underlying state snapshot.
func (db *BinaryDB) Snapshot() *snapshot.Tree {
	return nil
}

// Commit flushes all pending writes and finalizes the state transition,
// committing the changes to the underlying storage. It returns an error
// if the commit fails.
func (db *BinaryDB) Commit(update *stateUpdate) error {
	// Short circuit if nothing to commit
	if update.empty() {
		return nil
	}
	// Commit dirty contract code if any exists
	if len(update.codes) > 0 {
		batch := db.codedb.NewBatchWithSize(len(update.codes))
		for _, code := range update.codes {
			batch.Put(code.hash, code.blob)
		}
		if err := batch.Commit(); err != nil {
			return err
		}
	}
	return db.triedb.Update(update.root, update.originRoot, update.blockNumber, update.nodes, update.stateSet())
}

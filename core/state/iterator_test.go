// Copyright 2016 The go-ethereum Authors
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
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
)

// Tests that the node iterator indeed walks over the entire database contents.
func TestNodeIteratorCoverageHashBased(t *testing.T) { testNodeIteratorCoverage(t, trie.HashScheme) }
func TestNodeIteratorCoveragePathBased(t *testing.T) { testNodeIteratorCoverage(t, trie.PathScheme) }

func testNodeIteratorCoverage(t *testing.T, scheme string) {
	// Create some arbitrary test state to iterate
	db, sdb, ndb, root, _ := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb, nil)
	if err != nil {
		t.Fatalf("failed to create state trie at %x: %v", root, err)
	}
	// Gather all the node hashes found by the iterator
	hashes := make(map[common.Hash]struct{})
	for it := NewNodeIterator(state); it.Next(); {
		if it.Hash != (common.Hash{}) {
			hashes[it.Hash] = struct{}{}
		}
	}
	// Check in-disk nodes
	var (
		seenNodes = make(map[common.Hash]struct{})
		seenCodes = make(map[common.Hash]struct{})
	)
	it := db.NewIterator(nil, nil)
	for it.Next() {
		if ok, _ := ndb.Scheme().IsTrieNode(it.Key()); !ok {
			continue
		}
		if scheme == trie.HashScheme {
			seenNodes[common.BytesToHash(it.Key())] = struct{}{}
		} else {
			hash := crypto.Keccak256Hash(it.Value())
			if _, ok := hashes[hash]; !ok {
				t.Errorf("state entry not reported %x", it.Key())
			}
			seenNodes[hash] = struct{}{}
		}
	}
	it.Release()

	// Check in-disk codes
	it = db.NewIterator(nil, nil)
	for it.Next() {
		ok, hash := rawdb.IsCodeKey(it.Key())
		if !ok {
			continue
		}
		if _, ok := hashes[common.BytesToHash(hash)]; !ok {
			t.Errorf("state entry not reported %x", it.Key())
		}
		seenCodes[common.BytesToHash(hash)] = struct{}{}
	}
	it.Release()

	// Cross check the iterated hashes and the database/nodepool content
	for hash := range hashes {
		_, ok := seenNodes[hash]
		if !ok {
			_, ok = seenCodes[hash]
		}
		if !ok {
			t.Errorf("failed to retrieve reported node %x", hash)
		}
	}
}

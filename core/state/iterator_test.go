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
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
)

// Tests that the node iterator indeed walks over the entire database contents.
func TestNodeIteratorCoverage(t *testing.T) {
	testNodeIteratorCoverage(t, rawdb.HashScheme)
	testNodeIteratorCoverage(t, rawdb.PathScheme)
}

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
	for it := newNodeIterator(state); it.Next(); {
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
		ok, hash := isTrieNode(scheme, it.Key(), it.Value())
		if !ok {
			continue
		}
		seenNodes[hash] = struct{}{}
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

// isTrieNode is a helper function which reports if the provided
// database entry belongs to a trie node or not.
func isTrieNode(scheme string, key, val []byte) (bool, common.Hash) {
	if scheme == rawdb.HashScheme {
		if rawdb.IsLegacyTrieNode(key, val) {
			return true, common.BytesToHash(key)
		}
	} else {
		ok := rawdb.IsAccountTrieNode(key)
		if ok {
			return true, crypto.Keccak256Hash(val)
		}
		ok = rawdb.IsStorageTrieNode(key)
		if ok {
			return true, crypto.Keccak256Hash(val)
		}
	}
	return false, common.Hash{}
}

func TestFoo(t *testing.T) {
	a := common.FromHex("0xf90211a039ba7d5a8c3aad095be2f6e463cc77f48d63dfdc41b3f696da442623949fa354a0a1bf13a2c41cce4d77644430167fa58915122e0afcc233377209b13d4946f7aea045cadab880f1cc73a2cde0738a34d5d2c9fc02ba6e2e650424646734b0c5b6c1a0f625cb110cae1a07e0d230167580b1b65a45dd93926799caf9433e0664a1a7e9a036ac8e34dfb3cec8dd77516829529c6b4bba72cb3f3894b8a7ea92d0472deaa4a08c73b676fbbfad22866e0b137df12ce761744b983f0f88fbb5fd51749366828ba059a5b45d9c3e85a7d36197e6099fde77ea09909d490ae7dc024ce71e82998191a06aca76b0dbbb0de07d9249d01c96aec5dc28053582d1676dcf0595d4727d10f6a09df8b631d963633567bda992a64e8744b381c66ced48293c219d6f547e062894a0815003ea30329f06adb377e3b84c0791412a7b4cd1ea0b10d46440ba0c34dc23a0462822f07e8d579379602d1017d70eb9f01407567bb5681665cc66ba5df520afa080dd645f3100daddb9c159af545f5bb1d671730882a79d45e100d372ae83a53aa0fbcaeb83bc626d987fac08c420936f6b6f48658a7f7adc7a785dc9fece3c1679a0f0cf026dd0f0b8d88c4cf95f127507cba4c6e996d47561130ff39c273b636827a000a776e82d5075c272c52bafe78a30b283d3a1437e0a87495d17fe80c659ba88a010f9f3f068e4066cee16fcedcfabdf2b0272e974d1ddc313d0ce4884418aa51f80")
	fmt.Println(crypto.Keccak256Hash(a).Hex())
}

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

package pathdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/internal/testrand"
)

// randomTrienodes generates a random trienode set.
func randomTrienodes(n int) map[common.Hash]map[string][]byte {
	nodes := make(map[common.Hash]map[string][]byte)
	for i := 0; i < n; i++ {
		owner := testrand.Hash()
		nodes[owner] = make(map[string][]byte)
		for j := 0; j < 2; j++ {
			path := testrand.Bytes(10)
			for z := 0; z < len(path); z++ {
				nodes[owner][string(path[:z])] = testrand.Bytes(32)
			}
		}
	}
	return nodes
}

func makeTrinodeHistory() *trienodeHistory {
	return newTrienodeHistory(randomTrienodes(1))
}

func TestEncodeDecodeTrienodeHistory(t *testing.T) {
	var (
		dec trienodeHistory
		obj = makeTrinodeHistory()
	)
	blob, err := obj.encode()
	if err != nil {
		t.Fatalf("Failed to encode trienode history: %v", err)
	}
	if err := dec.decode(blob); err != nil {
		t.Fatalf("Failed to decode trienode history: %v", err)
	}
	if !compareList(dec.owners, obj.owners) {
		t.Fatal("trie owner list is mismatched")
	}
	if !compareMapList(dec.nodeList, obj.nodeList) {
		t.Fatal("trienode list is mismatched")
	}
	if !compareMapSet(dec.nodes, obj.nodes) {
		t.Fatal("trienode content is mismatched")
	}
}

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

package pathdb

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// benchLayer is the input of a mainnet-like state transition: 500 accounts (a
// fifth of them hot), 600 slots over the 60 hottest contracts, along with the
// trie nodes on their paths. unique is an account and its leaf node path only
// modified by this transition.
type benchLayer struct {
	nodes  *trienode.MergedNodeSet
	states *StateSetWithOrigin
	unique common.Hash
}

func benchNibbles(h common.Hash) []byte {
	buf := make([]byte, 64)
	for i, b := range h {
		buf[2*i], buf[2*i+1] = b>>4, b&0xf
	}
	return buf
}

func newBenchLayer(r *rand.Rand, hot []common.Hash) *benchLayer {
	var (
		nodes    = trienode.NewMergedNodeSet()
		accounts = make(map[common.Hash][]byte)
		storages = make(map[common.Hash]map[common.Hash][]byte)
		sets     = map[common.Hash]*trienode.NodeSet{{}: trienode.NewNodeSet(common.Hash{})}
		seen     = make(map[common.Hash]map[string]bool)
		val      = []byte{0x1}
		unique   common.Hash
	)
	addNode := func(owner common.Hash, path []byte) {
		if seen[owner] == nil {
			seen[owner] = make(map[string]bool)
		}
		if seen[owner][string(path)] {
			return
		}
		seen[owner][string(path)] = true
		if sets[owner] == nil {
			sets[owner] = trienode.NewNodeSet(owner)
		}
		blob := append([]byte{0x80}, path...)
		sets[owner].AddNode(path, trienode.NewNodeWithPrev(crypto.Keccak256Hash(blob), blob, nil))
	}
	for i := 0; i < 500; i++ {
		var acct common.Hash
		if i < 100 {
			acct = hot[r.Intn(len(hot))]
		} else {
			r.Read(acct[:])
			unique = acct
		}
		accounts[acct] = val
		p := benchNibbles(acct)
		for d := 0; d <= 9; d++ {
			addNode(common.Hash{}, p[:d])
		}
	}
	for i := 0; i < 600; i++ {
		owner := hot[r.Intn(60)]
		if storages[owner] == nil {
			storages[owner] = make(map[common.Hash][]byte)
		}
		var slot common.Hash
		r.Read(slot[:])
		storages[owner][slot] = val
		p := benchNibbles(slot)
		for d := 0; d <= 5; d++ {
			addNode(owner, p[:d])
		}
	}
	for _, set := range sets {
		nodes.Merge(set)
	}
	return &benchLayer{
		nodes:  nodes,
		states: NewStateSetWithOrigin(accounts, storages, nil, nil, false),
		unique: unique,
	}
}

// newBenchTree constructs a database holding 128 diff layers on top of the disk
// layer, with the pool of the transitions for the following updates.
func newBenchTree(b *testing.B) (*Database, []common.Hash, []*benchLayer, []*benchLayer) {
	r := rand.New(rand.NewSource(1))
	hot := make([]common.Hash, 200)
	for i := range hot {
		r.Read(hot[i][:])
	}
	db := New(rawdb.NewMemoryDatabase(), &Config{
		WriteBufferSize: 256 * 1024 * 1024,
		SnapshotNoBuild: true,
		NoAsyncFlush:    true,
	}, false)

	var (
		roots  = []common.Hash{types.EmptyRootHash}
		layers []*benchLayer
		pool   []*benchLayer
	)
	for i := 0; i < 128; i++ {
		l := newBenchLayer(r, hot)
		var root common.Hash
		r.Read(root[:])
		if err := db.Update(root, roots[len(roots)-1], uint64(i+1), l.nodes, l.states); err != nil {
			b.Fatal(err)
		}
		roots = append(roots, root)
		layers = append(layers, l)
	}
	for i := 0; i < 256; i++ {
		pool = append(pool, newBenchLayer(r, hot))
	}
	return db, roots, layers, pool
}

// BenchmarkLayerTreeUpdate measures the cost of linking a new layer on top of
// 128 diff layers and persisting the bottom one into the buffer.
func BenchmarkLayerTreeUpdate(b *testing.B) {
	db, roots, _, pool := newBenchTree(b)
	r := rand.New(rand.NewSource(2))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := pool[i%len(pool)]
		var root common.Hash
		r.Read(root[:])
		if err := db.Update(root, roots[len(roots)-1], uint64(len(roots)), l.nodes, l.states); err != nil {
			b.Fatal(err)
		}
		roots = append(roots, root)
	}
}

// BenchmarkLayerTreeRead measures the state and trie node lookups from the head
// of 128 diff layers, for the entries outside the diff layers and the entries
// at various depths.
func BenchmarkLayerTreeRead(b *testing.B) {
	db, roots, layers, _ := newBenchTree(b)
	head := roots[len(roots)-1]

	// Persist a trie node only available in the disk layer
	missPath := []byte{0x10, 0x10}
	missBlob := []byte{0x80, 0x1}
	rawdb.WriteAccountTrieNode(db.diskdb, missPath, missBlob)
	missHash := crypto.Keccak256Hash(missBlob)

	stateReader, err := db.StateReader(head)
	if err != nil {
		b.Fatal(err)
	}
	sr := stateReader.(*reader)
	nr, err := db.NodeReader(head)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("account/miss", func(b *testing.B) {
		var hash common.Hash
		for i := 0; i < b.N; i++ {
			hash[0], hash[1] = byte(i), byte(i>>8)
			// Without the snapshot generated, the disk layer reports the
			// entries beyond the buffer as not covered.
			if _, err := sr.AccountRLP(hash); err != nil && !errors.Is(err, errNotCoveredYet) {
				b.Fatal(err)
			}
		}
	})
	b.Run("node/miss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := nr.Node(common.Hash{}, missPath, missHash); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, depth := range []int{1, 32, 100} {
		var (
			l    = layers[len(layers)-1-depth]
			path = benchNibbles(l.unique)[:9]
			blob = append([]byte{0x80}, path...)
		)
		b.Run(fmt.Sprintf("account/depth-%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if blob, err := sr.AccountRLP(l.unique); err != nil || len(blob) == 0 {
					b.Fatal("missing account", err)
				}
			}
		})
		b.Run(fmt.Sprintf("node/depth-%d", depth), func(b *testing.B) {
			hash := crypto.Keccak256Hash(blob)
			for i := 0; i < b.N; i++ {
				if _, err := nr.Node(common.Hash{}, path, hash); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// lookupTestLayer constructs a diff layer with the given number of mutated
// accounts (a fifth of them drawn from the hot set) and slots (spread over the
// 100 hottest contracts).
func lookupTestLayer(r *rand.Rand, hot []common.Hash, nAccounts int, nSlots int) *diffLayer {
	var (
		val      = []byte{0x1}
		accounts = make(map[common.Hash][]byte)
		storages = make(map[common.Hash]map[common.Hash][]byte)
		root     common.Hash
	)
	for i := 0; i < nAccounts; i++ {
		var acct common.Hash
		if i < nAccounts/5 {
			acct = hot[r.Intn(len(hot))]
		} else {
			r.Read(acct[:])
		}
		accounts[acct] = val
	}
	for i := 0; i < nSlots; i++ {
		owner := hot[r.Intn(100)]
		if storages[owner] == nil {
			storages[owner] = make(map[common.Hash][]byte)
		}
		var slot common.Hash
		r.Read(slot[:])
		storages[owner][slot] = val
	}
	r.Read(root[:])
	return &diffLayer{root: root, states: NewStateSetWithOrigin(accounts, storages, nil, nil, false)}
}

// BenchmarkLookupAddRemove measures the cost of linking a new diff layer into
// the lookup and unlinking the oldest one, with 128 layers retained.
func BenchmarkLookupAddRemove(b *testing.B) {
	b.Run("storage-heavy", func(b *testing.B) { benchmarkLookupAddRemove(b, 1500, 5000) })
	b.Run("account-heavy", func(b *testing.B) { benchmarkLookupAddRemove(b, 5000, 1500) })
}

func benchmarkLookupAddRemove(b *testing.B, nAccounts int, nSlots int) {
	r := rand.New(rand.NewSource(1))
	hot := make([]common.Hash, 500)
	for i := range hot {
		r.Read(hot[i][:])
	}
	pool := make([]*diffLayer, 256)
	for i := range pool {
		pool[i] = lookupTestLayer(r, hot, nAccounts, nSlots)
	}
	l := newLookup(nil, func(common.Hash, common.Hash) bool { return false })

	var live []*diffLayer
	for i := 0; i < 128; i++ {
		l.addLayer(pool[i])
		live = append(live, pool[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Recycle the layer leaving the window, keeping the roots unique
		next := pool[(128+i)%len(pool)]
		r.Read(next.root[:])
		l.addLayer(next)
		live = append(live, next)

		if err := l.removeLayer(live[0]); err != nil {
			b.Fatal(err)
		}
		live = live[1:]
	}
}

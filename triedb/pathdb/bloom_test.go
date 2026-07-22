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
)

// BenchmarkBloomInsertSet measures the cost of inserting the keys of a single
// state transition into the bloom filter, which is paid within the critical
// commit path.
func BenchmarkBloomInsertSet(b *testing.B) {
	var (
		r             = rand.New(rand.NewSource(0x1337))
		nodes, states = randomBufferSets(r, 4096)
		bloom         = newBufferBloom(defaultBufferSize)
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bloom.insertSet(nodes, states)
	}
}

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

package utils

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-verkle"
)

func TestTreeKey(t *testing.T) {
	var (
		address      = []byte{0x01}
		addressEval  = EvaluateAddressPoint(address)
		smallIndex   = 1
		largeIndex   = 10000
		smallStorage = []byte{0x1}
		largeStorage = bytes.Repeat([]byte{0xff}, 16)
	)
	if !bytes.Equal(BasicDataKey(address), BasicDataKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched basic data key")
	}
	if !bytes.Equal(CodeHashKey(address), CodeHashKeyWithEvaluatedAddress(addressEval)) {
		t.Fatal("Unmatched code hash key")
	}
	if !bytes.Equal(CodeChunkKey(address, smallIndex), CodeChunkKeyWithEvaluatedAddress(addressEval, smallIndex)) {
		t.Fatal("Unmatched code chunk key")
	}
	if !bytes.Equal(CodeChunkKey(address, largeIndex), CodeChunkKeyWithEvaluatedAddress(addressEval, largeIndex)) {
		t.Fatal("Unmatched code chunk key")
	}
	if !bytes.Equal(StorageSlotKey(address, smallStorage), StorageSlotKeyWithEvaluatedAddress(addressEval, smallStorage)) {
		t.Fatal("Unmatched storage slot key")
	}
	if !bytes.Equal(StorageSlotKey(address, largeStorage), StorageSlotKeyWithEvaluatedAddress(addressEval, largeStorage)) {
		t.Fatal("Unmatched storage slot key")
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: Apple M1 Pro
// BenchmarkTreeKey
// BenchmarkTreeKey-8   	  611726	      1967 ns/op	      40 B/op	       2 allocs/op
func BenchmarkTreeKey(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BasicDataKey([]byte{0x01})
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: Apple M1 Pro
// BenchmarkTreeKeyWithEvaluation
// BenchmarkTreeKeyWithEvaluation-8   	  740726	      1620 ns/op	      40 B/op	       2 allocs/op
func BenchmarkTreeKeyWithEvaluation(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	addr := []byte{0x01}
	eval := EvaluateAddressPoint(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BasicDataKeyWithEvaluatedAddress(eval)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: Apple M1 Pro
// BenchmarkStorageKey
// BenchmarkStorageKey-8   	  441252	      2680 ns/op	     104 B/op	       4 allocs/op
func BenchmarkStorageKey(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		StorageSlotKey([]byte{0x01}, bytes.Repeat([]byte{0xff}, 32))
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie/utils
// cpu: Apple M1 Pro
// BenchmarkStorageKey
// BenchmarkStorageKey-8   	  452821	      2653 ns/op	     104 B/op	       4 allocs/op
func BenchmarkStorageKeyWithEvaluation(b *testing.B) {
	// Initialize the IPA settings which can be pretty expensive.
	verkle.GetConfig()

	addr := []byte{0x01}
	eval := EvaluateAddressPoint(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StorageSlotKeyWithEvaluatedAddress(eval, bytes.Repeat([]byte{0xff}, 32))
	}
}

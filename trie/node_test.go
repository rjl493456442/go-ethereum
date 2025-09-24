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

package trie

import (
	"bytes"
	"github.com/ethereum/go-ethereum/internal/testrand"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

func newTestFullNode(v []byte) []interface{} {
	fullNodeData := []interface{}{}
	for i := 0; i < 16; i++ {
		k := bytes.Repeat([]byte{byte(i + 1)}, 32)
		fullNodeData = append(fullNodeData, k)
	}
	fullNodeData = append(fullNodeData, v)
	return fullNodeData
}

func TestDecodeNestedNode(t *testing.T) {
	fullNodeData := newTestFullNode([]byte("fullnode"))

	data := [][]byte{}
	for i := 0; i < 16; i++ {
		data = append(data, nil)
	}
	data = append(data, []byte("subnode"))
	fullNodeData[15] = data

	buf := bytes.NewBuffer([]byte{})
	rlp.Encode(buf, fullNodeData)

	if _, err := decodeNode([]byte("testdecode"), buf.Bytes()); err != nil {
		t.Fatalf("decode nested full node err: %v", err)
	}
}

func TestDecodeFullNodeWrongSizeChild(t *testing.T) {
	fullNodeData := newTestFullNode([]byte("wrongsizechild"))
	fullNodeData[0] = []byte("00")
	buf := bytes.NewBuffer([]byte{})
	rlp.Encode(buf, fullNodeData)

	_, err := decodeNode([]byte("testdecode"), buf.Bytes())
	if _, ok := err.(*decodeError); !ok {
		t.Fatalf("decodeNode returned wrong err: %v", err)
	}
}

func TestDecodeFullNodeWrongNestedFullNode(t *testing.T) {
	fullNodeData := newTestFullNode([]byte("fullnode"))

	data := [][]byte{}
	for i := 0; i < 16; i++ {
		data = append(data, []byte("123456"))
	}
	data = append(data, []byte("subnode"))
	fullNodeData[15] = data

	buf := bytes.NewBuffer([]byte{})
	rlp.Encode(buf, fullNodeData)

	_, err := decodeNode([]byte("testdecode"), buf.Bytes())
	if _, ok := err.(*decodeError); !ok {
		t.Fatalf("decodeNode returned wrong err: %v", err)
	}
}

func TestDecodeFullNode(t *testing.T) {
	fullNodeData := newTestFullNode([]byte("decodefullnode"))
	buf := bytes.NewBuffer([]byte{})
	rlp.Encode(buf, fullNodeData)

	_, err := decodeNode([]byte("testdecode"), buf.Bytes())
	if err != nil {
		t.Fatalf("decode full node err: %v", err)
	}
}

func makeTestShortNode(small bool) []byte {
	l := leafNodeEncoder{}
	if small {
		l.Key = testrand.Bytes(10)
		l.Val = testrand.Bytes(10)
	} else {
		l.Key = testrand.Bytes(10)
		l.Val = testrand.Bytes(32)
	}
	buf := rlp.NewEncoderBuffer(nil)
	l.encode(buf)
	return buf.ToBytes()
}

func TestDecodeRawFullNode(t *testing.T) {
	var n fullnodeEncoder
	for i := 0; i < 16; i++ {
		switch rand.Intn(3) {
		case 0:
			// write nil
		case 1:
			// write hash
			n.Children[i] = testrand.Bytes(32)
		case 2:
			// write embedded node
			n.Children[i] = makeTestShortNode(true)
		}
	}
	n.Children[16] = testrand.Bytes(32) // value

	buf := rlp.NewEncoderBuffer(nil)
	n.encode(buf)
	enc := buf.ToBytes()

	children, err := decodeRawFullNode(enc)
	if err != nil {
		t.Fatalf("Failed to decode raw full node, %v", err)
	}
	if len(n.Children) != len(children) {
		t.Fatalf("Length mismatch, want: %d, got: %d", len(n.Children), len(children))
	}
	for i := 0; i < len(n.Children); i++ {
		if !reflect.DeepEqual(n.Children[i], children[i]) {
			t.Fatalf("Child at %d mismatch, want: %v, got: %v", i, n.Children[i], children[i])
		}
	}
}

func TestDeriveFullNodeDiffs(t *testing.T) {
	type testsuite struct {
		old        []byte
		new        []byte
		expErr     bool
		expIndices []int
		expValues  [][]byte
	}
	makeFullNodes := func() ([]byte, []byte, [][]byte, []int) {
		var (
			na      = fullnodeEncoder{}
			nb      = fullnodeEncoder{}
			indices []int
			values  [][]byte
		)
		for i := 0; i < 16; i++ {
			switch rand.Intn(3) {
			case 0:
				// write nil
			case 1:
				// write same
				var child []byte
				if rand.Intn(2) == 0 {
					child = testrand.Bytes(32) // hashnode
				} else {
					child = makeTestShortNode(true) // embedded node
				}
				na.Children[i] = child
				nb.Children[i] = child
			case 2:
				// write different
				var va []byte
				if rand.Intn(2) == 0 {
					va = testrand.Bytes(32) // hashnode
				} else {
					va = makeTestShortNode(true) // embedded node
				}
				vb := testrand.Bytes(32) // hashnode
				na.Children[i] = va
				nb.Children[i] = vb
				indices = append(indices, i)
				values = append(values, va)
			}
		}
		bufa, bufb := rlp.NewEncoderBuffer(nil), rlp.NewEncoderBuffer(nil)
		na.encode(bufa)
		nb.encode(bufb)
		return bufa.ToBytes(), bufb.ToBytes(), values, indices
	}
	makeFullNode := func() []byte {
		na, _, _, _ := makeFullNodes()
		return na
	}
	makeTestsuite := func() testsuite {
		oldn, newn, values, indices := makeFullNodes()
		return testsuite{
			old:        oldn,
			new:        newn,
			expErr:     false,
			expIndices: indices,
			expValues:  values,
		}
	}

	var tests = []testsuite{
		// Invalid node data
		{
			old: nil, new: nil, expErr: true,
		},
		{
			old: testrand.Bytes(32), new: nil, expErr: true,
		},
		{
			old: nil, new: testrand.Bytes(32), expErr: true,
		},
		{
			old: testrand.Bytes(32), new: testrand.Bytes(32), expErr: true,
		},
		// Short node data
		{
			old: makeTestShortNode(true), new: makeTestShortNode(true), expErr: true,
		},
		{
			old: makeTestShortNode(false), new: makeTestShortNode(false), expErr: true,
		},
		{
			old: makeTestShortNode(true), new: makeFullNode(), expErr: true,
		},
		{
			old: makeFullNode(), new: makeTestShortNode(true), expErr: true,
		},
		{
			old: makeTestShortNode(false), new: makeFullNode(), expErr: true,
		},
		{
			old: makeFullNode(), new: makeTestShortNode(false), expErr: true,
		},
	}
	for i := 0; i < 10; i++ {
		tests = append(tests, makeTestsuite())
	}

	for _, test := range tests {
		indices, values, err := FullNodeDifference(test.old, test.new)
		if test.expErr && err == nil {
			t.Fatal("Expect error, got nil")
		}
		if !test.expErr && err != nil {
			t.Fatalf("Unexpect error, %v", err)
		}
		if err == nil {
			if !reflect.DeepEqual(indices, test.expIndices) {
				t.Fatalf("Unexpected indices, want: %v, got: %v", test.expIndices, indices)
			}
			if !reflect.DeepEqual(values, test.expValues) {
				t.Fatalf("Unexpected values, want: %v, got: %v", test.expValues, values)
			}
		}
	}
}

func makeFullNodes() ([]byte, []byte, [][]byte, []int) {
	var (
		na      = fullnodeEncoder{}
		nb      = fullnodeEncoder{}
		indices []int
		values  [][]byte
	)
	for i := 0; i < 16; i++ {
		switch rand.Intn(3) {
		case 0:
			// write nil
		case 1:
			// write same
			var child []byte
			if rand.Intn(2) == 0 {
				child = testrand.Bytes(32) // hashnode
			} else {
				child = makeTestShortNode(true) // embedded node
			}
			na.Children[i] = child
			nb.Children[i] = child
		case 2:
			// write different
			var va []byte
			if rand.Intn(2) == 0 {
				va = testrand.Bytes(32) // hashnode
			} else {
				va = makeTestShortNode(true) // embedded node
			}
			vb := testrand.Bytes(32) // hashnode
			na.Children[i] = va
			nb.Children[i] = vb
			indices = append(indices, i)
			values = append(values, va)
		}
	}
	bufa, bufb := rlp.NewEncoderBuffer(nil), rlp.NewEncoderBuffer(nil)
	na.encode(bufa)
	nb.encode(bufb)
	return bufa.ToBytes(), bufb.ToBytes(), values, indices
}

func TestReassembleFullNode(t *testing.T) {
	var fn fullnodeEncoder
	for i := 0; i < 16; i++ {
		if rand.Intn(2) == 0 {
			fn.Children[i] = testrand.Bytes(32)
		}
	}
	buf := rlp.NewEncoderBuffer(nil)
	fn.encode(buf)
	enc := buf.ToBytes()

	// Generate a list of diffs
	var (
		values  [][][]byte
		indices [][]int
	)
	for i := 0; i < 10; i++ {
		var (
			pos       = make(map[int]struct{})
			poslist   []int
			valuelist [][]byte
		)
		for j := 0; j < 3; j++ {
			p := rand.Intn(16)
			if _, ok := pos[p]; ok {
				continue
			}
			pos[p] = struct{}{}

			nh := testrand.Bytes(32)
			poslist = append(poslist, p)
			valuelist = append(valuelist, nh)
			fn.Children[p] = nh
		}
		values = append(values, valuelist)
		indices = append(indices, poslist)
	}
	reassembled, err := ReassembleFullNode(enc, values, indices)
	if err != nil {
		t.Fatalf("Failed to re-assemble full node %v", err)
	}
	buf2 := rlp.NewEncoderBuffer(nil)
	fn.encode(buf2)
	enc2 := buf2.ToBytes()
	if !reflect.DeepEqual(enc2, reassembled) {
		t.Fatalf("Unexpeted reassembled node")
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkEncodeShortNode
// BenchmarkEncodeShortNode-8   	16878850	        70.81 ns/op	      48 B/op	       1 allocs/op
func BenchmarkEncodeShortNode(b *testing.B) {
	node := &shortNode{
		Key: []byte{0x1, 0x2},
		Val: hashNode(randBytes(32)),
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		nodeToBytes(node)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkEncodeFullNode
// BenchmarkEncodeFullNode-8   	 4323273	       284.4 ns/op	     576 B/op	       1 allocs/op
func BenchmarkEncodeFullNode(b *testing.B) {
	node := &fullNode{}
	for i := 0; i < 16; i++ {
		node.Children[i] = hashNode(randBytes(32))
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		nodeToBytes(node)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkDecodeShortNode
// BenchmarkDecodeShortNode-8   	 7925638	       151.0 ns/op	     157 B/op	       4 allocs/op
func BenchmarkDecodeShortNode(b *testing.B) {
	node := &shortNode{
		Key: []byte{0x1, 0x2},
		Val: hashNode(randBytes(32)),
	}
	blob := nodeToBytes(node)
	hash := crypto.Keccak256(blob)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mustDecodeNode(hash, blob)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkDecodeShortNodeUnsafe
// BenchmarkDecodeShortNodeUnsafe-8   	 9027476	       128.6 ns/op	     109 B/op	       3 allocs/op
func BenchmarkDecodeShortNodeUnsafe(b *testing.B) {
	node := &shortNode{
		Key: []byte{0x1, 0x2},
		Val: hashNode(randBytes(32)),
	}
	blob := nodeToBytes(node)
	hash := crypto.Keccak256(blob)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mustDecodeNodeUnsafe(hash, blob)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkDecodeFullNode
// BenchmarkDecodeFullNode-8   	 1597462	       761.9 ns/op	    1280 B/op	      18 allocs/op
func BenchmarkDecodeFullNode(b *testing.B) {
	node := &fullNode{}
	for i := 0; i < 16; i++ {
		node.Children[i] = hashNode(randBytes(32))
	}
	blob := nodeToBytes(node)
	hash := crypto.Keccak256(blob)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mustDecodeNode(hash, blob)
	}
}

// goos: darwin
// goarch: arm64
// pkg: github.com/ethereum/go-ethereum/trie
// BenchmarkDecodeFullNodeUnsafe
// BenchmarkDecodeFullNodeUnsafe-8   	 1789070	       687.1 ns/op	     704 B/op	      17 allocs/op
func BenchmarkDecodeFullNodeUnsafe(b *testing.B) {
	node := &fullNode{}
	for i := 0; i < 16; i++ {
		node.Children[i] = hashNode(randBytes(32))
	}
	blob := nodeToBytes(node)
	hash := crypto.Keccak256(blob)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mustDecodeNodeUnsafe(hash, blob)
	}
}

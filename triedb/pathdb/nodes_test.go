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
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

func TestNodeSetEncode(t *testing.T) {
	testNodeSetEncode(t)
	testNodeSetEncode(t)
}

func testNodeSetEncode(t *testing.T) {
	nodes := make(map[common.Hash]map[string]*trienode.Node)
	nodes[common.Hash{}] = map[string]*trienode.Node{
		"":  trienode.New(crypto.Keccak256Hash([]byte{0x0}), []byte{0x0}),
		"1": trienode.New(crypto.Keccak256Hash([]byte{0x1}), []byte{0x1}),
		"2": trienode.New(crypto.Keccak256Hash([]byte{0x2}), []byte{0x2}),
	}
	nodes[common.Hash{0x1}] = map[string]*trienode.Node{
		"":  trienode.New(crypto.Keccak256Hash([]byte{0x0}), []byte{0x0}),
		"1": trienode.New(crypto.Keccak256Hash([]byte{0x1}), []byte{0x1}),
		"2": trienode.New(crypto.Keccak256Hash([]byte{0x2}), []byte{0x2}),
	}
	s := newNodeSet(nodes)

	buf := bytes.NewBuffer(nil)
	if err := s.encode(buf); err != nil {
		t.Fatalf("Failed to encode states, %v", err)
	}
	var dec nodeSet
	if err := dec.decode(rlp.NewStream(buf, 0)); err != nil {
		t.Fatalf("Failed to decode states, %v", err)
	}
	if !reflect.DeepEqual(s.accountNodes, dec.accountNodes) {
		t.Fatal("Unexpected account data")
	}
	if !reflect.DeepEqual(s.storageNodes, dec.storageNodes) {
		t.Fatal("Unexpected storage data")
	}
}

func TestNodeSetWithOriginEncode(t *testing.T) {
	testNodeSetWithOriginEncode(t)
	testNodeSetWithOriginEncode(t)
}

func testNodeSetWithOriginEncode(t *testing.T) {
	nodes := make(map[common.Hash]map[string]*trienode.Node)
	nodes[common.Hash{}] = map[string]*trienode.Node{
		"":  trienode.New(crypto.Keccak256Hash([]byte{0x0}), []byte{0x0}),
		"1": trienode.New(crypto.Keccak256Hash([]byte{0x1}), []byte{0x1}),
		"2": trienode.New(crypto.Keccak256Hash([]byte{0x2}), []byte{0x2}),
	}
	nodes[common.Hash{0x1}] = map[string]*trienode.Node{
		"":  trienode.New(crypto.Keccak256Hash([]byte{0x0}), []byte{0x0}),
		"1": trienode.New(crypto.Keccak256Hash([]byte{0x1}), []byte{0x1}),
		"2": trienode.New(crypto.Keccak256Hash([]byte{0x2}), []byte{0x2}),
	}
	origins := make(map[common.Hash]map[string][]byte)
	origins[common.Hash{}] = map[string][]byte{
		"":  nil,
		"1": {0x1},
		"2": {0x2},
	}
	origins[common.Hash{0x1}] = map[string][]byte{
		"":  nil,
		"1": {0x1},
		"2": {0x2},
	}
	s := NewNodeSetWithOrigin(nodes, origins)

	buf := bytes.NewBuffer(nil)
	if err := s.encode(buf); err != nil {
		t.Fatalf("Failed to encode states, %v", err)
	}
	var dec nodeSetWithOrigin
	if err := dec.decode(rlp.NewStream(buf, 0)); err != nil {
		t.Fatalf("Failed to decode states, %v", err)
	}
	if !reflect.DeepEqual(s.accountNodes, dec.accountNodes) {
		t.Fatal("Unexpected account data")
	}
	if !reflect.DeepEqual(s.storageNodes, dec.storageNodes) {
		t.Fatal("Unexpected storage data")
	}
	if !reflect.DeepEqual(s.accountNodeOrigin, dec.accountNodeOrigin) {
		t.Fatal("Unexpected account origin data")
	}
	if !reflect.DeepEqual(s.storageNodeOrigin, dec.storageNodeOrigin) {
		t.Fatal("Unexpected storage origin data")
	}
}

func TestFoo(t *testing.T) {
	x := common.FromHex("0xf90211a0e92d10b2ee583bdb8afdb6e678716bcd7f0cf4d2c7f400929e64d299e8e5c5c0a0371cb81ab845648c34cdcc0289fa9d2b492df356f0a5df0946f3606cd98e5805a0dff71b8276b1cadfce191f41789baba3beefdf79de1e7211b13d6274d41a127fa0438958a6fcff6cd669c9f138d741f083b31d231997ff7e286af8de8c9412a111a058c826798d827488574df1c38f9dc4394d76048574e9911a18ad250bde4ea015a09160b70d20c90d34c6e72e5197ad800c613e5b41275561e9719a44c69b7e7aeba003c44b499433df8c50b995cd90b14df15c05f0a5fa361cc483402adf03457da0a0129ae594911337f205406ef8945398fd4f416af5e949ce09cee8aea1c166d85fa0781cdd94b8e7d840b4907b98a786e7df1b19a71f9f5b5d7cc722a3c32c9fe8f1a07c8a14b719bcad793ba3b9861e791d918d71a7e52f67c2899df938bbf485f2efa0d44296be26b9be295f82de147fad5ce7e491b8a6a471347564ed807925b19f9ca02457f1ee167b77bbb161266e7eaa65383e9639c019d565f406e49c683e6b9179a02e1b8489c2f3da75d3d2c1ca06b467f38e134a19429ffc5d1f119957a5fe538fa0460d08cb9d2fd76d8d602ab9c4811c3ba5a6315281f8a0852b1047cceb41b394a04e4f1ad37f50351ff47600ab7e06b6aff0a46b6953ece79221b53c753665193da0a0d358defad4a218f7e434d908d12f1b415156e01746e61fd58ab878ddd22b2980")
	fmt.Println(crypto.Keccak256Hash(x).Hex())
}

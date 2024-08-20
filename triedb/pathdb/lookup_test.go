// Copyright 2024 The go-ethereum Authors
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
	"github.com/ethereum/go-ethereum/trie/trienode"
)

func TestAccountLookup(t *testing.T) {
	tr := newTestLayerTree() // base = 0x1

	tr.add(common.Hash{0x2}, common.Hash{0x1}, 1, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(
		nil,
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa1},
		},
		nil, nil, nil,
	))
	tr.add(common.Hash{0x3}, common.Hash{0x2}, 2, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(
		map[common.Hash]struct{}{
			common.Hash{0xa}: {},
		},
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa2},
			common.Hash{0xb}: {0xb2},
		},
		nil, nil, nil,
	))
	tr.add(common.Hash{0x4}, common.Hash{0x3}, 3, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(nil, nil, nil, nil, nil))

	var cases = []struct {
		account common.Hash
		state   common.Hash
		expect  common.Hash
	}{
		{
			common.Hash{0xa}, common.Hash{0x4}, common.Hash{0x3},
		},
		{
			common.Hash{0xa}, common.Hash{0x3}, common.Hash{0x3},
		},
		{
			common.Hash{0xa}, common.Hash{0x2}, common.Hash{0x2},
		},
		{
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x1},
		},
		{
			common.Hash{0xb}, common.Hash{0x4}, common.Hash{0x3},
		},
		{
			// unknown state
			common.Hash{0xc}, common.Hash{0x4}, common.Hash{0x1},
		},
	}
	for _, c := range cases {
		l := tr.lookupAccount(c.account, c.state)
		if l.rootHash() != c.expect {
			t.Errorf("Unexpected tiphash, want: %x, got: %x", c.expect, l.rootHash())
		}
	}
}

func TestStorageLookup(t *testing.T) {
	tr := newTestLayerTree() // base = 0x1

	tr.add(common.Hash{0x2}, common.Hash{0x1}, 1, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(
		nil,
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa1},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x1},
				common.Hash{0x2}: {0x2},
			},
		}, nil, nil,
	))
	tr.add(common.Hash{0x3}, common.Hash{0x2}, 2, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(
		map[common.Hash]struct{}{
			common.Hash{0xa}: {},
		},
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa2},
			common.Hash{0xb}: {0xb2},
		},
		nil, nil, nil,
	))
	tr.add(common.Hash{0x4}, common.Hash{0x3}, 3, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(
		nil,
		map[common.Hash][]byte{
			common.Hash{0xa}: {0xa3},
		},
		map[common.Hash]map[common.Hash][]byte{
			common.Hash{0xa}: {
				common.Hash{0x1}: {0x11},
				common.Hash{0x3}: {0x33},
			},
		},
		nil, nil,
	))
	tr.add(common.Hash{0x5}, common.Hash{0x4}, 3, trienode.NewMergedNodeSet(), NewStateSetWithOrigin(nil, nil, nil, nil, nil))

	var cases = []struct {
		account common.Hash
		storage common.Hash
		state   common.Hash
		expect  common.Hash
	}{
		{
			// unknown account
			common.Hash{0xc}, common.Hash{0x1}, common.Hash{0x5}, common.Hash{0x1},
		},
		{
			// untracked storage, be captured at account deletion
			common.Hash{0xa}, common.Hash{0x4}, common.Hash{0x5}, common.Hash{0x3},
		},
		{
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x5}, common.Hash{0x4},
		},
		{
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x4}, common.Hash{0x4},
		},
		{
			// storage deletion
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x3}, common.Hash{0x3},
		},
		{
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x2}, common.Hash{0x2},
		},
		{
			common.Hash{0xa}, common.Hash{0x1}, common.Hash{0x1}, common.Hash{0x1},
		},
		{
			// tracked storage, be captured at account deletion
			common.Hash{0xa}, common.Hash{0x2}, common.Hash{0x5}, common.Hash{0x3},
		},
		{
			// tracked storage, be captured at account deletion
			common.Hash{0xa}, common.Hash{0x2}, common.Hash{0x2}, common.Hash{0x2},
		},
	}
	for i, c := range cases {
		l := tr.lookupStorage(c.account, c.storage, c.state)
		if l.rootHash() != c.expect {
			t.Errorf("Unexpected tiphash, %d, want: %x, got: %x", i, c.expect, l.rootHash())
		}
	}
}

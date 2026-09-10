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

package trie

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// PartialStackTrie builds the subtrie of one partition of a larger trie on top
// of a StackTrie: the subtrie under a fixed nibble path, its prefix. It is used
// for parallel trie generation, where the key space is split into partitions by
// the leading nibbles and each partition is built independently before being
// mounted under a common root.
//
// Two adjustments make the produced subtrie line up with its position in the
// full trie:
//
//   - Keys are inserted with the prefix nibbles stripped. Those nibbles are
//     implied by the partition's position in the branches above it, so
//     duplicating them inside the keys would corrupt every node hash below.
//
//   - Paths reported to onTrieNode are prefixed with the partition's nibbles,
//     so a node's path matches its absolute position in the full trie. This is
//     required by the path-based storage scheme, which keys nodes by path.
//
// The hashes themselves are independent of the absolute path, so prefixing the
// path does not change any node hash.
//
// All inserted keys must begin with the prefix; the caller guarantees this by
// construction (e.g. by partitioning a hash range along nibble boundaries).
type PartialStackTrie struct {
	prefix  []byte // the partition's nibble path
	inner   *StackTrie
	pathBuf []byte // reusable buffer for the prefixed path
}

// NewPartialStackTrie creates a partition builder for the given leading nibble.
// See NewPartialStackTrieAt for the general form.
func NewPartialStackTrie(nibble byte, onTrieNode OnTrieNode) *PartialStackTrie {
	return NewPartialStackTrieAt([]byte{nibble}, onTrieNode)
}

// NewPartialStackTrieAt creates a partition builder for the subtrie under the
// given nibble path, which must be non-empty. The onTrieNode callback, if
// non-nil, is invoked for every committed node with its absolute path already
// prefixed with the partition's nibbles.
func NewPartialStackTrieAt(prefix []byte, onTrieNode OnTrieNode) *PartialStackTrie {
	if len(prefix) == 0 {
		panic("partial stack trie needs a non-empty prefix")
	}
	p := &PartialStackTrie{prefix: bytes.Clone(prefix)}
	p.inner = NewStackTrie(func(path []byte, hash common.Hash, blob []byte) {
		if onTrieNode == nil {
			return
		}
		// Prefix the path with the partition's nibbles. The buffer is reused
		// across calls, so the callback must consume it synchronously.
		p.pathBuf = append(p.pathBuf[:0], p.prefix...)
		p.pathBuf = append(p.pathBuf, path...)
		onTrieNode(p.pathBuf, hash, blob)
	})
	return p
}

// Update inserts a (key, value) pair, stripping the key's leading nibbles,
// which are implied by the partition. The key must begin with the prefix.
func (p *PartialStackTrie) Update(key, value []byte) error {
	if len(value) == 0 {
		return errors.New("trying to insert empty (deletion)")
	}
	t := p.inner
	t.grow(key)
	k := writeHexKey(t.kBuf, key)

	if len(k) < len(p.prefix) || !bytes.Equal(k[:len(p.prefix)], p.prefix) {
		return fmt.Errorf("unexpected key prefix %x, expected %x", k[:min(len(k), len(p.prefix))], p.prefix)
	}
	return t.update(k[len(p.prefix):], value)
}

// Hash returns the root hash of the partition subtrie (built with the prefix
// stripped). It is the reference the parent mounts at the prefix's last nibble.
func (p *PartialStackTrie) Hash() common.Hash {
	return p.inner.Hash()
}

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

// PartialStackTrie builds the subtree below a fixed nibble prefix. It strips
// that prefix from inserted keys and restores it on emitted node paths so
// nodes can be persisted at their absolute positions in the full trie.
type PartialStackTrie struct {
	prefix  []byte
	inner   *StackTrie
	pathBuf []byte // reusable buffer for the absolute path
}

// NewPartialStackTrie creates a partition builder for the given leading nibble.
// The onTrieNode callback, if non-nil, is invoked for every committed node with
// its absolute path already prefixed with the partition nibble.
func NewPartialStackTrie(nibble byte, onTrieNode OnTrieNode) *PartialStackTrie {
	return newPartialStackTrie([]byte{nibble}, false, onTrieNode)
}

// NewBytePartitionStackTrie builds the account subtree for one full leading
// byte. Reverse selects descending key and callback order.
func NewBytePartitionStackTrie(partition byte, reverse bool, onTrieNode OnTrieNode) *PartialStackTrie {
	return newPartialStackTrie([]byte{partition >> 4, partition & 0xf}, reverse, onTrieNode)
}

func newPartialStackTrie(prefix []byte, reverse bool, onTrieNode OnTrieNode) *PartialStackTrie {
	p := &PartialStackTrie{prefix: prefix}
	p.inner = newStackTrie(func(path []byte, hash common.Hash, blob []byte) {
		if onTrieNode == nil {
			return
		}
		// Prefix the path with the partition nibble. The buffer is reused across
		// calls, so the callback must consume it synchronously.
		p.pathBuf = append(p.pathBuf[:0], prefix...)
		p.pathBuf = append(p.pathBuf, path...)
		onTrieNode(p.pathBuf, hash, blob)
	}, newUnsafeBytesPool(common.HashLength, stackTrieHashPoolSize))
	p.inner.reverse = reverse
	return p
}

// NewReversePartialStackTrie builds the partition from descending keys and
// emits nodes in strictly descending order of their nibble-prefixed paths.
func NewReversePartialStackTrie(nibble byte, onTrieNode OnTrieNode) *PartialStackTrie {
	return newPartialStackTrie([]byte{nibble}, true, onTrieNode)
}

// NewStorageTrie creates a full trie with the partition's insertion order and
// hash buffer pool. Use the partition and its storage tries serially so they
// can reuse the buffers across successive accounts.
func (p *PartialStackTrie) NewStorageTrie(onTrieNode OnTrieNode) *StackTrie {
	t := newStackTrie(onTrieNode, p.inner.hashPool)
	t.reverse = p.inner.reverse
	return t
}

// Update inserts a (key, value) pair, stripping the partition prefix.
func (p *PartialStackTrie) Update(key, value []byte) error {
	if len(value) == 0 {
		return errors.New("trying to insert empty (deletion)")
	}
	t := p.inner
	t.grow(key)
	k := writeHexKey(t.kBuf, key)

	if !bytes.HasPrefix(k, p.prefix) {
		return fmt.Errorf("key %x outside partition %x", key, p.prefix)
	}
	return t.update(k[len(p.prefix):], value)
}

// Hash returns the root hash of the subtree below the partition prefix.
func (p *PartialStackTrie) Hash() common.Hash {
	return p.inner.Hash()
}

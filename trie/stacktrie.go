// Copyright 2020 The go-ethereum Authors
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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

// StackTrie is a trie implementation that expects keys to be inserted
// in order. Once it determines that a subtree will no longer be inserted
// into, it will hash it and free up the memory it uses.
type StackTrie struct {
	nodeType  uint8          // node type (as in branch, ext, leaf)
	val       []byte         // value contained by this node if it's a leaf
	key       []byte         // key chunk covered by this (full|ext) node
	keyOffset int            // offset of the key chunk inside a full key
	children  [16]*StackTrie // list of children (for fullnodes and exts)

	db *Database // Pointer to the commit db, can be nil
}

// NewStackTrie allocates and initializes an empty trie.
func NewStackTrie(db *Database) *StackTrie {
	return &StackTrie{
		nodeType: emptyNode,
		db:       db,
	}
}

func newLeaf(ko int, key, val []byte, db *Database) *StackTrie {
	return &StackTrie{
		nodeType:  leafNode,
		keyOffset: ko,
		key:       key[ko:],
		val:       val,
		db:        db,
	}
}

func (st *StackTrie) convertToHash(ko int) {
	st.keyOffset = ko
	st.val = st.hash()
	st.nodeType = hashedNode
	st.key = nil
}

func newExt(ko int, key []byte, child *StackTrie, db *Database) *StackTrie {
	st := &StackTrie{
		nodeType:  extNode,
		keyOffset: ko,
		key:       key[ko:],
		db:        db,
	}
	st.children[0] = child
	return st
}

// List all values that StackTrie#nodeType can hold
const (
	emptyNode = iota
	branchNode
	extNode
	leafNode
	hashedNode
)

// TryUpdate inserts a (key, value) pair into the stack trie
func (st *StackTrie) TryUpdate(key, value []byte) error {
	k := keybytesToHex(key)
	if len(value) == 0 {
		panic("deletion not supported")
	}
	st.insert(k[:len(k)-1], value)
	return nil
}

// Helper function that, given a full key, determines the index
// at which the chunk pointed by st.keyOffset is different from
// the same chunk in the full key.
func (st *StackTrie) getDiffIndex(key []byte) int {
	diffindex := 0
	for ; diffindex < len(st.key) && st.key[diffindex] == key[st.keyOffset+diffindex]; diffindex++ {
	}
	return diffindex
}

// Helper function to that inserts a (key, value) pair into
// the trie.
func (st *StackTrie) insert(key, value []byte) {
	switch st.nodeType {
	case branchNode: /* Branch */
		idx := int(key[st.keyOffset])
		if st.children[idx] == nil {
			st.children[idx] = NewStackTrie(st.db)
			st.children[idx].keyOffset = st.keyOffset + 1
		}
		for i := idx - 1; i >= 0; i-- {
			if st.children[i] != nil {
				if st.children[i].nodeType != hashedNode {
					st.children[i].val = st.children[i].hash()
					st.children[i].key = nil
					st.children[i].nodeType = hashedNode
				}

				break
			}

		}
		st.children[idx].insert(key, value)
	case extNode: /* Ext */
		// Compare both key chunks and see where they differ
		diffidx := st.getDiffIndex(key)

		// Check if chunks are identical. If so, recurse into
		// the child node. Otherwise, the key has to be split
		// into 1) an optional common prefix, 2) the fullnode
		// representing the two differing path, and 3) a leaf
		// for each of the differentiated subtrees.
		if diffidx == len(st.key) {
			// Ext key and key segment are identical, recurse into
			// the child node.
			st.children[0].insert(key, value)
			return
		}
		// Save the original part. Depending if the break is
		// at the extension's last byte or not, create an
		// intermediate extension or use the extension's child
		// node directly.
		var n *StackTrie
		if diffidx < len(st.key)-1 {
			n = newExt(diffidx+1, st.key, st.children[0], st.db)
		} else {
			// Break on the last byte, no need to insert
			// an extension node: reuse the current node
			n = st.children[0]
		}

		var p *StackTrie
		if diffidx == 0 {
			// the break is on the first byte, so
			// the current node is converted into
			// a branch node.
			st.children[0] = nil
			p = st
			st.nodeType = branchNode
		} else {
			// the common prefix is at least one byte
			// long, insert a new intermediate branch
			// node.
			st.children[0] = NewStackTrie(st.db)
			st.children[0].nodeType = branchNode
			st.children[0].keyOffset = st.keyOffset + diffidx
			p = st.children[0]
		}

		n.val = n.hash()
		n.nodeType = hashedNode
		n.key = nil

		// Create a leaf for the inserted part
		o := newLeaf(st.keyOffset+diffidx+1, key, value, st.db)

		// Insert both child leaves where they belong:
		origIdx := st.key[diffidx]
		newIdx := key[diffidx+st.keyOffset]
		p.children[origIdx] = n
		p.children[newIdx] = o
		st.key = st.key[:diffidx]

	case leafNode: /* Leaf */
		// Compare both key chunks and see where they differ
		diffidx := st.getDiffIndex(key)

		// Overwriting a key isn't supported, which means that
		// the current leaf is expected to be split into 1) an
		// optional extension for the common prefix of these 2
		// keys, 2) a fullnode selecting the path on which the
		// keys differ, and 3) one leaf for the differentiated
		// component of each key.
		if diffidx >= len(st.key) {
			panic("Trying to insert into existing key")
		}

		// Check if the split occurs at the first nibble of the
		// chunk. In that case, no prefix extnode is necessary.
		// Otherwise, create that
		var p *StackTrie
		if diffidx == 0 {
			// Convert current leaf into a branch
			st.nodeType = branchNode
			p = st
			st.children[0] = nil
		} else {
			// Convert current node into an ext,
			// and insert a child branch node.
			st.nodeType = extNode
			st.children[0] = NewStackTrie(st.db)
			st.children[0].nodeType = branchNode
			st.children[0].keyOffset = st.keyOffset + diffidx
			p = st.children[0]
		}

		// Create the two child leaves: the one containing the
		// original value and the one containing the new value
		// The child leave will be hashed directly in order to
		// free up some memory.
		origIdx := st.key[diffidx]
		p.children[origIdx] = newLeaf(diffidx+1, st.key, st.val, st.db)
		p.children[origIdx].convertToHash(p.keyOffset + 1)

		newIdx := key[diffidx+st.keyOffset]
		p.children[newIdx] = newLeaf(p.keyOffset+1, key, value, st.db)

		// Finally, cut off the key part that has been passed
		// over to the children.
		st.key = st.key[:diffidx]
		st.val = nil
	case emptyNode: /* Empty */
		st.nodeType = leafNode
		st.key = key[st.keyOffset:]
		st.val = value
	case hashedNode:
		panic("trying to insert into hash")
	default:
		panic("invalid type")
	}
}

func (st *StackTrie) hash() []byte {
	/* Shortcut if node is already hashed */
	if st.nodeType == hashedNode {
		return st.val
	}

	var preimage bytes.Buffer
	var n node
	d := sha3.NewLegacyKeccak256()
	switch st.nodeType {
	case branchNode:
		fn := &fullNode{}
		payload := [544]byte{}
		pos := 3 // maximum header length given what we know
		for i, v := range st.children {
			if v != nil {
				// Write a 32 byte list to the sponge
				childhash := v.hash()
				if len(childhash) == 32 {
					payload[pos] = 128 + byte(len(childhash))
					pos++
				}
				copy(payload[pos:pos+len(childhash)], childhash)
				pos += len(childhash)
				if len(childhash) < 32 {
					fn.Children[i] = rawNode(childhash)

				} else {
					fn.Children[i] = hashNode(childhash)
				}
				st.children[i] = nil // Reclaim mem from subtree
			} else {
				// Write an empty list to the sponge
				payload[pos] = 0x80
				pos++
			}
		}
		// Add an empty 17th value
		payload[pos] = 0x80
		pos++

		// Compute the header, length size is either 0, 1 or 2 bytes since
		// there are at least 17 empty list headers, and at most 16 hashes
		// plus an empty header for the value.
		var start int
		if pos-3 < 56 {
			payload[2] = 0xc0 + byte(pos-3)
			start = 2
		} else if pos-3 < 256 {
			payload[2] = byte(pos - 3)
			payload[1] = 0xf8
			start = 1
		} else {
			payload[2] = byte(pos - 3)
			payload[1] = byte((pos - 3) >> 8)
			payload[0] = 0xf9
			start = 0
		}

		// Do not hash if the payload length is less than 32 bytes
		if pos-start < 32 {
			// rlp len < 32, will be embedded
			// into its parent.
			return payload[start:pos]
		}
		n = fn
		preimage.Write(payload[start:pos])
	case extNode:
		ch := st.children[0].hash()
		n = &shortNode{
			Key: hexToCompact(st.key),
			Val: hashNode(ch),
		}
		err := rlp.Encode(&preimage, n)
		st.children[0] = nil // Reclaim mem from subtree
		if err != nil {
			panic(err)
		}
		if preimage.Len() < 32 {
			return preimage.Bytes()
		}
	case leafNode:
		n = &shortNode{
			Key: hexToCompact(append(st.key, byte(16))),
			Val: valueNode(st.val),
		}
		err := rlp.Encode(&preimage, n)
		if err != nil {
			panic(err)
		}
		if preimage.Len() < 32 {
			return preimage.Bytes()
		}
	case emptyNode:
		return emptyRoot[:]
	default:
		panic("Invalid node type")
	}
	rlpSize := preimage.Len()
	d.Write(preimage.Bytes())
	ret := d.Sum(nil)

	if st.db != nil {
		if n != nil {
			st.db.insert(common.BytesToHash(ret), rlpSize, n)
			// Commit if the size is above 100M
			if st.db.dirtiesSize > 104857600 {
				if err := st.db.Commit(common.BytesToHash(ret), false); err != nil {
					panic(err)
				}
			}
		}
	}
	return ret
}

// Hash returns the hash of the current node
func (st *StackTrie) Hash() (h common.Hash) {
	st.val = st.hash()
	st.nodeType = hashedNode
	return common.BytesToHash(st.val)
}

// Commit will commit the current node to database db
func (st *StackTrie) Commit(db *Database) common.Hash {
	oldDb := st.db
	st.db = db
	defer func() {
		st.db = oldDb
	}()
	h := common.BytesToHash(st.hash())
	return h
}

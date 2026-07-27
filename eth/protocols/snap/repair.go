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

package snap

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// repairReader is a trie node reader over the raw key-value store, resolving
// nodes purely by path and ignoring the requested hash. It is used by the
// repair pass, where the on-disk trie is mutually inconsistent by design:
// parents may reference stale children on exactly the paths about to be
// rewritten. Every stale reference is on a dirty path, so applying all dirty
// leaf updates through this reader rewrites all of them.
type repairReader struct {
	db ethdb.KeyValueReader
}

// NodeReader implements database.NodeDatabase.
func (r *repairReader) NodeReader(root common.Hash) (database.NodeReader, error) {
	return r, nil
}

// Node implements database.NodeReader, resolving the node by path only.
func (r *repairReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	if owner == (common.Hash{}) {
		return rawdb.ReadAccountTrieNode(r.db, path), nil
	}
	return rawdb.ReadStorageTrieNode(r.db, owner, path), nil
}

// applyNodeSet flushes a committed trie node set into the given batch,
// including the deletions it carries (displaced nodes after structural
// collapses must not linger as dangling entries).
func applyNodeSet(batch ethdb.KeyValueWriter, owner common.Hash, set *trienode.NodeSet) {
	if set == nil {
		return
	}
	set.ForEachWithOrder(func(path string, n *trienode.Node) {
		if n.IsDeleted() {
			rawdb.DeleteTrieNode(batch, owner, []byte(path), n.Hash, rawdb.PathScheme)
		} else {
			rawdb.WriteTrieNode(batch, owner, []byte(path), n.Hash, n.Blob, rawdb.PathScheme)
		}
	})
}

// storageTrieRange returns the key range occupied by an account's storage
// trie nodes.
func storageTrieRange(account common.Hash) (start, limit []byte) {
	start = append(bytes.Clone(rawdb.TrieNodeStoragePrefix), account.Bytes()...)
	return start, increaseKey(bytes.Clone(start))
}

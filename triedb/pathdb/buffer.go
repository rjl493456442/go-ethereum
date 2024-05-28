// Copyright 2022 The go-ethereum Authors
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
	"maps"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// buffer is a collection of modified flat states along with the modified trie
// nodes. They are cached here to aggregate the disk write. The content of the
// buffer must be checked before diving into disk (since it basically is not
// yet written data).
type buffer struct {
	layers uint64                                    // The number of diff layers aggregated inside
	size   uint64                                    // The size of aggregated writes
	limit  uint64                                    // The maximum memory allowance in bytes
	nodes  map[common.Hash]map[string]*trienode.Node // Aggregated node set, mapped by owner and path
	states *stateSet                                 // Aggregated state set
}

// newBuffer initializes the node buffer with the provided flat states and
// a set of trie nodes.
func newBuffer(limit int, nodes map[common.Hash]map[string]*trienode.Node, states *stateSet, layers uint64) *buffer {
	// Don't panic for lazy users if any provided set is nil
	if nodes == nil {
		nodes = make(map[common.Hash]map[string]*trienode.Node)
	}
	if states == nil {
		states = newStates(nil, nil, nil)
	}
	// Compute the total size of held nodes and states
	var size uint64
	for _, subset := range nodes {
		for path, n := range subset {
			size += uint64(len(n.Blob) + len(path))
		}
	}
	size += uint64(states.size)

	return &buffer{
		layers: layers,
		nodes:  nodes,
		size:   size,
		limit:  uint64(limit),
		states: states,
	}
}

// account retrieves the account blob with account address hash.
func (b *buffer) account(hash common.Hash) ([]byte, bool) {
	return b.states.account(hash)
}

// storage retrieves the storage slot with account address hash and slot key.
func (b *buffer) storage(addrHash common.Hash, storageHash common.Hash) ([]byte, bool) {
	return b.states.storage(addrHash, storageHash)
}

// node retrieves the trie node with node path and its trie identifier.
func (b *buffer) node(owner common.Hash, path []byte) (*trienode.Node, bool) {
	subset, ok := b.nodes[owner]
	if !ok {
		return nil, false
	}
	n, ok := subset[string(path)]
	if !ok {
		return nil, false
	}
	return n, true
}

// commitNodes merges the given set of nodes into the buffer.
func (b *buffer) commitNodes(nodes map[common.Hash]map[string]*trienode.Node) int64 {
	var (
		delta     int64   // size difference resulting from node merging
		overwrite counter // counter of nodes being overwritten
	)
	for owner, subset := range nodes {
		current, exist := b.nodes[owner]
		if !exist {
			// Allocate a new map for the subset instead of claiming it directly
			// from the passed map to avoid potential concurrent map read/write.
			// The nodes belong to original diff layer are still accessible even
			// after merging, thus the ownership of nodes map should still belong
			// to original layer and any mutation on it should be prevented.
			for path, n := range subset {
				delta += int64(len(n.Blob) + len(path))
			}
			b.nodes[owner] = maps.Clone(subset)
			continue
		}
		for path, n := range subset {
			if orig, exist := current[path]; !exist {
				delta += int64(len(n.Blob) + len(path))
			} else {
				delta += int64(len(n.Blob) - len(orig.Blob))
				overwrite.add(len(orig.Blob) + len(path))
			}
			current[path] = n
		}
		b.nodes[owner] = current
	}
	overwrite.report(gcTrieNodeMeter, gcTrieNodeBytesMeter)
	return delta
}

// commit merges the provided flat states and dirty trie nodes into the buffer.
// This operation does not take ownership of the passed maps, which belong to
// the bottom-most diff layer. Instead, it holds references to the given maps,
// which are safe to copy.
func (b *buffer) commit(nodes map[common.Hash]map[string]*trienode.Node, states *stateSet) *buffer {
	b.updateSize(b.states.merge(states) + b.commitNodes(nodes))
	b.layers++
	return b
}

// revertNodes merges the given trie nodes into buffer. It should reverse the
// changes made by the most recent state transition.
func (b *buffer) revertNodes(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) int64 {
	var delta int64
	for owner, subset := range nodes {
		current, ok := b.nodes[owner]
		if !ok {
			panic(fmt.Sprintf("non-existent subset (%x)", owner))
		}
		for path, n := range subset {
			orig, ok := current[path]
			if !ok {
				// There is a special case in MPT that one child is removed from
				// a fullNode which only has two children, and then a new child
				// with different position is immediately inserted into the fullNode.
				// In this case, the clean child of the fullNode will also be
				// marked as dirty because of node collapse and expansion.
				//
				// In case of database rollback, don't panic if this "clean"
				// node occurs which is not present in buffer.
				var blob []byte
				if owner == (common.Hash{}) {
					blob = rawdb.ReadAccountTrieNode(db, []byte(path))
				} else {
					blob = rawdb.ReadStorageTrieNode(db, owner, []byte(path))
				}
				// Ignore the clean node in the case described above.
				if bytes.Equal(blob, n.Blob) {
					continue
				}
				panic(fmt.Sprintf("non-existent node (%x %v) blob: %v", owner, path, crypto.Keccak256Hash(n.Blob).Hex()))
			}
			current[path] = n
			delta += int64(len(n.Blob)) - int64(len(orig.Blob))
		}
	}
	return delta
}

// revert is the reverse operation of commit. It also merges the provided flat
// states and trie nodes into the buffer. The key difference is that the provided
// state set should reverse the changes made by the most recent state transition.
func (b *buffer) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) error {
	// Short circuit if no embedded state transition to revert.
	if b.layers == 0 {
		return errStateUnrecoverable
	}
	b.layers--

	// Reset the entire buffer if only a single transition left.
	if b.layers == 0 {
		b.reset()
		return nil
	}
	stateDelta, err := b.states.revert(accounts, storages)
	if err != nil {
		return err
	}
	nodeDelta := b.revertNodes(db, nodes)
	b.updateSize(nodeDelta + stateDelta)
	return nil
}

// updateSize updates the total cache size by the given delta.
func (b *buffer) updateSize(delta int64) {
	size := int64(b.size) + delta
	if size >= 0 {
		b.size = uint64(size)
		return
	}
	s := b.size
	b.size = 0
	log.Error("Buffer size underflow", "size", common.StorageSize(s), "diff", common.StorageSize(delta))
}

// reset cleans up the disk cache.
func (b *buffer) reset() {
	b.layers = 0
	b.size = 0
	b.nodes = make(map[common.Hash]map[string]*trienode.Node)
	b.states.reset()
}

// empty returns an indicator if buffer is empty.
func (b *buffer) empty() bool {
	return b.layers == 0
}

// full returns an indicator if the size of accumulated states exceeds the
// configured threshold.
func (b *buffer) full() bool {
	return b.size > b.limit
}

// allocBatch returns a database batch with pre-allocated buffer.
func (b *buffer) allocBatch(db ethdb.KeyValueStore) ethdb.Batch {
	var metasize int
	for owner, nodes := range b.nodes {
		if owner == (common.Hash{}) {
			metasize += len(nodes) * len(rawdb.TrieNodeAccountPrefix) // database key prefix
		} else {
			metasize += len(nodes) * (len(rawdb.TrieNodeStoragePrefix) + common.HashLength) // database key prefix + owner
		}
	}
	// count the state metasize
	metasize += (len(b.states.destructSet) + len(b.states.accountData)) * len(rawdb.SnapshotAccountPrefix)
	for _, slots := range b.states.storageData {
		metasize += len(slots) * len(rawdb.SnapshotStoragePrefix)
	}
	return db.NewBatchWithSize((metasize + int(b.size)) * 11 / 10) // extra 10% for potential pebble internal stuff
}

// flush persists the in-memory dirty trie node into the disk if the configured
// memory threshold is reached. Note, all data must be written atomically.
func (b *buffer) flush(root common.Hash, db ethdb.KeyValueStore, progress []byte, clean *fastcache.Cache, id uint64) error {
	// Ensure the target state id is aligned with the internal counter.
	head := rawdb.ReadPersistentStateID(db)
	if head+b.layers != id {
		return fmt.Errorf("buffer layers (%d) cannot be applied on top of persisted state id (%d) to reach requested state id (%d)", b.layers, head, id)
	}
	// Terminate the state snapshot generation if it's active
	var (
		start = time.Now()
		batch = b.allocBatch(db)
	)
	nodes := writeNodes(batch, b.nodes, clean)
	accounts, slots := b.states.write(db, batch, progress)
	rawdb.WritePersistentStateID(batch, id)
	rawdb.WriteSnapshotRoot(batch, root)

	// Flush all mutations in a single batch
	size := batch.ValueSize()
	if err := batch.Write(); err != nil {
		return err
	}
	commitBytesMeter.Mark(int64(size))
	commitNodesMeter.Mark(int64(nodes))
	commitAccountsMeter.Mark(int64(accounts))
	commitStoragesMeter.Mark(int64(slots))
	commitTimeTimer.UpdateSince(start)
	b.reset()
	log.Debug("Persisted buffer content", "nodes", nodes, "accounts", accounts, "slots", slots, "bytes", common.StorageSize(size), "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// cacheKey constructs the unique key of clean cache.
func cacheKey(owner common.Hash, path []byte) []byte {
	if owner == (common.Hash{}) {
		return path
	}
	return append(owner.Bytes(), path...)
}

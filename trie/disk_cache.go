// Copyright 2021 The go-ethereum Authors
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
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var (
	// dirtyMemoryLimit is the maximum size of a single dirty node set that
	// aggregates the writes from above until it's flushed into the disk.
	dirtyMemoryLimit = uint64(256 * 1024 * 1024)
)

// dirtyCache is a collection of uncommitted dirty nodes to aggregate the disk
// write. And it can act as an additional cache avoid hitting disk too much.
type dirtyCache struct {
	db    ethdb.Database
	nodes map[string]*cachedNode // Uncommitted dirty nodes, indexed by storage key
	size  uint64                 // The approximate size of cached nodes

	// Frozen node set
	frozenCommitted chan struct{}          // Channel used to send signal when the frozen set is flushed
	frozenLock      sync.RWMutex           // Lock used to protect frozen list
	frozen          map[string]*cachedNode // Frozen dirty node set, waiting for flushing
}

// newDirtyCache initializes the dirty node cache with the given nodes.
func newDirtyCache(db ethdb.Database, nodes map[string]*cachedNode) *dirtyCache {
	if nodes == nil {
		nodes = make(map[string]*cachedNode)
	}
	var size uint64
	for key, node := range nodes {
		size += uint64(len(key) + int(node.size) + cachedNodeSize)
	}
	return &dirtyCache{
		db:              db,
		nodes:           nodes,
		size:            size,
		frozenCommitted: make(chan struct{}),
	}
}

// node retrieves the node with given path and hash.
func (cache *dirtyCache) node(storage []byte, hash common.Hash) (node, error) {
	for _, set := range cache.dirtySets() {
		n, ok := set[string(storage)]
		if ok {
			if n.node == nil || n.hash != hash {
				owner, path := DecodeStorageKey(storage)
				return nil, fmt.Errorf("%w %x(%x %v)", errUnexpectedNode, hash, owner, path)
			}
			triedbDirtyHitMeter.Mark(1)
			triedbDirtyNodeHitDepthHist.Update(int64(128))
			triedbDirtyReadMeter.Mark(int64(n.size))
			return n.obj(hash), nil
		}
	}
	return nil, nil
}

// nodeBlob retrieves the node blob with given path and hash.
func (cache *dirtyCache) nodeBlob(storage []byte, hash common.Hash) ([]byte, error) {
	for _, set := range cache.dirtySets() {
		n, ok := set[string(storage)]
		if ok {
			if n.node == nil || n.hash != hash {
				owner, path := DecodeStorageKey(storage)
				return nil, fmt.Errorf("%w %x(%x %v)", errUnexpectedNode, hash, owner, path)
			}
			triedbDirtyHitMeter.Mark(1)
			triedbDirtyNodeHitDepthHist.Update(int64(128))
			triedbDirtyReadMeter.Mark(int64(n.size))
			return n.rlp(), nil
		}
	}
	return nil, nil
}

// nodeBlobByPath retrieves the node blob with given path regardless of the node hash.
func (cache *dirtyCache) nodeBlobByPath(storage []byte) ([]byte, bool) {
	for _, set := range cache.dirtySets() {
		n, ok := set[string(storage)]
		if ok {
			if n.node == nil {
				return nil, true
			}
			return n.rlp(), true
		}
	}
	return nil, false
}

// update merges the given nodes into the cache. This function should never be called
// simultaneously with other map accessors.
func (cache *dirtyCache) update(nodes map[string]*cachedNode) *dirtyCache {
	var diff int64
	for storage, n := range nodes {
		if prev, exist := cache.nodes[storage]; exist {
			diff += int64(n.size) - int64(prev.size)
			triedbDiskCacheHitMeter.Mark(1)
		} else {
			diff += int64(int(n.size) + len(storage) + cachedNodeSize)
			triedbDiskCacheMissMeter.Mark(1)
		}
		cache.nodes[storage] = n
	}
	if final := int64(cache.size) + diff; final < 0 {
		log.Error("Negative dirty cache size", "previous", cache.size, "diff", diff)
		cache.size = 0
	} else {
		cache.size = uint64(final)
	}
	return cache
}

// revert applies the reverse diff to the local dirty node set. This function
// should never be called simultaneously with other map accessors.
func (cache *dirtyCache) revert(diff *reverseDiff) error {
	for _, state := range diff.States {
		_, ok := cache.nodes[string(state.Key)]
		if !ok {
			owner, path := DecodeStorageKey(state.Key)
			return fmt.Errorf("non-existent node (%x %v)", owner, path)
		}
		if len(state.Val) == 0 {
			cache.nodes[string(state.Key)] = &cachedNode{
				node: nil,
				size: 0,
				hash: common.Hash{},
			}
		} else {
			cache.nodes[string(state.Key)] = &cachedNode{
				node: rawNode(state.Val),
				size: uint16(len(state.Val)),
				hash: crypto.Keccak256Hash(state.Val),
			}
		}
	}
	return nil
}

// flush persists the in-memory dirty trie node into the disk if the predefined
// memory threshold is reached. Depends on the given config, the additional legacy
// format node can be written as well (e.g. for archive node). Note, all data must
// be written to disk atomically.
// This function should never be called simultaneously with other map accessors.
func (cache *dirtyCache) flush(clean *fastcache.Cache, config *Config, force bool, sync bool) error {
	if cache.size <= dirtyMemoryLimit && !force {
		return nil
	}
	cache.waitCommit()
	cache.addFrozen(cache.nodes)

	// Reset the local dirty node set
	cache.nodes = make(map[string]*cachedNode)
	cache.size = 0
	cache.frozenCommitted = make(chan struct{})

	go cache.flushFrozen(clean, config)
	if sync {
		cache.waitCommit()
	}
	return nil
}

func (cache *dirtyCache) addFrozen(frozen map[string]*cachedNode) {
	cache.frozenLock.Lock()
	defer cache.frozenLock.Unlock()

	cache.frozen = frozen
}

// waitCommit blocks until the frozen dirty set is flushed into the disk,
// or return directly if there is no frozen set waiting for flushing.
func (cache *dirtyCache) waitCommit() {
	cache.frozenLock.RLock()
	frozen := cache.frozen
	cache.frozenLock.RUnlock()

	if frozen == nil {
		return
	}
	<-cache.frozenCommitted
}

func (cache *dirtyCache) dirtySets() []map[string]*cachedNode {
	cache.frozenLock.RLock()
	defer cache.frozenLock.RUnlock()

	lookup := []map[string]*cachedNode{cache.nodes}
	if cache.frozen != nil {
		lookup = append(lookup, cache.frozen)
	}
	return lookup
}

func (cache *dirtyCache) flushFrozen(clean *fastcache.Cache, config *Config) {
	var (
		total int64
		start = time.Now()
		batch = cache.db.NewBatch()

		encodeTime time.Duration
		batchTime  time.Duration
		flushTime  time.Duration
	)
	for storage, n := range cache.frozen {
		if n.node == nil {
			rawdb.DeleteTrieNode(batch, []byte(storage))
			continue
		}
		t := time.Now()
		blob := n.rlp()
		encodeTime += time.Since(t)

		t = time.Now()
		rawdb.WriteTrieNode(batch, []byte(storage), blob)
		if config != nil && config.WriteLegacy {
			rawdb.WriteArchiveTrieNode(batch, n.hash, blob)
		}
		batchTime += time.Since(t)
		if clean != nil {
			clean.Set(EncodeInternalKey([]byte(storage), n.hash), blob)
		}
		total += int64(len(blob) + len(storage))
	}
	triedbCommitSizeMeter.Mark(total)
	triedbCommitNodesMeter.Mark(int64(len(cache.nodes)))
	triedbCommitTimeTimer.UpdateSince(start)

	t := time.Now()
	if err := batch.Write(); err != nil {
		panic(fmt.Sprintf("failed to write %v", err))
	}
	flushTime = time.Since(t)

	log.Debug("Persisted uncommitted nodes",
		"nodes", len(cache.nodes),
		"size", common.StorageSize(total),
		"encode-time", common.PrettyDuration(encodeTime),
		"batch-time", common.PrettyDuration(batchTime),
		"flush-time", common.PrettyDuration(flushTime),
		"elapsed", common.PrettyDuration(time.Since(start)),
	)
	cache.frozenLock.Lock()
	cache.frozen = nil
	cache.frozenLock.Unlock()

	close(cache.frozenCommitted) // fire the signal
}

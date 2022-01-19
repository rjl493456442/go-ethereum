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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package trie

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	reverseDiffVersion = uint64(0) // Initial version of reverse diff structure
)

// stateDiff represents a reverse change of a state data. The value refers to the
// content before the change is applied.
type stateDiff struct {
	Key []byte // Storage format node key
	Val []byte // RLP-encoded node blob, nil means the node is previously non-existent
}

// reverseDiff represents a set of state diffs belong to the same block. All the
// reverse-diffs in disk are linked with each other by a unique id(8byte integer),
// the tail(oldest) reverse-diff will be pruned in order to control the storage size.
type reverseDiff struct {
	Version uint64      // The version tag of stored reverse diff
	Parent  common.Hash // The corresponding state root of parent block
	Root    common.Hash // The corresponding state root which these diffs belong to
	States  []stateDiff // The list of state changes
}

// loadReverseDiff reads and decodes the reverse diff by the given id.
func loadReverseDiff(db ethdb.Database, id uint64) (*reverseDiff, error) {
	blob := rawdb.ReadReverseDiff(db, id)
	if len(blob) == 0 {
		return nil, errors.New("reverse diff not found")
	}
	var diff reverseDiff
	if err := rlp.DecodeBytes(blob, &diff); err != nil {
		return nil, err
	}
	if diff.Version != reverseDiffVersion {
		return nil, fmt.Errorf("%w want %d got %d", errors.New("unexpected reverse diff version"), reverseDiffVersion, diff.Version)
	}
	return &diff, nil
}

// storeReverseDiff generates the reverse state diff for the passed bottom-most
// diff layer. After storing the corresponding reverse diffs, it will also prune
// the stale reverse diffs from the disk by the given limit.
// This function will panic if it's called for non-bottom-most diff layer.
func storeReverseDiff(dl *diffLayer, limit uint64) error {
	var (
		startTime = time.Now()
		base      = dl.Parent().(*diskLayer)
		states    []stateDiff
	)
	for key := range dl.nodes {
		// Read the previous value stored in the disk. Note that here we expect
		// to get a node with a different hash, thus no need to compare hash here.
		//
		// It's possible the previous node is a legacy node, so no blob can be
		// found with the new scheme. It's OK to use the empty previous value
		// here since the legacy node can always be found.
		pre, err := base.nodeBlobByPath([]byte(key))
		if err != nil {
			return err
		}
		states = append(states, stateDiff{
			Key: []byte(key),
			Val: pre,
		})
	}
	diff := &reverseDiff{
		Version: reverseDiffVersion,
		Parent:  base.root,
		Root:    dl.root,
		States:  states,
	}
	blob, err := rlp.EncodeToBytes(diff)
	if err != nil {
		return err
	}
	// The reverse diff object and the lookup are stored in two different
	// places, so there is no atomicity guarantee. It's possible that reverse
	// diff object is written but lookup is not, vice versa. So double-check
	// the presence when using the reverse diff.
	rawdb.WriteReverseDiff(base.diskdb, dl.rid, blob, base.root)
	rawdb.WriteReverseDiffLookup(base.diskdb, base.root, dl.rid)
	triedbReverseDiffSizeMeter.Mark(int64(len(blob)))
	triedbReverseDiffCountMeter.Mark(int64(len(dl.nodes)))

	// Prune stale reverse diffs if necessary
	pruned, err := truncateFromTail(base.diskdb, dl.rid, limit)
	if err != nil {
		return err
	}
	duration := time.Since(startTime)
	triedbReverseDiffTimeTimer.Update(duration)

	logCtx := []interface{}{
		"id", dl.rid,
		"nodes", len(dl.nodes),
		"size", common.StorageSize(len(blob)),
	}
	if pruned != 0 {
		logCtx = append(logCtx, "pruned", pruned)
	}
	logCtx = append(logCtx, "elapsed", common.PrettyDuration(duration))
	log.Debug("Stored the reverse diff", logCtx...)
	return nil
}

// truncateFromTail removes the extra reverse diff from the tail with the
// given parameters. If the passed database is a non-freezer database, do
// nothing here.
func truncateFromTail(db ethdb.Database, head uint64, limit uint64) (int, error) {
	if head <= limit {
		return 0, nil
	}
	oldTail, err := db.Tail(rawdb.ReverseDiffFreezer)
	if err != nil {
		return 0, nil // It's non-freezer database, skip it
	}
	var (
		batch   = db.NewBatch()
		newTail = head - limit
	)
	for i := oldTail; i < newTail; i++ {
		// The reverse diff id is added by 1 here. Because reverse diff is
		// encoded from 1 in this package while in the freezer it's encoded
		// from 0.
		hash := rawdb.ReadReverseDiffHash(db, i+1)
		if hash != (common.Hash{}) {
			rawdb.DeleteReverseDiffLookup(batch, hash)
		}
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	if err := db.TruncateTail(rawdb.ReverseDiffFreezer, newTail); err != nil {
		return 0, err
	}
	return int(newTail - oldTail), nil
}

// truncateFromHead applies the head truncation with the given parameter.
// Hold the fact that the reverse diff history can already be truncated
// from the tail, which means the lowest available head will be the current
// tail. So always return the new head after the truncation.
func truncateFromHead(db ethdb.Database, items uint64) uint64 {
	origin, err := db.Ancients(rawdb.ReverseDiffFreezer)
	if err != nil {
		return 0 // ancient store is not supported
	}
	db.TruncateHead(rawdb.ReverseDiffFreezer, items)

	truncated, _ := db.Ancients(rawdb.ReverseDiffFreezer)
	log.Debug("Truncated reverse diff history", "request", items, "truncated", truncated, "origin", origin)
	return truncated
}

// repairReverseDiff is called when database is constructed. It ensures reverse diff
// history is aligned with disk layer, or truncate the extra diffs from the freezer.
// The id of disk layer will be returned as well.
func repairReverseDiff(db ethdb.Database, diskroot common.Hash) uint64 {
	var (
		items, _ = db.Ancients(rawdb.ReverseDiffFreezer)
		tail, _  = db.Tail(rawdb.ReverseDiffFreezer)
	)
	// Start from the head of the reverse diffs, compare the hash one by one
	// and drop the unmatched one directly.
	for current := items; current > tail; current -= 1 {
		rdiff, err := loadReverseDiff(db, current)
		if err != nil {
			continue
		}
		if rdiff.Root == diskroot {
			return current
		}
		db.TruncateHead(rawdb.ReverseDiffFreezer, current-1)
	}
	return tail
}

// loadDiffHead loads the reverse diff id for the disk layer with the given state root.
func loadDiffHead(config *Config, db ethdb.Database, root common.Hash) uint64 {
	var head uint64
	if config != nil && config.ReadOnly {
		// The database is opened in read-only mode, skip the
		// reverse checking and return the head reverse diff
		// id directly.
		head, _ = db.Ancients(rawdb.ReverseDiffFreezer)
		return head
	} else {
		head = repairReverseDiff(db, root)
	}
	return head
}

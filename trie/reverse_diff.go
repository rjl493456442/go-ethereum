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
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// stateDiff represents a reverse change of a state data. The value refers to the
// content before the change is applied.
type stateDiff struct {
	Key []byte // Storage format node key
	Val []byte // RLP-encoded node blob, nil means the node is previously non-existent
}

// reverseDiff represents a set of state diffs belong to the same block. All the
// reverse-diffs in disk are linked with each other by a unique id(8byte integer),
// the head reverse-diff will be pruned in order to control the storage size.
type reverseDiff struct {
	Parent common.Hash // The corresponding state root of parent block
	Root   common.Hash // The corresponding state root which these diffs belong to
	States []stateDiff // The list of state changes
}

// loadReverseDiff reads and decodes the reverse diff by the given id.
func loadReverseDiff(db ethdb.KeyValueReader, id uint64) (*reverseDiff, error) {
	blob := rawdb.ReadReverseDiff(db, id)
	if len(blob) == 0 {
		return nil, errors.New("reverse diff not found")
	}
	var diff reverseDiff
	if err := rlp.DecodeBytes(blob, &diff); err != nil {
		return nil, err
	}
	return &diff, nil
}

// storeReverseDiff extracts the reverse state diff by the passed bottom-most
// diff layer and its parent.
// This function will panic if it's called for non-bottom-most diff layer.
func storeReverseDiff(dl *diffLayer) error {
	var (
		startTime = time.Now()
		base      = dl.parent.(*diskLayer)
		states    []stateDiff
		batch     = base.diskdb.NewBatch()
	)
	for key := range dl.nodes {
		pre, _ := rawdb.ReadTrieNode(base.diskdb, []byte(key))
		states = append(states, stateDiff{
			Key: []byte(key),
			Val: pre,
		})
	}
	diff := &reverseDiff{
		Parent: base.root,
		Root:   dl.root,
		States: states,
	}
	blob, err := rlp.EncodeToBytes(diff)
	if err != nil {
		return err
	}
	rawdb.WriteReverseDiff(batch, dl.rid, blob)
	rawdb.WriteReverseDiffLookup(batch, base.root, dl.rid)
	if err := batch.Write(); err != nil {
		return err
	}
	batch.Reset()
	triedbReverseDiffSizeMeter.Mark(int64(len(blob)))

	duration := time.Since(startTime)
	triedbReverseDiffTimeTimer.Update(duration)
	log.Debug("Stored the reverse diff", "id", dl.rid, "elapsed", common.PrettyDuration(duration))
	return nil

}

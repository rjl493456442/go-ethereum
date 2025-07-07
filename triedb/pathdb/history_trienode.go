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
	"encoding/binary"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// Header section:
//
//      + the number of tries           + the offset within the data slice
//     /                               /
//    +--------------------+---------------+-----------------+---------------+--------------+-----------------+
//    |  counter (4 bytes) |  trie owner 1 | offset(4 bytes) |       ...     | trie owner n | offset(4 bytes) |
//    +--------------------+---------------+-----------------+---------------+--------------------------------+
//
//
// Data section:
//
//      + restart point                 + restart point (depends on restart interval)
//     /                               /
//    +---------------+---------------+---------------+---------------+---------+
//    |  node entry 1 |  node entry 2 |      ...      |  node entry n | trailer |
//    +---------------+---------------+---------------+---------------+---------+
//     \                             /
//      +---------  block ----------+
//
//                 node entry
//
//              +---- key len ----+
//             /                   \
//    +-------+---------+-----------+---------+--------------------+--------------+----------------+
//    | shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
//    +-----------------+---------------------+--------------------+--------------+----------------+
//
// Block trailer:
//
//      +-- 4-bytes --+
//     /               \
//    +-----------------+-----------------+-----------------+------------------------------+
//    | restart point 1 |       ....      | restart point n | restart points len (4-bytes) |
//    +-----------------+-----------------+-----------------+------------------------------+
//
//
// NOTE: All fixed-length integer are big-endian.

const (
	trienodeHistoryV0           = uint8(0)              // initial version of node history structure
	trienodeHistoryVersion      = trienodeHistoryV0     // the default node history version
	trienodeHistoryHeaderSize   = 4 + common.HashLength // the size of a single header in node history
	trienodeDataBlockRestartLen = 16                    // The restart interval length of trie node block
)

// trienodeHistory represents a trienode diff corresponding a state transition.
type trienodeHistory struct {
	nodes    map[common.Hash]map[string][]byte // Set of original value of trie nodes before state transition
	owners   []common.Hash                     // List of trie identifier sorted lexicographically
	nodeList map[common.Hash][]string          // Set of node paths  sorted lexicographically
}

// newTrienodeHistory constructs a trienode history with the provided trienodes
func newTrienodeHistory(nodes map[common.Hash]map[string][]byte) *trienodeHistory {
	nodeList := make(map[common.Hash][]string)
	for owner, subset := range nodes {
		keys := sort.StringSlice(slices.Collect(maps.Keys(subset)))
		keys.Sort()
		nodeList[owner] = keys
	}
	return &trienodeHistory{
		nodes:    nodes,
		owners:   slices.SortedFunc(maps.Keys(nodes), common.Hash.Cmp),
		nodeList: nodeList,
	}
}

// sharedLen returns the length of the common prefix shared by a and b.
func sharedLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// typ implements the history interface, returning the historical data type held.
func (h *trienodeHistory) typ() historyType {
	return typeTrienodeHistory
}

// forEach implements the history interface, returning an iterator to traverse the
// state entries in the history.
func (h *trienodeHistory) forEach() iter.Seq[stateIdent] {
	return func(yield func(stateIdent) bool) {
		for _, owner := range h.owners {
			for _, path := range h.nodeList[owner] {
				if !yield(newTrienodeIdent(owner, path)) {
					return
				}
			}
		}
	}
}

func (h *trienodeHistory) encode() ([]byte, error) {
	var (
		headerSection bytes.Buffer
		dataSection   bytes.Buffer
		scratch       = make([]byte, 64)
	)
	binary.Write(&headerSection, binary.BigEndian, trienodeHistoryVersion) // 1 byte
	binary.Write(&headerSection, binary.BigEndian, uint32(len(h.owners)))  // 4 bytes
	for _, owner := range h.owners {
		// Write the header of the trie in the header section
		headerSection.Write(owner.Bytes())                                        // 32 bytes
		binary.Write(&headerSection, binary.BigEndian, uint32(dataSection.Len())) // 4 bytes

		// Write the trie data in the data section
		var (
			prevKey   []byte
			restarts  []uint32
			prefixLen int
		)
		for i, p := range h.nodeList[owner] {
			key := []byte(p)
			if i%trienodeDataBlockRestartLen == 0 {
				restarts = append(restarts, uint32(dataSection.Len()))
				prefixLen = 0
			} else {
				prefixLen = sharedLen(prevKey, key)
			}
			value := h.nodes[owner][p]

			n := binary.PutUvarint(scratch[0:], uint64(prefixLen))
			n += binary.PutUvarint(scratch[n:], uint64(len(key)-prefixLen))
			n += binary.PutUvarint(scratch[n:], uint64(len(value)))
			if _, err := dataSection.Write(scratch[:n]); err != nil {
				return nil, err
			}
			if _, err := dataSection.Write(key[prefixLen:]); err != nil {
				return nil, err
			}
			if _, err := dataSection.Write(value); err != nil {
				return nil, err
			}
			prevKey = key
		}

		// Encode trailer
		var trailer []byte
		for _, number := range append(restarts, uint32(len(restarts))) {
			binary.BigEndian.PutUint32(scratch[:4], number)
			trailer = append(trailer, scratch[:4]...)
		}
		if _, err := dataSection.Write(trailer); err != nil {
			return nil, err
		}
	}
	return append(headerSection.Bytes(), dataSection.Bytes()...), nil
}

func (h *trienodeHistory) decode(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("trienode history is too small, size: %d", len(data))
	}
	version := data[0]
	if version != trienodeHistoryVersion {
		return fmt.Errorf("unregonized trienode history version: %d", version)
	}
	count := binary.BigEndian.Uint32(data[1:5])
	if len(data) < 5+trienodeHistoryHeaderSize*int(count) {
		return fmt.Errorf("truncated trienode history data, size %d, count: %d", len(data), count)
	}
	// Decode headers (trie id and the associated offset)
	var (
		prev    common.Hash
		owners  = make([]common.Hash, 0, count)
		offsets = make([]uint32, 0, count)
	)
	for i := 0; i < int(count); i++ {
		n := 5 + trienodeHistoryHeaderSize*i
		owner := common.BytesToHash(data[n : n+common.HashLength])
		if i != 0 && bytes.Compare(owner.Bytes(), prev.Bytes()) <= 0 {
			return fmt.Errorf("trienode owners are out of order, prev: %v, cur: %v", prev, owner)
		}
		owners = append(owners, owner)
		offsets = append(offsets, binary.BigEndian.Uint32(data[n+common.HashLength:n+common.HashLength+4]))
	}
	data = data[5+trienodeHistoryHeaderSize*int(count):]

	// Decode the trie data respectively
	var (
		nodes    = make(map[common.Hash]map[string][]byte)
		nodeList = make(map[common.Hash][]string)
	)
	for i := 0; i < len(owners); i++ {
		start, limit := int(offsets[i]), len(data)
		if i != len(owners)-1 {
			limit = int(offsets[i+1])
		}
		var (
			paths    []string
			subset   = make(map[string][]byte)
			prevKey  []byte
			items    int
			restarts []uint32
		)
		// Decode restarts
		nRestarts := binary.BigEndian.Uint32(data[limit-4 : limit])
		for i := 0; i < int(nRestarts); i++ {
			o := limit - 4*(int(nRestarts)-i+1)
			restarts = append(restarts, binary.BigEndian.Uint32(data[o:o+4]))
		}
		limit = limit - (int(nRestarts)+1)*4

		// Decode data
		for off := start; off < limit; {
			nShared, nn := binary.Uvarint(data[off:])
			off += nn
			nUnshared, nn := binary.Uvarint(data[off:])
			off += nn
			nValue, nn := binary.Uvarint(data[off:])
			off += nn

			// resolve unshared key
			unsharedKey := data[off : off+int(nUnshared)]
			off += int(nUnshared)

			// resolve value
			value := data[off : off+int(nValue)]
			off += int(nValue)

			// assemble the full key
			var key []byte
			if items%trienodeDataBlockRestartLen == 0 {
				key = unsharedKey
			} else {
				key = append([]byte{}, prevKey[:nShared]...)
				key = append(key, unsharedKey...)
			}
			if items != 0 && bytes.Compare(prevKey, key) >= 0 {
				return fmt.Errorf("trienode paths are out of order, prev: %v, cur: %v", prevKey, key)
			}
			prevKey = key
			items++

			path := string(key)
			subset[path] = value
			paths = append(paths, path)
		}
		nodes[owners[i]] = subset
		nodeList[owners[i]] = paths
	}
	h.owners = owners
	h.nodeList = nodeList
	h.nodes = nodes
	return nil
}

// writeTrienodeHistory persists the trienode history associated with the given
// diff layer.
func writeTrienodeHistory(writer ethdb.AncientWriter, dl *diffLayer) error {
	start := time.Now()
	combined := maps.Clone(dl.nodes.storageNodeOrigin)
	combined[common.Hash{}] = dl.nodes.accountNodeOrigin

	history := newTrienodeHistory(combined)
	data, err := history.encode()
	if err != nil {
		return err
	}
	// Write history data into five freezer table respectively.
	if err := rawdb.WriteTrienodeHistory(writer, dl.stateID(), data); err != nil {
		return err
	}
	trienodeHistoryDataBytesMeter.Mark(int64(len(data)))
	trienodeHistoryBuildTimeMeter.UpdateSince(start)
	log.Debug("Stored trienode history", "id", dl.stateID(), "block", dl.block, "data", common.StorageSize(len(data)), "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

func readTrienodeHistory(reader ethdb.AncientReader, id uint64) (*trienodeHistory, error) {
	data, err := rawdb.ReadTrienodeHistory(reader, id)
	if err != nil {
		return nil, err
	}
	var h trienodeHistory
	if err := h.decode(data); err != nil {
		return nil, err
	}
	return &h, nil
}

func readTrienodeHistories(reader ethdb.AncientReader, start uint64, count uint64) ([]history, error) {
	list, err := rawdb.ReadTrienodeHistoryList(reader, start, count)
	if err != nil {
		return nil, err
	}
	var res []history
	for _, data := range list {
		var h trienodeHistory
		if err := h.decode(data); err != nil {
			return nil, err
		}
		res = append(res, &h)
	}
	return res, nil
}

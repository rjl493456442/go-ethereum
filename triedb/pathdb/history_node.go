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
	"maps"
	"slices"
	"sort"

	"github.com/ethereum/go-ethereum/common"
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
	nodeHistoryV0           = uint8(0)              // initial version of node history structure
	nodeHistoryVersion      = nodeHistoryV0         // the default node history version
	nodeHistoryHeaderSize   = 4 + common.HashLength // the size of a single header in node history
	nodeDataBlockRestartLen = 16                    // The restart interval length of trie node block
)

type nodeHistory struct {
	nodes     map[common.Hash]map[string][]byte
	ownerList []common.Hash
	nodeList  map[common.Hash][]string
}

func newNodeHistory(nodes map[common.Hash]map[string][]byte) *nodeHistory {
	nodeList := make(map[common.Hash][]string)
	for owner, subset := range nodes {
		keys := sort.StringSlice(slices.Collect(maps.Keys(subset)))
		keys.Sort()
		nodeList[owner] = keys
	}
	return &nodeHistory{
		nodes:     nodes,
		ownerList: slices.SortedFunc(maps.Keys(nodes), common.Hash.Cmp),
		nodeList:  nodeList,
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

func (h *nodeHistory) encode() ([]byte, error) {
	var (
		headerSection bytes.Buffer
		dataSection   bytes.Buffer
		scratch       = make([]byte, 64)
	)
	binary.Write(&headerSection, binary.BigEndian, nodeHistoryVersion)       // 1 byte
	binary.Write(&headerSection, binary.BigEndian, uint32(len(h.ownerList))) // 4 bytes
	for _, owner := range h.ownerList {
		// Write the header of the trie in the header section
		headerSection.Write(owner.Bytes())                                        // 32 bytes
		binary.Write(&headerSection, binary.BigEndian, uint32(dataSection.Len())) // 4 bytes

		// Write the trie data in the data section
		var (
			prevKey   []byte
			curKey    []byte
			restarts  []uint32
			prefixLen int
		)
		for i, p := range h.nodeList[owner] {
			key := []byte(p)
			if i%nodeDataBlockRestartLen == 0 {
				restarts = append(restarts, uint32(dataSection.Len()))
				prefixLen = 0
				curKey = key
			} else {
				prefixLen = sharedLen(prevKey, curKey)
				curKey = key[prefixLen:]
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
		for _, number := range restarts {
			binary.BigEndian.PutUint32(scratch[:4], number)
			trailer = append(trailer, scratch[:4]...)
		}
		if _, err := dataSection.Write(trailer); err != nil {
			return nil, err
		}
	}
	return append(headerSection.Bytes(), dataSection.Bytes()...), nil
}

func (h *nodeHistory) decode(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("node history is too small, size: %d", len(data))
	}
	version := data[0]
	if version != nodeHistoryVersion {
		return fmt.Errorf("unregonized node history version: %d", version)
	}
	count := binary.BigEndian.Uint32(data[1:5])
	if len(data) < 5+nodeHistoryHeaderSize*int(count) {
		return fmt.Errorf("truncated node history data, size %d, count: %d", len(data), count)
	}
	// Decode headers (trie id and the associated offset)
	ownerList := make([]common.Hash, 0, count)
	offsets := make([]uint32, 0, count)
	for i := 0; i < int(count); i++ {
		n := 5 + nodeHistoryHeaderSize*i
		ownerList = append(ownerList, common.BytesToHash(data[n:n+common.HashLength]))
		offsets = append(offsets, binary.BigEndian.Uint32(data[n+common.HashLength:n+common.HashLength+4]))
	}
	// Decode the trie data respectively
	var (
		nodes    = make(map[common.Hash]map[string][]byte)
		nodeList = make(map[common.Hash][]string)
	)
	for i := 0; i < len(ownerList); i++ {
		start, limit := int(offsets[i]), len(data)
		if i != len(ownerList)-1 {
			limit = int(offsets[i+1])
		}
		var (
			paths   []string
			subset  = make(map[string][]byte)
			prevKey []byte
			items   int
		)
		for off := start; off < limit; {
			sharedLen, n := binary.Uvarint(data[off:])
			unsharedLen, size := binary.Uvarint(data[off+n:])
			n += size
			valLen, size := binary.Uvarint(data[off+n:])
			n += size

			// Resolve unshared key
			unsharedKey := data[off+n : off+n+int(unsharedLen)]
			n += int(unsharedLen)

			// Resolve data
			value := data[off+n : off+n+int(valLen)]

			var key []byte
			if items%nodeDataBlockRestartLen == 0 {
				key = unsharedKey
			} else {
				key = append(prevKey[:sharedLen], unsharedKey...)
			}
			if bytes.Compare(prevKey, key) >= 0 {
				return fmt.Errorf("node paths are out of order, prev: %v, cur: %v", prevKey, key)
			}
			prevKey = key

			path := string(key)
			subset[path] = value
			paths = append(paths, path)
		}
		nodes[ownerList[i]] = subset
		nodeList[ownerList[i]] = paths
	}
	h.ownerList = ownerList
	h.nodeList = nodeList
	h.nodes = nodes
	return nil
}

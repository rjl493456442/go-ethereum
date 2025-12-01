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
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"slices"
	"strings"
)

// sharedLen returns the length of the common prefix shared by a and b.
func sharedLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// findLeafPaths scans a lexicographically sorted list of paths and returns
// the subset of paths that represent leaves.
//
// A path is considered a leaf if:
//   - it is the last element in the list, or
//   - the next path does not have the current path as its prefix.
//
// In other words, a leaf is a path that has no children extending it.
//
// Example:
//
//	Input:  ["a", "ab", "abc", "b", "ba"]
//	Output: ["abc", "ba"]
//
// The input must be sorted; otherwise the result is undefined.
func findLeafPaths(paths []string) []string {
	var leaves []string
	for i := 0; i < len(paths); i++ {
		if i == len(paths)-1 || !strings.HasPrefix(paths[i+1], paths[i]) {
			leaves = append(leaves, paths[i])
		}
	}
	return leaves
}

// hexPathNodeID computes a numeric node ID from the given path. The path is
// interpreted as a sequence of base-16 digits, where each byte of the input
// is treated as one hexadecimal digit in a big-endian number.
//
// The resulting node ID is constructed as:
//
//	ID = 1 + 16 + 16^2 + ... + 16^(n-1) + value
//
// where n is the number of bytes in the path, and `value` is the base-16
// interpretation of the byte sequence.
//
// The offset (1 + 16 + 16^2 + ... + 16^(n-1)) ensures that all IDs of shorter
// paths occupy a lower numeric range, preserving lexicographic ordering between
// differently-length paths.
//
// The numeric node ID is represented by the uint16 with the assumption the length
// of path won't be greater than 3.
func hexPathNodeID(path string) uint16 {
	var (
		offset = uint16(0)
		pow    = uint16(1)
		value  = uint16(0)
		bytes  = []byte(path)
	)
	for i := 0; i < len(bytes); i++ {
		offset += pow
		pow *= 16
	}
	for i := 0; i < len(bytes); i++ {
		value = value*16 + uint16(bytes[i])
	}
	return offset + value
}

// bitmapSize computes the number of bytes required for the marker bitmap
// corresponding to the remaining portion of a path after a cut point.
// The marker is a bitmap where each bit represents the presence of a
// possible element in the remaining path segment.
func bitmapSize(levels int) int {
	// Compute: total = 1 + 16 + 16^2 + ... + 16^(segLen-1)
	var (
		bits = 0
		pow  = 1
	)
	for i := 0; i < levels; i++ {
		bits += pow
		pow *= 16
	}
	// A small adjustment is applied to exclude the root element of this path
	// segment, since any existing element would already imply the mutation of
	// the root element. This trick can save us 1 byte for each bitmap which is
	// non-trivial.
	bits -= 1
	return bits / 8
}

// indexScheme defines how trie node paths are converted into index prefixes.
// skipRoot indicates whether the root node should be excluded from indexing.
// cutPoints specifies the prefix lengths (in nibbles) extracted from each path.
// A node path may generate multiple index entries, one for each cut point
// shorter than or equal to the full path length.
type indexScheme struct {
	// skipRoot indicates whether the root node should be excluded from indexing.
	// In the account trie, the root is mutated on every state transition, so
	// indexing it provides no value.
	skipRoot bool

	// cutPoints defines the prefix lengths for index paths. A single trie node
	// path may generate multiple prefixes based on these lengths.
	cutPoints []int

	// bitmaps specifies the required bitmap size for each prefix length.
	bitmaps map[int]int
}

var (
	accountIndexScheme = newIndexScheme(true)
	storageIndexScheme = newIndexScheme(false)
)

// newIndexScheme initializes the index scheme.
func newIndexScheme(skipRoot bool) *indexScheme {
	var (
		cuts    []int
		bitmaps = make(map[int]int)
	)
	for v := 0; v <= 64; v += 3 {
		var (
			levels int
			length int
		)
		if v == 0 && skipRoot {
			length = 1
			levels = 2
		} else {
			length = v
			levels = 3
		}
		cuts = append(cuts, length)
		bitmaps[length] = bitmapSize(levels)
	}
	return &indexScheme{
		skipRoot:  skipRoot,
		cutPoints: cuts,
		bitmaps:   bitmaps,
	}
}

// getBitmapSize returns the required bytes for bitmap at the specified path prefix.
func (s *indexScheme) getBitmapSize(pathLen int) int {
	return s.bitmaps[pathLen]
}

// cuts returns how many chunks should be generated with the given path length.
func (s *indexScheme) cuts(length int) int {
	var n int
	for _, cut := range s.cutPoints {
		if length >= cut {
			n++
			continue
		}
	}
	return n
}

// splitPath applies the indexScheme to the given path and returns two lists:
//
// - prefixes: the progressive prefix cuts defined by the scheme
// - innerIDs: the computed node ID for the path segment following each cut
//
// The scheme defines a set of cut points that partition the path. For each cut:
//
// - prefixes[i] is path[:cutPoints[i]]
// - innerIDs[i] is the node ID of the segment path[cutPoints[i] : nextCut-1]
func (s *indexScheme) splitPath(path string) ([]string, []uint16) {
	// Special case: the root node of the account trie is mutated in every
	// state transition, so its mutation records can be ignored.
	n := len(path)
	if n == 0 && s.skipRoot {
		return nil, nil
	}
	var (
		// Determine how many cuts apply based on path length
		cuts     = s.cuts(n)
		prefixes = make([]string, 0, cuts)
		nodeIDs  = make([]uint16, 0, cuts)
	)
	for i := 0; i < cuts; i++ {
		position := s.cutPoints[i]
		prefixes = append(prefixes, path[:position])

		var limit int
		if i != cuts-1 {
			limit = s.cutPoints[i+1] - 1
		} else {
			limit = len(path)
		}
		nodeIDs = append(nodeIDs, hexPathNodeID(path[position:limit]))
	}
	return prefixes, nodeIDs
}

// splitPathLast returns the longest index prefix produced from the given path,
// along with its corresponding node ID. If the path yields no prefixes, it
// returns an empty prefix and 0.
func (s *indexScheme) splitPathLast(path string) (string, uint16) {
	prefixes, nodeIDs := s.splitPath(path)
	if len(prefixes) == 0 {
		return "", 0
	}
	n := len(prefixes)
	return prefixes[n-1], nodeIDs[n-1]
}

// encodeIDs sorts the given list of uint16 IDs and encodes them into a
// compact byte slice using variable-length unsigned integer encoding.
func encodeIDs(ids []uint16) []byte {
	slices.Sort(ids)
	buf := make([]byte, 0, len(ids))
	for _, id := range ids {
		buf = binary.AppendUvarint(buf, uint64(id))
	}
	return buf
}

// decodeIDs decodes a sequence of variable-length encoded uint16 IDs from the
// given byte slice and returns them as a set.
//
// Returns an error if the input buffer does not contain a complete Uvarint value.
func decodeIDs(buf []byte) ([]uint16, error) {
	var res []uint16
	for len(buf) > 0 {
		id, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, fmt.Errorf("too short for decoding node id, %v", buf)
		}
		buf = buf[n:]
		res = append(res, uint16(id))
	}
	return res, nil
}

// isAncestor reports whether node x is the ancestor of node y.
func isAncestor(x, y uint16) bool {
	if y <= x {
		return false
	}
	for y > x {
		y = (y - 1) / 16 // parentID(y) = (y - 1) / 16
		if y == x {
			return true
		}
	}
	return false
}

// isBitSet reports whether the bit at `index` in the byte slice `b` is set.
func isBitSet(b []byte, index int) bool {
	return b[index/8]&(1<<(index%8)) != 0
}

// setBit sets the bit at `index` in the byte slice `b` to 1.
func setBit(b []byte, index int) {
	b[index/8] |= 1 << (index % 8)
}

//nolint:unused
func debugLog(indexes map[string]map[uint16]struct{}, paths []string, id uint64) {
	if rand.Intn(10000) != 0 {
		return
	}
	f, _ := os.OpenFile(fmt.Sprintf("%d.debug", id), os.O_RDWR|os.O_CREATE, 644)
	for i, path := range paths {
		f.WriteString(fmt.Sprintf("P%d: %v\n", i, []byte(path)))
	}
	leaves := findLeafPaths(paths)
	for i, leaf := range leaves {
		f.WriteString(fmt.Sprintf("L%d: %v\n", i, []byte(leaf)))
	}
	for chunk, IDs := range indexes {
		list := slices.Collect(maps.Keys(IDs))
		f.WriteString(fmt.Sprintf("C%v: %v %v\n", []byte(chunk), list, encodeIDs(list)))
	}
	f.Close()
}

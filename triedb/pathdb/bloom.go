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

package pathdb

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// Domain salts to keep the account, storage and trie node key spaces apart
// within the shared bloom filter, ensuring the identical raw bytes in the
// different domains won't alias to the same bloom key.
const (
	bloomAccountSalt = 0xff51afd7ed558ccd
	bloomStorageSalt = 0xc4ceb9fe1a85ec53
	bloomNodeSalt    = 0x9e3779b97f4a7c15
)

// bloomMix is the splitmix64 finalizer, scrambling the provided value into
// a uniformly distributed bloom key. The keccak-derived keys are uniform
// already, however, the trie node keys (owner + path) are constructed from
// the low entropy inputs and require the extra scrambling.
func bloomMix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// bloomAccountKey derives the bloom key for the account with the specified
// account address hash.
func bloomAccountKey(hash common.Hash) uint64 {
	return bloomMix(binary.BigEndian.Uint64(hash[:8]) ^ bloomAccountSalt)
}

// bloomStorageKey derives the bloom key for the storage slot with the
// specified account address hash and slot key hash.
func bloomStorageKey(accountHash common.Hash, storageHash common.Hash) uint64 {
	x := binary.BigEndian.Uint64(accountHash[:8])
	y := binary.BigEndian.Uint64(storageHash[:8])
	return bloomMix((x<<17 | x>>47) ^ y ^ bloomStorageSalt)
}

// bloomNodeKey derives the bloom key for the trie node with the specified
// trie identifier and node path.
func bloomNodeKey(owner common.Hash, path []byte) uint64 {
	// FNV-1a over the node path, which is too short and low-entropy
	// to be consumed directly
	h := uint64(14695981039346656037)
	for _, b := range path {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return bloomMix(binary.BigEndian.Uint64(owner[:8]) ^ h ^ bloomNodeSalt)
}

// bloomFilter is a blocked bloom filter: the 3 bits of a key sit in one 512-bit
// block, so a probe touches a single cache line. It serves two purposes:
//
//   - The buffer-wide filter, short-circuiting the lookups that miss the buffer
//     entirely (the vast majority of the reads landing on the disk layer). It's
//     shared by the consecutive views and keeps growing while being probed: keys
//     are inserted with insertAtomic, before the content holding them is
//     published, so no false negative can be observed.
//
//   - The per-set filters, locating the set holding an entry without probing the
//     maps of the newer sets, which span megabytes and fall out of the CPU caches.
//     They're built with insert before the set is published and never modified.
//
// Keys are only ever inserted, never removed. Stale keys (e.g. left by a revert)
// merely increase the false positive rate, which is harmless for correctness.
type bloomFilter struct {
	blocks [][8]uint64
}

// newBloomFilter constructs a filter with at least the given number of bits.
func newBloomFilter(bits uint64) *bloomFilter {
	return &bloomFilter{blocks: make([][8]uint64, bits/512+1)}
}

// bloomBits returns the bloom bit count for the given buffer memory allowance:
// 12 bits per entry with a conservative average entry size of 48 bytes, that
// is limit/48*12 = limit/4 bits, giving ~1% false positive rate with 3 probes.
// The count grows with the allowance, with a floor for the tiny buffers in tests.
// An allowance above maxBufferSize (only in tests, the config is sanitized
// against it) is sized as the maximum.
func bloomBits(limit uint64) uint64 {
	return max(min(limit, maxBufferSize)/4, 1<<17)
}

// newBufferBloom constructs the buffer-wide filter for the given buffer memory
// allowance.
func newBufferBloom(limit uint64) *bloomFilter {
	return newBloomFilter(bloomBits(limit))
}

// newSetBloom constructs the filter over the keys of the provided sets, with 12
// bits per key. The keys are also passed to fn if it's not nil, sparing the key
// derivation of another filter over the same keys.
func newSetBloom(nodes *nodeSet, states *stateSet, fn func(uint64)) *bloomFilter {
	b := newBloomFilter(uint64(bloomKeyCount(nodes, states)) * 12)
	forEachBloomKey(nodes, states, func(key uint64) {
		b.insert(key)
		if fn != nil {
			fn(key)
		}
	})
	return b
}

// block returns the block hosting the bits of the key, picked by the high 32
// bits of the key. The bits inside the block are picked by the low 27 bits.
func (b *bloomFilter) block(key uint64) *[8]uint64 {
	return &b.blocks[(key>>32)*uint64(len(b.blocks))>>32]
}

// insert adds the key into a filter that isn't probed concurrently.
func (b *bloomFilter) insert(key uint64) {
	blk := b.block(key)
	for i := range uint64(3) {
		bit := (key >> (9 * i)) & 511
		blk[bit/64] |= 1 << (bit % 64)
	}
}

// insertAtomic adds the key into a filter that may be probed concurrently. The
// callers must serialize the insertions themselves with respect to the content
// publication order.
func (b *bloomFilter) insertAtomic(key uint64) {
	blk := b.block(key)
	for i := range uint64(3) {
		bit := (key >> (9 * i)) & 511
		atomic.OrUint64(&blk[bit/64], 1<<(bit%64))
	}
}

// contains reports whether the key is possibly contained in the filter. False
// positives are possible, false negatives are not (for the keys inserted before
// the guarded content was published). A nil filter can't rule out any key.
func (b *bloomFilter) contains(key uint64) bool {
	if b == nil {
		return true
	}
	blk := b.block(key)
	for i := range uint64(3) {
		bit := (key >> (9 * i)) & 511
		if atomic.LoadUint64(&blk[bit/64])&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// insertSet adds all the keys aggregated in the provided node set and state
// set into a filter that may be probed concurrently.
func (b *bloomFilter) insertSet(nodes *nodeSet, states *stateSet) {
	forEachBloomKey(nodes, states, b.insertAtomic)
}

// forEachBloomKey derives the bloom keys of all the entries aggregated in the
// provided node set and state set.
func forEachBloomKey(nodes *nodeSet, states *stateSet, fn func(uint64)) {
	if nodes != nil {
		for path := range nodes.accountNodes {
			fn(bloomNodeKey(common.Hash{}, []byte(path)))
		}
		for owner, subset := range nodes.storageNodes {
			for path := range subset {
				fn(bloomNodeKey(owner, []byte(path)))
			}
		}
	}
	if states != nil {
		for hash := range states.accountData {
			fn(bloomAccountKey(hash))
		}
		for accountHash, slots := range states.storageData {
			for storageHash := range slots {
				fn(bloomStorageKey(accountHash, storageHash))
			}
		}
	}
}

// bloomKeyCount returns the number of the entries aggregated in the provided
// node set and state set.
func bloomKeyCount(nodes *nodeSet, states *stateSet) int {
	var n int
	if nodes != nil {
		n += len(nodes.accountNodes)
		for _, subset := range nodes.storageNodes {
			n += len(subset)
		}
	}
	if states != nil {
		n += len(states.accountData)
		for _, slots := range states.storageData {
			n += len(slots)
		}
	}
	return n
}

// bloomKeyPool recycles the key slices staged between the key derivation and
// the insertion into the buffer-wide filter.
var bloomKeyPool = sync.Pool{New: func() any { return new([]uint64) }}

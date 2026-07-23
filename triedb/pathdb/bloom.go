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

// bufferBloom is a lock-free bloom filter guarding the aggregated sets in the
// buffer. As the vast majority of the state lookups landing on the disk layer
// miss the buffer entirely, probing the filter first short-circuits the linear
// scan over the aggregated sets, keeping the read amplification of the
// log-structured buffer negligible.
//
// The filter is strictly additive: keys are only ever inserted, never removed.
// Stale keys (e.g. left by a revert operation) merely increase the false
// positive rate, which is harmless for correctness. Concurrent insertion and
// probing is safe: the bits are set with atomic OR and observed with atomic
// loads. Writers must ensure the relevant keys are fully inserted before the
// guarded content is published to the readers, guaranteeing no false negative
// can ever be observed.
type bufferBloom struct {
	words []uint64 // bit array, atomic access only
	mask  uint32   // number of bits - 1, for cheap modulo (bits is power of two)
}

// bloomSize returns the bloom bit count for the given buffer memory allowance,
// targeting roughly 12 bits per entry with the conservative estimation of the
// average entry size (~48 bytes), i.e. ~1% false positive rate with 3 probes.
// The result is clamped into a sane range and rounded up to a power of two.
func bloomSize(limit uint64) uint64 {
	const (
		minBits = 1 << 17 // 16KiB, for the tiny buffers in tests
		maxBits = 1 << 27 // 16MiB, enough for the maximum 256MiB buffer
	)
	bits := min(max(limit/4, minBits), maxBits)

	// Round up to the next power of two
	if bits&(bits-1) != 0 {
		p := uint64(minBits)
		for p < bits {
			p <<= 1
		}
		bits = p
	}
	return bits
}

// newBufferBloom constructs the bloom filter with the size derived from the
// provided buffer memory allowance.
func newBufferBloom(limit uint64) *bufferBloom {
	bits := bloomSize(limit)
	return &bufferBloom{
		words: make([]uint64, bits/64),
		mask:  uint32(bits - 1),
	}
}

// insert adds the provided key into the bloom filter. It's safe for concurrent
// use with probing, but the callers must serialize the insertions themselves
// with respect to the content publication order.
func (b *bufferBloom) insert(key uint64) {
	var (
		h1 = uint32(key)
		h2 = uint32(key >> 32)
	)
	for i := range uint32(3) {
		bit := (h1 + i*h2) & b.mask
		atomic.OrUint64(&b.words[bit/64], 1<<(bit%64))
	}
}

// contains reports whether the provided key is possibly contained in the
// filter. False positives are possible, false negatives are not (for the
// keys inserted before the guarded content was published).
func (b *bufferBloom) contains(key uint64) bool {
	var (
		h1 = uint32(key)
		h2 = uint32(key >> 32)
	)
	for i := range uint32(3) {
		bit := (h1 + i*h2) & b.mask
		if atomic.LoadUint64(&b.words[bit/64])&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// insertSet adds all the keys aggregated in the provided node set and state
// set into the bloom filter.
func (b *bufferBloom) insertSet(nodes *nodeSet, states *stateSet) {
	if nodes != nil {
		for path := range nodes.accountNodes {
			b.insert(bloomNodeKey(common.Hash{}, []byte(path)))
		}
		for owner, subset := range nodes.storageNodes {
			for path := range subset {
				b.insert(bloomNodeKey(owner, []byte(path)))
			}
		}
	}
	if states != nil {
		for hash := range states.accountData {
			b.insert(bloomAccountKey(hash))
		}
		for accountHash, slots := range states.storageData {
			for storageHash := range slots {
				b.insert(bloomStorageKey(accountHash, storageHash))
			}
		}
	}
}

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

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	jumpDestHitMeter  = metrics.NewRegisteredMeter("chain/cache/jumpdest/hit", nil)
	jumpDestMissMeter = metrics.NewRegisteredMeter("chain/cache/jumpdest/miss", nil)
)

const (
	// Cache size granted for caching jump destination. It's the size
	// of one bucket. The total size is one gigabyte.
	jumpDestCacheSize = 64 * 1024 * 1024
)

// JumpDestCache implements vm.JumpDestCache. It's thread-safe and is shared
// across different block processing as a global cache.
type JumpDestCache struct {
	// List of code dest buckets, each of which is thread-safe.
	buckets [16]struct {
		dest *lru.SizeConstrainedCache[common.Hash, vm.BitVec]
	}
}

// NewJumpDestCache constructs the code analysis cache.
func NewJumpDestCache() *JumpDestCache {
	c := new(JumpDestCache)
	for i := range c.buckets {
		c.buckets[i].dest = lru.NewSizeConstrainedCache[common.Hash, vm.BitVec](jumpDestCacheSize)
	}
	return c
}

// Load retrieves the cached jumpdest analysis for the given code hash.
// Returns the BitVec and true if found, or nil and false if not cached.
func (c *JumpDestCache) Load(hash common.Hash) (vm.BitVec, bool) {
	bucket := &c.buckets[hash[0]&0x0f]
	v, ok := bucket.dest.Get(hash)
	if ok {
		jumpDestHitMeter.Mark(1)
	} else {
		jumpDestMissMeter.Mark(1)
	}
	return v, ok
}

// Store saves the jumpdest analysis for the given code hash.
func (c *JumpDestCache) Store(hash common.Hash, b vm.BitVec) {
	bucket := &c.buckets[hash[0]&0x0f]
	bucket.dest.Add(hash, b)
}

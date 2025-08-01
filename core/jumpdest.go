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
	"bytes"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	jumpDestHitMeter    = metrics.NewRegisteredMeter("chain/cache/jumpdest/hit", nil)
	jumpDestMissMeter   = metrics.NewRegisteredMeter("chain/cache/jumpdest/miss", nil)
	precompileHitMeter  = metrics.NewRegisteredMeter("chain/cache/precompile/hit", nil)
	precompileMissMeter = metrics.NewRegisteredMeter("chain/cache/precompile/miss", nil)
)

const (
	// Cache size granted for caching jump destination. It's the size
	// of one bucket. The total size is one gigabyte.
	jumpDestCacheSize   = 16 * 1024 * 1024
	precompileCacheSize = 4 * 1024 * 1024
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

type PrecompileCache struct {
	cache *lru.SizeConstrainedCache[string, []byte]
}

func NewPrecompileCache(size uint64) *PrecompileCache {
	return &PrecompileCache{
		cache: lru.NewSizeConstrainedCache[string, []byte](size),
	}
}

func (p *PrecompileCache) key(name string, input []byte) string {
	return name + string(input)
}

func (p *PrecompileCache) Load(name string, input []byte) ([]byte, bool) {
	v, found := p.cache.Get(p.key(name, input))
	if !found {
		return nil, false
	}
	return bytes.Clone(v), true
}

func (p *PrecompileCache) Store(name string, input []byte, result []byte) {
	p.cache.Add(p.key(name, input), bytes.Clone(result))
}

type PrecompileCacheWithStats struct {
	cache vm.PrecompileCache
	hit   atomic.Int32
	miss  atomic.Int32
}

func NewPrecompileCacheWithStats(cache vm.PrecompileCache) *PrecompileCacheWithStats {
	return &PrecompileCacheWithStats{cache: cache}
}

func (p *PrecompileCacheWithStats) Load(name string, input []byte) ([]byte, bool) {
	v, ok := p.cache.Load(name, input)
	if ok {
		precompileHitMeter.Mark(1)
		p.hit.Add(1)
	} else {
		precompileMissMeter.Mark(1)
		p.miss.Add(1)
	}
	return v, ok
}

func (p *PrecompileCacheWithStats) Store(name string, input []byte, result []byte) {
	p.cache.Store(name, input, result)
}

func (p *PrecompileCacheWithStats) Stats() (int32, int32) {
	return p.hit.Load(), p.miss.Load()
}

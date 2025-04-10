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

package utils

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-verkle"
)

var (
	pointCacheHitGauge  = metrics.NewRegisteredGauge("trie/verkle/cache/point/hit", nil)
	pointCacheMissGauge = metrics.NewRegisteredGauge("trie/verkle/cache/point/miss", nil)
	hashCacheHitGauge   = metrics.NewRegisteredGauge("trie/verkle/cache/hash/hit", nil)
	hashCacheMissGauge  = metrics.NewRegisteredGauge("trie/verkle/cache/hash/miss", nil)
)

// PointCache is the LRU cache for storing evaluated address commitment.
type PointCache struct {
	points *lru.Cache[common.Address, *verkle.Point]
	hashes *lru.Cache[common.Address, [32]byte]
}

// NewPointCache returns the cache with specified size.
func NewPointCache(maxItems int) *PointCache {
	return &PointCache{
		points: lru.NewCache[common.Address, *verkle.Point](maxItems),
		hashes: lru.NewCache[common.Address, [32]byte](maxItems),
	}
}

// GetPoint returns the evaluated address commitment for the specified address.
// If not already available, it will be computed on the fly.
func (c *PointCache) GetPoint(addr common.Address) *verkle.Point {
	p, ok := c.points.Get(addr)
	if ok {
		pointCacheHitGauge.Inc(1)
		return p
	}
	pointCacheMissGauge.Inc(1)

	p = EvaluateAddressPoint(addr.Bytes())
	c.points.Add(addr, p)
	return p
}

// GetPointHash returns the hash of evaluated address commitment for the specified
// address. If not already available, it will be computed on the fly.
func (c *PointCache) GetPointHash(addr common.Address) [32]byte {
	h, ok := c.hashes.Get(addr)
	if ok {
		hashCacheHitGauge.Inc(1)
		return h
	}
	hashCacheMissGauge.Inc(1)

	h = verkle.HashPointToBytes(c.GetPoint(addr))
	c.hashes.Add(addr, h)
	return h
}

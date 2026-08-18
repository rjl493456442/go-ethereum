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

package pebble

import (
	"github.com/cockroachdb/pebble/v2"
)

// Sizing of the write-heavy profile. The numbers are derived from the level
// layout pebble computes in initLevelMaxBytes: the base level is chosen by
// walking up from the deepest non-empty level while the projected size of that
// level exceeds LBaseMaxBytes. Each step up adds a whole extra level for every
// byte to be rewritten on its way down, so the aim is to stop that walk early.
const (
	// writeHeavyLBaseMaxBytes puts the base level at L5 for a mainnet-sized
	// state, leaving L5 -> L6 as the only level transition below L0. The default
	// of 64MB lands the base at L2 instead, which is four transitions.
	//
	// Sizing this is coarse by nature: the base level moves in whole steps, each
	// a factor of LevelMultiplier apart, so values between two steps buy nothing
	// but a flatter pyramid.
	writeHeavyLBaseMaxBytes = 32 << 30

	// writeHeavyL0CompactionThreshold lets L0 grow to this many sublevels before
	// compacting. It must be raised together with LBaseMaxBytes: an L0 -> Lbase
	// compaction rewrites the overlapping part of Lbase, so a large base level
	// drained in small batches costs far more than the levels it saves.
	writeHeavyL0CompactionThreshold = 24

	// writeHeavyL0StopWritesThreshold is the hard ceiling on L0 sublevels, past
	// which writes stop. It has to clear the compaction threshold with room to
	// spare, or writes stall while the backlog drains.
	writeHeavyL0StopWritesThreshold = 96

	// writeHeavyTargetFileSize is the target size of an L0 file. Pebble derives
	// FlushSplitBytes from it, so the 2MB used for regular operation shatters a
	// large memtable flush into hundreds of small tables.
	writeHeavyTargetFileSize = 64 * 1024 * 1024

	// writeHeavyMemTableNumber is the number of memtables kept in flight, with
	// the stop-writes threshold at twice that. Memory is budgeted against the
	// threshold rather than the count, since that is what bounds the footprint.
	writeHeavyMemTableNumber = 4

	// writeHeavyBlockCacheDivisor is the fraction of the cache allowance left to
	// the block cache, the rest going to memtables.
	writeHeavyBlockCacheDivisor = 8

	// writeHeavyMinBlockCache is the floor for the block cache, in megabytes.
	writeHeavyMinBlockCache = 128
)

// applyWriteHeavy re-tunes the options for a bounded, write-dominated phase such
// as the state download of a snap sync: writes are near-sorted and dominated by
// keys that have never been seen before, and reads are rare.
//
// The bulk of the saving is in write amplification rather than in deferred work.
// A key with no prior version reclaims nothing when it is compacted, so every
// compaction output is the full sum of its inputs; the only product is a shape
// that reads better, which this phase does not need. Merging later and fewer
// times therefore removes rewrites outright rather than postponing them.
//
// The database must be reopened without this profile once the phase is over.
// Reads against the L0 backlog this leaves behind are slow, and the backlog only
// drains under the regular settings.
func applyWriteHeavy(opt *pebble.Options, cache int) {
	// Reads are rare, so the block cache earns little. Memtables come from a
	// separate manual arena in pebble v2 rather than from the block cache, so
	// shrinking one genuinely funds the other.
	blockCache := max(cache/writeHeavyBlockCacheDivisor, writeHeavyMinBlockCache)
	opt.Cache.Unref()
	opt.Cache = pebble.NewCache(int64(blockCache) * 1024 * 1024)

	// Every flush lands another file in L0 that has to be merged down eventually,
	// so fewer and larger flushes mean less to compact.
	stopWrites := writeHeavyMemTableNumber * 2
	memTableSize := (cache - blockCache) * 1024 * 1024 / stopWrites
	if memTableSize >= maxMemTableSize {
		memTableSize = maxMemTableSize - 1
	}
	opt.MemTableSize = uint64(memTableSize)
	opt.MemTableStopWritesThreshold = stopWrites

	// Letting L0 accumulate keeps write amplification near one for as long as it
	// lasts, and amortises the eventual Lbase rewrite over a much larger batch.
	opt.L0CompactionThreshold = writeHeavyL0CompactionThreshold
	opt.L0StopWritesThreshold = writeHeavyL0StopWritesThreshold

	// Land L0 in a deep base level so that a byte crosses as few levels as
	// possible on its way to the bottom.
	opt.LBaseMaxBytes = writeHeavyLBaseMaxBytes

	// Keep the file count down, both in L0 and on the way down.
	opt.TargetFileSizes[0] = writeHeavyTargetFileSize
	for i := 1; i < len(opt.TargetFileSizes); i++ {
		opt.TargetFileSizes[i] = opt.TargetFileSizes[i-1] * 2
	}
	opt.FlushSplitBytes = opt.TargetFileSizes[0]

	// Bloom filters only pay off on reads, yet are rebuilt by every flush and
	// every compaction. The tables written here pick them up once they are
	// rewritten under the regular options.
	for i := range opt.Levels {
		opt.Levels[i].FilterPolicy = pebble.NoFilterPolicy
	}
}

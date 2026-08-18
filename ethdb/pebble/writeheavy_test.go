package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// TestApplyWriteHeavy checks the knobs that have to move together. Raising
// LBaseMaxBytes on its own is worse than leaving it alone: a large base level
// drained in small L0 batches is rewritten once per batch, which costs more
// than the levels the deeper base saves.
func TestApplyWriteHeavy(t *testing.T) {
	const cache = 8192 // MB

	opt := &pebble.Options{Cache: pebble.NewCache(int64(cache) * 1024 * 1024)}
	opt.EnsureDefaults()
	applyWriteHeavy(opt, cache)

	if opt.LBaseMaxBytes <= 64<<20 {
		t.Errorf("LBaseMaxBytes = %d, want more than the 64MB default", opt.LBaseMaxBytes)
	}
	if opt.L0CompactionThreshold <= 4 {
		t.Errorf("L0CompactionThreshold = %d, want L0 to be allowed to accumulate", opt.L0CompactionThreshold)
	}
	if opt.L0StopWritesThreshold <= opt.L0CompactionThreshold {
		t.Errorf("L0StopWritesThreshold = %d must clear L0CompactionThreshold = %d, or writes stall",
			opt.L0StopWritesThreshold, opt.L0CompactionThreshold)
	}
	// FlushSplitBytes is derived from the L0 target when left unset, so a small
	// target shatters a large memtable flush into many tiny tables.
	if opt.TargetFileSizes[0] < 32<<20 {
		t.Errorf("TargetFileSizes[0] = %d, want a large L0 target", opt.TargetFileSizes[0])
	}
	// Memory has to be budgeted against the stop-writes threshold, not the
	// nominal memtable count, since that is what bounds the footprint.
	if worst := opt.MemTableSize * uint64(opt.MemTableStopWritesThreshold); worst > uint64(cache)*1024*1024 {
		t.Errorf("worst-case memtable footprint %d exceeds the %d MB allowance", worst, cache)
	}
	if opt.MemTableSize >= maxMemTableSize {
		t.Errorf("MemTableSize = %d exceeds the arena limit %d", opt.MemTableSize, uint64(maxMemTableSize))
	}
	// Reads are rare, so the block cache should have given ground to memtables.
	if got := opt.Cache.MaxSize(); got >= int64(cache)*1024*1024 {
		t.Errorf("block cache = %d, want it shrunk below the full %d MB allowance", got, cache)
	}
	for i, l := range opt.Levels {
		if l.FilterPolicy != nil && l.FilterPolicy != pebble.NoFilterPolicy {
			t.Errorf("level %d still builds bloom filters", i)
		}
	}
	// The options have to be acceptable to pebble itself.
	if err := opt.Validate(); err != nil {
		t.Fatalf("validating write-heavy options: %v", err)
	}
}

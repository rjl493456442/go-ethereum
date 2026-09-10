// Copyright 2023 The go-ethereum Authors
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

// Package pebble implements the key-value database layer based on pebble.
package pebble

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	// minCache is the minimum amount of memory in megabytes to allocate to pebble
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16

	// metricsGatheringInterval specifies the interval to retrieve pebble database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second

	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute
)

// Database is a persistent key-value store based on the pebble v2 storage engine.
// Apart from basic data storage functionality it also supports batch writes and
// iterating over the keyspace in binary-alphabetical order.
type Database struct {
	fn        string     // filename for reporting
	db        *pebble.DB // Underlying pebble storage engine
	namespace string     // Namespace for metrics

	compTimeMeter          *metrics.Meter   // Meter for measuring the total time spent in database compaction
	compReadMeter          *metrics.Meter   // Meter for measuring the data read during compaction
	compWriteMeter         *metrics.Meter   // Meter for measuring the data written during compaction
	writeDelayNMeter       *metrics.Meter   // Meter for measuring the write delay number due to database compaction
	writeDelayMeter        *metrics.Meter   // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge          *metrics.Gauge   // Gauge for tracking the size of all the levels in the database
	diskReadMeter          *metrics.Meter   // Meter for measuring the effective amount of data read
	diskWriteMeter         *metrics.Meter   // Meter for measuring the effective amount of data written
	memCompGauge           *metrics.Gauge   // Gauge for tracking the number of memory compaction
	level0CompGauge        *metrics.Gauge   // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge     *metrics.Gauge   // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge          *metrics.Gauge   // Gauge for tracking the number of table compaction caused by read opt
	manualMemAllocGauge    *metrics.Gauge   // Gauge for tracking amount of non-managed memory currently allocated
	liveMemTablesGauge     *metrics.Gauge   // Gauge for tracking the number of live memory tables
	zombieMemTablesGauge   *metrics.Gauge   // Gauge for tracking the number of zombie memory tables
	blockCacheHitGauge     *metrics.Gauge   // Gauge for tracking the number of total hit in the block cache
	blockCacheMissGauge    *metrics.Gauge   // Gauge for tracking the number of total miss in the block cache
	tableCacheHitGauge     *metrics.Gauge   // Gauge for tracking the number of total hit in the file cache
	tableCacheMissGauge    *metrics.Gauge   // Gauge for tracking the number of total miss in the file cache
	filterHitGauge         *metrics.Gauge   // Gauge for tracking the number of total hit in bloom filter
	filterMissGauge        *metrics.Gauge   // Gauge for tracking the number of total miss in bloom filter
	estimatedCompDebtGauge *metrics.Gauge   // Gauge for tracking the number of bytes that need to be compacted
	liveCompGauge          *metrics.Gauge   // Gauge for tracking the number of in-progress compactions
	liveCompSizeGauge      *metrics.Gauge   // Gauge for tracking the size of in-progress compactions
	liveIterGauge          *metrics.Gauge   // Gauge for tracking the number of live database iterators
	levelsGauge            []*metrics.Gauge // Gauge for tracking the number of tables in levels

	quitLock sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	closed   bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path

	activeComp    int           // Current number of active compactions
	compStartTime time.Time     // The start time of the earliest currently-active compaction
	compTime      atomic.Int64  // Total time spent in compaction in ns
	level0Comp    atomic.Uint32 // Total number of level-zero compactions
	nonLevel0Comp atomic.Uint32 // Total number of non level-zero compactions

	writeStalled        atomic.Bool  // Flag whether the write is stalled
	writeDelayStartTime time.Time    // The start time of the latest write stall
	writeDelayReason    string       // The reason of the latest write stall
	writeDelayCount     atomic.Int64 // Total number of write stall counts
	writeDelayTime      atomic.Int64 // Total time spent in write stalls
	stallMemtableCount  atomic.Int64 // Write stalls at the memtable queue, ie flush falling behind
	stallMemtableTime   atomic.Int64
	stallL0Count        atomic.Int64 // Write stalls at the L0 limit, ie compaction falling behind
	stallL0Time         atomic.Int64

	// The L0 -> base level step, from compaction events; see CompactionStats.
	l0DrainCount     atomic.Int64
	l0DrainSublevels atomic.Int64
	l0DrainBytes     atomic.Int64
	lbaseDrainBytes  atomic.Int64
	l0DrainPeakBytes atomic.Int64
	l0MoveCount      atomic.Int64
	l0MoveBytes      atomic.Int64
	intraL0Count     atomic.Int64
	intraL0Bytes     atomic.Int64
	peakSublevels    atomic.Int64 // deepest L0 seen by the metrics sampler

	writeOptions *pebble.WriteOptions
}

func (d *Database) onCompactionBegin(info pebble.CompactionInfo) {
	if d.activeComp == 0 {
		d.compStartTime = time.Now()
	}
	l0 := info.Input[0]
	if l0.Level == 0 {
		d.level0Comp.Add(1)
	} else {
		d.nonLevel0Comp.Add(1)
	}
	d.activeComp++
}

func (d *Database) onCompactionEnd(info pebble.CompactionInfo) {
	if d.activeComp == 1 {
		d.compTime.Add(int64(time.Since(d.compStartTime)))
	} else if d.activeComp == 0 {
		panic("should not happen")
	}
	d.activeComp--

	// Record how much of the L0 stack an L0->Lbase compaction actually drained.
	if info.Err != nil || len(info.Input) == 0 {
		return
	}
	// Intra-L0 compactions rewrite L0 into itself. They buy read amplification
	// but every byte they write has to be written again on the way to Lbase, so
	// they are pure overhead as far as write amplification is concerned.
	if l0 := info.Input[0]; l0.Level == 0 && info.Output.Level == 0 {
		var n uint64
		for _, t := range l0.Tables {
			n += t.Size
		}
		d.intraL0Count.Add(1)
		d.intraL0Bytes.Add(int64(n))
	}
	if l0 := info.Input[0]; l0.Level == 0 && info.Output.Level != 0 {
		var l0Bytes uint64
		for _, t := range l0.Tables {
			l0Bytes += t.Size
		}
		// The base level tables are rewritten wholesale to absorb the L0 input,
		// so together the two sides give the measured write amplification of
		// this step, rather than one modelled from level sizes.
		var lbaseBytes uint64
		for _, lvl := range info.Input[1:] {
			for _, t := range lvl.Tables {
				lbaseBytes += t.Size
			}
		}
		// A move compaction relinks its input into the base level and writes
		// nothing, so counting it as a drain would both overstate what this
		// stage costs and drag its write amplification towards 1. Note this has
		// to key off the compaction kind: a compaction that merely overlaps no
		// base level table is not a move, it still reads L0 and writes the
		// result out, and belongs in the average at a write amplification of 1.
		if strings.HasSuffix(info.Reason, "move") {
			d.l0MoveCount.Add(1)
			d.l0MoveBytes.Add(int64(l0Bytes))
			return
		}
		d.l0DrainCount.Add(1)
		d.l0DrainSublevels.Add(int64(l0OverlapDepth(l0.Tables)))
		d.l0DrainBytes.Add(int64(l0Bytes))
		d.lbaseDrainBytes.Add(int64(lbaseBytes))

		// Track the largest single drain. pebble stops growing a drain once its
		// L0 input passes 100MB and grows by more than half again, and hard
		// stops at 500MB, so where the peak sits says which limit is binding.
		for peak := d.l0DrainPeakBytes.Load(); int64(l0Bytes) > peak; peak = d.l0DrainPeakBytes.Load() {
			if d.l0DrainPeakBytes.CompareAndSwap(peak, int64(l0Bytes)) {
				break
			}
		}
	}
}

func (d *Database) onWriteStallBegin(b pebble.WriteStallBeginInfo) {
	d.writeDelayStartTime = time.Now()
	d.writeDelayCount.Add(1)
	d.writeStalled.Store(true)

	// Take just the first word of the reason. These are two potential
	// reasons for the write stall:
	// - memtable count limit reached
	// - L0 file count limit exceeded
	reason := b.Reason
	if i := strings.IndexByte(reason, ' '); i != -1 {
		reason = reason[:i]
	}
	if reason == "L0" || reason == "memtable" {
		d.writeDelayReason = reason
		if reason == "memtable" {
			d.stallMemtableCount.Add(1)
		} else {
			d.stallL0Count.Add(1)
		}
		metrics.GetOrRegisterGauge(d.namespace+"stall/count/"+reason, nil).Inc(1)
	}
}

func (d *Database) onWriteStallEnd() {
	stalled := time.Since(d.writeDelayStartTime)
	d.writeDelayTime.Add(int64(stalled))
	d.writeStalled.Store(false)
	switch d.writeDelayReason {
	case "memtable":
		d.stallMemtableTime.Add(int64(stalled))
	case "L0":
		d.stallL0Time.Add(int64(stalled))
	}

	if d.writeDelayReason != "" {
		metrics.GetOrRegisterResettingTimer(d.namespace+"stall/time/"+d.writeDelayReason, nil).UpdateSince(d.writeDelayStartTime)
		d.writeDelayReason = ""
	}
	d.writeDelayStartTime = time.Time{}
}

// panicLogger is just a noop logger to disable Pebble's internal logger.
//
// TODO(karalabe): Remove when Pebble sets this as the default.
type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{}) {
}

func (l panicLogger) Errorf(format string, args ...interface{}) {
}

func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Errorf("fatal: "+format, args...))
}

// New returns a wrapped pebble DB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles, "version", "v2")

	// The max memtable size is limited by the uint32 offsets stored in
	// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
	//
	// - MaxUint32 on 64-bit platforms;
	// - MaxInt on 32-bit platforms.
	//
	// It is used when slices are limited to Uint32 on 64-bit platforms (the
	// length limit for slices is naturally MaxInt on 32-bit platforms).
	//
	// Taken from https://github.com/cockroachdb/pebble/blob/master/internal/constants/constants.go
	maxMemTableSize := (1<<31)<<(^uint(0)>>63) - 1

	// Four memory tables are configured, each with a default size of 256 MB.
	// Having multiple smaller memory tables while keeping the total memory
	// limit unchanged allows writes to be flushed more smoothly. This helps
	// avoid compaction spikes and mitigates write stalls caused by heavy
	// compaction workloads.
	memTableNumber := 4
	memTableSize := cache * 1024 * 1024 / 2 / memTableNumber

	// The memory table size is currently capped at maxMemTableSize-1 due to a
	// known bug in the pebble where maxMemTableSize is not recognized as a
	// valid size.
	//
	// TODO use the maxMemTableSize as the maximum table size once the issue
	// in pebble is fixed.
	if memTableSize >= maxMemTableSize {
		memTableSize = maxMemTableSize - 1
	}
	db := &Database{
		fn:        file,
		log:       logger,
		quitChan:  make(chan chan error),
		namespace: namespace,

		// Use asynchronous write mode by default. Otherwise, the overhead of frequent fsync
		// operations can be significant, especially on platforms with slow fsync performance
		// (e.g., macOS) or less capable SSDs.
		//
		// Note that enabling async writes means recent data may be lost in the event of an
		// application-level panic (writes will also be lost on a machine-level failure,
		// of course). Geth is expected to handle recovery from an unclean shutdown.
		writeOptions: pebble.NoSync,
	}
	numCPU := runtime.NumCPU()
	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: uint64(memTableSize),

		// MemTableStopWritesThreshold places a hard limit on the number
		// of the existent MemTables(including the frozen one).
		//
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		//
		// MemTableStopWritesThreshold is set to twice the maximum number of
		// allowed memtables to accommodate temporary spikes.
		MemTableStopWritesThreshold: memTableNumber * 2,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		CompactionConcurrencyRange: func() (int, int) { return 1, numCPU },

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: [7]pebble.LevelOptions{
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},
			{FilterPolicy: bloom.FilterPolicy(10)},

			// No bloom filter on the bottommost level: a point lookup only
			// reaches it after every level above has missed, so its filter is
			// the largest and the least useful, and every compaction into it
			// would otherwise pay to rebuild one.
			//
			// This has to be explicit. In pebble v2 an empty LevelOptions for
			// L1 and below inherits the level above it (EnsureL1PlusDefaults),
			// so the empty literal used before this kept L5's bloom filter on
			// L6 and the comment claiming otherwise was wrong.
			{FilterPolicy: pebble.NoFilterPolicy},
		},
		// Per-level target file sizes, indexed relative to the base level:
		// [0] is L0, [1] the base level, [2] the level below it, and so on.
		//
		// The L0 target is larger than the ladder below it on purpose. A flush
		// is cut into files of this size, and every file costs an fsync of
		// 20-30ms on NVMe regardless of its size, so a memtable flush of a few
		// hundred MB into 2MB files was hundreds of fsyncs and took ~10s. A
		// flush cannot run faster than file size / fsync latency, about 80MB/s
		// at 2MB, and once a sustained write burst exceeds that the queue of
		// memtables awaiting flush fills and pebble stops writes at the
		// memtable limit while the disk sits half idle. At 16MB the same flush
		// is a few dozen files and ~2.5s, and the stall moves to the L0 limit,
		// which is the disk's compaction bandwidth itself.
		//
		// FlushSplitBytes is left at pebble's default of twice this value.
		TargetFileSizes: [7]int64{
			16 * 1024 * 1024, // L0
			4 * 1024 * 1024,  // base level
			8 * 1024 * 1024,
			16 * 1024 * 1024,
			32 * 1024 * 1024,
			64 * 1024 * 1024,
			128 * 1024 * 1024,
		},
		ReadOnly: readonly,
		EventListener: &pebble.EventListener{
			CompactionBegin: db.onCompactionBegin,
			CompactionEnd:   db.onCompactionEnd,
			WriteStallBegin: db.onWriteStallBegin,
			WriteStallEnd:   db.onWriteStallEnd,
		},
		Logger: panicLogger{}, // TODO(karalabe): Delete when this is upstreamed in Pebble

		// Pebble is configured to use asynchronous write mode, meaning write operations
		// return as soon as the data is cached in memory, without waiting for the WAL
		// to be written. This mode offers better write performance but risks losing
		// recent writes if the application crashes or a power failure/system crash occurs.
		//
		// By setting the WALBytesPerSync, the cached WAL writes will be periodically
		// flushed at the background if the accumulated size exceeds this threshold.
		WALBytesPerSync: 5 * ethdb.IdealBatchSize,

		// L0CompactionThreshold specifies the number of L0 read-amplification
		// necessary to trigger an L0 compaction. It essentially refers to the
		// number of sub-levels at the L0. For each sub-level, it contains several
		// L0 files which are non-overlapping with each other, typically produced
		// by a single memory-table flush.
		//
		// The default value in Pebble is 4, which is a bit too large to have
		// the compaction debt as around 10GB. By reducing it to 2, the compaction
		// debt will be less than 1GB, but with more frequent compactions scheduled.
		L0CompactionThreshold: 2,

		// FormatFlushableIngest is the minimum FormatMajorVersion supported by
		// pebble v2. The more advanced version can be enabled later.
		//
		// This version is supported by both v1 and v2. It serves as the natural
		// bridge point: a v1 database can be ratcheted up to FormatFlushableIngest
		// using pebble v1, and then pebble v2 can open it since that's its minimum.
		FormatMajorVersion: formatMinV2,
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	// These two settings define the conditions under which compaction concurrency
	// is increased. Specifically, one additional compaction job will be enabled when:
	// - there is one more overlapping sub-level0;
	// - there is an additional 256 MB of compaction debt;
	//
	// The maximum concurrency is still capped by CompactionConcurrencyRange, but with
	// these settings compactions can scale up more readily.
	opt.Experimental.L0CompactionConcurrency = 1
	opt.Experimental.CompactionDebtConcurrency = 1 << 28 // 256MB

	// Open the db and recover any potential corruptions
	innerDB, err := pebble.Open(file, opt)
	if err != nil {
		return nil, err
	}
	db.db = innerDB

	db.compTimeMeter = metrics.GetOrRegisterMeter(namespace+"compact/time", nil)
	db.compReadMeter = metrics.GetOrRegisterMeter(namespace+"compact/input", nil)
	db.compWriteMeter = metrics.GetOrRegisterMeter(namespace+"compact/output", nil)
	db.diskSizeGauge = metrics.GetOrRegisterGauge(namespace+"disk/size", nil)
	db.diskReadMeter = metrics.GetOrRegisterMeter(namespace+"disk/read", nil)
	db.diskWriteMeter = metrics.GetOrRegisterMeter(namespace+"disk/write", nil)
	db.writeDelayMeter = metrics.GetOrRegisterMeter(namespace+"compact/writedelay/duration", nil)
	db.writeDelayNMeter = metrics.GetOrRegisterMeter(namespace+"compact/writedelay/counter", nil)
	db.memCompGauge = metrics.GetOrRegisterGauge(namespace+"compact/memory", nil)
	db.level0CompGauge = metrics.GetOrRegisterGauge(namespace+"compact/level0", nil)
	db.nonlevel0CompGauge = metrics.GetOrRegisterGauge(namespace+"compact/nonlevel0", nil)
	db.seekCompGauge = metrics.GetOrRegisterGauge(namespace+"compact/seek", nil)
	db.manualMemAllocGauge = metrics.GetOrRegisterGauge(namespace+"memory/manualalloc", nil)
	db.liveMemTablesGauge = metrics.GetOrRegisterGauge(namespace+"table/live", nil)
	db.zombieMemTablesGauge = metrics.GetOrRegisterGauge(namespace+"table/zombie", nil)
	db.blockCacheHitGauge = metrics.GetOrRegisterGauge(namespace+"cache/block/hit", nil)
	db.blockCacheMissGauge = metrics.GetOrRegisterGauge(namespace+"cache/block/miss", nil)
	db.tableCacheHitGauge = metrics.GetOrRegisterGauge(namespace+"cache/table/hit", nil)
	db.tableCacheMissGauge = metrics.GetOrRegisterGauge(namespace+"cache/table/miss", nil)
	db.filterHitGauge = metrics.GetOrRegisterGauge(namespace+"filter/hit", nil)
	db.filterMissGauge = metrics.GetOrRegisterGauge(namespace+"filter/miss", nil)
	db.estimatedCompDebtGauge = metrics.GetOrRegisterGauge(namespace+"compact/estimateDebt", nil)
	db.liveCompGauge = metrics.GetOrRegisterGauge(namespace+"compact/live/count", nil)
	db.liveCompSizeGauge = metrics.GetOrRegisterGauge(namespace+"compact/live/size", nil)
	db.liveIterGauge = metrics.GetOrRegisterGauge(namespace+"iter/count", nil)

	// Start up the metrics gathering and return
	go db.meter(metricsGatheringInterval, namespace)
	return db, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *Database) Close() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
	// Allow double closing, simplifies things
	if d.closed {
		return nil
	}
	d.closed = true
	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		if err := <-errc; err != nil {
			d.log.Error("Metrics collection failed", "err", err)
		}
		d.quitChan = nil
	}
	return d.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, pebble.ErrClosed
	}
	_, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err = closer.Close(); err != nil {
		return false, err
	}
	return true, nil
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, pebble.ErrClosed
	}
	dat, closer, err := d.db.Get(key)
	if err != nil {
		return nil, err
	}
	ret := make([]byte, len(dat))
	copy(ret, dat)
	if err = closer.Close(); err != nil {
		return nil, err
	}
	return ret, nil
}

// Put inserts the given value into the key-value store.
func (d *Database) Put(key []byte, value []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return pebble.ErrClosed
	}
	return d.db.Set(key, value, d.writeOptions)
}

// Delete removes the key from the key-value store.
func (d *Database) Delete(key []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return pebble.ErrClosed
	}
	return d.db.Delete(key, d.writeOptions)
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
func (d *Database) DeleteRange(start, end []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	if d.closed {
		return pebble.ErrClosed
	}
	// There is no special flag to represent the end of key range
	// in pebble(nil in leveldb). Use an ugly hack to construct a
	// large key to represent it.
	if end == nil {
		end = ethdb.MaximumKey
	}
	return d.db.DeleteRange(start, end, d.writeOptions)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *Database) NewBatch() ethdb.Batch {
	return &batch{
		b:  d.db.NewBatch(),
		db: d,
	}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{
		b:  d.db.NewBatchWithSize(size),
		db: d,
	}
}

// upperBound returns the upper bound for the given prefix
func upperBound(prefix []byte) (limit []byte) {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c == 0xff {
			continue
		}
		limit = make([]byte, i+1)
		copy(limit, prefix)
		limit[i] = c + 1
		break
	}
	return limit
}

// Stat returns the internal metrics of Pebble in a text format. It's a developer
// method to read everything there is to read, independent of Pebble version.
func (d *Database) Stat() (string, error) {
	return d.db.Metrics().String(), nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (d *Database) Compact(start []byte, limit []byte) error {
	// There is no special flag to represent the end of key range
	// in pebble(nil in leveldb). Use an ugly hack to construct a
	// large key to represent it.
	// Note any prefixed database entry will be smaller than this
	// flag, as for trie nodes we need the 32 byte 0xff because
	// there might be a shared prefix starting with a number of
	// 0xff-s, so 32 ensures than only a hash collision could touch it.
	// https://github.com/cockroachdb/pebble/issues/2359#issuecomment-1443995833
	if limit == nil {
		limit = ethdb.MaximumKey
	}
	return d.db.Compact(context.Background(), start, limit, true) // Parallelization is preferred
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	return d.fn
}

// SyncKeyValue flushes all pending writes in the write-ahead-log to disk,
// ensuring data durability up to that point.
func (d *Database) SyncKeyValue() error {
	// The entry (value=nil) is not written to the database; it is only
	// added to the WAL. Writing this special log entry in sync mode
	// automatically flushes all previous writes, ensuring database
	// durability up to this point.
	b := d.db.NewBatch()
	b.LogData(nil, nil)
	return d.db.Apply(b, pebble.Sync)
}

// meter periodically retrieves internal pebble counters and reports them to
// the metrics subsystem.
func (d *Database) meter(refresh time.Duration, namespace string) {
	var errc chan error
	timer := time.NewTimer(refresh)
	defer timer.Stop()

	// Create storage and warning log tracer for write delay.
	var (
		compTimes  [2]int64
		compWrites [2]int64
		compReads  [2]int64

		nWrites [2]int64

		writeDelayTimes      [2]int64
		writeDelayCounts     [2]int64
		lastWriteStallReport time.Time
	)

	// Iterate ad infinitum and collect the stats
	for i := 1; errc == nil; i++ {
		var (
			compWrite int64
			compRead  int64
			nWrite    int64

			stats = d.db.Metrics()
		)
		if depth := int64(stats.Levels[0].Sublevels); depth > d.peakSublevels.Load() {
			d.peakSublevels.Store(depth)
		}
		var (
			compTime           = d.compTime.Load()
			writeDelayCount    = d.writeDelayCount.Load()
			writeDelayTime     = d.writeDelayTime.Load()
			nonLevel0CompCount = int64(d.nonLevel0Comp.Load())
			level0CompCount    = int64(d.level0Comp.Load())
		)
		writeDelayTimes[i%2] = writeDelayTime
		writeDelayCounts[i%2] = writeDelayCount
		compTimes[i%2] = compTime

		for _, levelMetrics := range stats.Levels {
			nWrite += int64(levelMetrics.TableBytesCompacted)
			nWrite += int64(levelMetrics.TableBytesFlushed)
			compWrite += int64(levelMetrics.TableBytesCompacted)
			compRead += int64(levelMetrics.TableBytesRead)
		}

		nWrite += int64(stats.WAL.BytesWritten)

		compWrites[i%2] = compWrite
		compReads[i%2] = compRead
		nWrites[i%2] = nWrite

		d.writeDelayNMeter.Mark(writeDelayCounts[i%2] - writeDelayCounts[(i-1)%2])
		d.writeDelayMeter.Mark(writeDelayTimes[i%2] - writeDelayTimes[(i-1)%2])
		// Print a warning log if writing has been stalled for a while. The log will
		// be printed per minute to avoid overwhelming users.
		if d.writeStalled.Load() && writeDelayCounts[i%2] == writeDelayCounts[(i-1)%2] &&
			time.Now().After(lastWriteStallReport.Add(degradationWarnInterval)) {
			d.log.Warn("Database compacting, degraded performance")
			lastWriteStallReport = time.Now()
		}
		d.compTimeMeter.Mark(compTimes[i%2] - compTimes[(i-1)%2])
		d.compReadMeter.Mark(compReads[i%2] - compReads[(i-1)%2])
		d.compWriteMeter.Mark(compWrites[i%2] - compWrites[(i-1)%2])
		d.diskSizeGauge.Update(int64(stats.DiskSpaceUsage()))
		d.diskReadMeter.Mark(0) // pebble doesn't track non-compaction reads
		d.diskWriteMeter.Mark(nWrites[i%2] - nWrites[(i-1)%2])

		// See https://github.com/cockroachdb/pebble/pull/1628#pullrequestreview-1026664054
		manuallyAllocated := stats.BlockCache.Size + int64(stats.MemTable.Size) + int64(stats.MemTable.ZombieSize)
		d.manualMemAllocGauge.Update(manuallyAllocated)
		d.memCompGauge.Update(stats.Flush.Count)
		d.nonlevel0CompGauge.Update(nonLevel0CompCount)
		d.level0CompGauge.Update(level0CompCount)
		d.seekCompGauge.Update(stats.Compact.ReadCount)
		d.liveCompGauge.Update(stats.Compact.NumInProgress)
		d.liveCompSizeGauge.Update(stats.Compact.InProgressBytes)
		d.liveIterGauge.Update(stats.TableIters)

		d.liveMemTablesGauge.Update(stats.MemTable.Count)
		d.zombieMemTablesGauge.Update(stats.MemTable.ZombieCount)
		d.estimatedCompDebtGauge.Update(int64(stats.Compact.EstimatedDebt))
		d.tableCacheHitGauge.Update(stats.FileCache.Hits)
		d.tableCacheMissGauge.Update(stats.FileCache.Misses)
		d.blockCacheHitGauge.Update(stats.BlockCache.Hits)
		d.blockCacheMissGauge.Update(stats.BlockCache.Misses)
		d.filterHitGauge.Update(stats.Filter.Hits)
		d.filterMissGauge.Update(stats.Filter.Misses)

		for i, level := range stats.Levels {
			// Append metrics for additional layers
			if i >= len(d.levelsGauge) {
				d.levelsGauge = append(d.levelsGauge, metrics.GetOrRegisterGauge(namespace+fmt.Sprintf("tables/level%v", i), nil))
			}
			d.levelsGauge[i].Update(level.TablesCount)
		}

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-d.quitChan:
			// Quit requesting, stop hammering the database
		case <-timer.C:
			timer.Reset(refresh)
			// Timeout, gather a new set of stats
		}
	}
	errc <- nil
}

// batch is a write-only batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	b    *pebble.Batch
	db   *Database
	size int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	if err := b.b.Set(key, value, nil); err != nil {
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	if err := b.b.Delete(key, nil); err != nil {
		return err
	}
	b.size += len(key)
	return nil
}

// DeleteRange removes all keys in the range [start, end) from the batch for
// later committing, inclusive on start, exclusive on end.
func (b *batch) DeleteRange(start, end []byte) error {
	// There is no special flag to represent the end of key range
	// in pebble(nil in leveldb). Use an ugly hack to construct a
	// large key to represent it.
	if end == nil {
		end = ethdb.MaximumKey
	}
	if err := b.b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	// Approximate size impact - just the keys
	b.size += len(start) + len(end)
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	b.db.quitLock.RLock()
	defer b.db.quitLock.RUnlock()
	if b.db.closed {
		return pebble.ErrClosed
	}
	return b.b.Commit(b.db.writeOptions)
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.b.Reset()
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	reader := b.b.Reader()
	for {
		kind, k, v, ok, err := reader.Next()
		if !ok || err != nil {
			return err
		}
		// The (k,v) slices might be overwritten if the batch is reset/reused,
		// and the receiver should copy them if they are to be retained long-term.
		if kind == pebble.InternalKeyKindSet {
			if err = w.Put(k, v); err != nil {
				return err
			}
		} else if kind == pebble.InternalKeyKindDelete {
			if err = w.Delete(k); err != nil {
				return err
			}
		} else if kind == pebble.InternalKeyKindRangeDelete {
			// For range deletion, k is the start key and v is the end key
			if rangeDeleter, ok := w.(ethdb.KeyValueRangeDeleter); ok {
				if err = rangeDeleter.DeleteRange(k, v); err != nil {
					return err
				}
			} else {
				return errors.New("ethdb.KeyValueWriter does not implement DeleteRange")
			}
		} else {
			return fmt.Errorf("unhandled operation, keytype: %v", kind)
		}
	}
}

// Close closes the batch and releases all associated resources. After it is
// closed, any subsequent operations on this batch are undefined.
func (b *batch) Close() {
	b.b.Close()
}

// pebbleIterator is a wrapper of underlying iterator in storage engine.
// The purpose of this structure is to implement the missing APIs.
//
// The pebble iterator is not thread-safe.
type pebbleIterator struct {
	iter     *pebble.Iterator
	moved    bool
	released bool
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iter, _ := d.db.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})
	iter.First()
	return &pebbleIterator{iter: iter, moved: true, released: false}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (iter *pebbleIterator) Next() bool {
	if iter.moved {
		iter.moved = false
		return iter.iter.Valid()
	}
	return iter.iter.Next()
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error.
func (iter *pebbleIterator) Error() error {
	return iter.iter.Error()
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (iter *pebbleIterator) Key() []byte {
	return iter.iter.Key()
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (iter *pebbleIterator) Value() []byte {
	return iter.iter.Value()
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (iter *pebbleIterator) Release() {
	if !iter.released {
		iter.iter.Close()
		iter.released = true
	}
}

// LevelStat describes one level's shape and the work done on it.
type LevelStat struct {
	Level          int
	Files          int64
	Size           int64
	Sublevels      int32  // only meaningful for L0
	BytesFlushed   uint64 // written into this level by flushes
	BytesCompacted uint64 // written into this level by compactions
	BytesRead      uint64 // read out of this level by compactions
	BytesMoved     uint64 // relinked into this level without rewriting
}

// CompactionStats summarises where a database's writes went. All counters are
// cumulative since the database was opened.
type CompactionStats struct {
	// Totals.
	WALBytes     uint64 // logical bytes accepted, the denominator of write amplification
	DiskBytes    uint64 // physical bytes written: the WAL plus every sstable
	ReadBytes    uint64 // bytes read back by compactions
	FlushedBytes uint64 // sstable bytes produced by flushes
	FlushCount   int64

	// Written bytes by stage, taken from each level's own counter rather than
	// from compaction inputs, so that the stages sum to DiskBytes exactly.
	// Compaction inputs overstate what lands on disk, because overwritten keys
	// and tombstones are dropped in the merge.
	IntraL0WrittenBytes uint64 // rewritten within L0
	AboveLbaseBytes     uint64 // written into levels shallower than the current base
	LbaseWrittenBytes   uint64 // written into the base level, ie the L0 drain
	CascadeWrittenBytes uint64 // written into every level below the base

	// The L0 to base level step.
	L0DrainBytes     uint64 // L0 bytes consumed by L0->Lbase compactions
	LbaseDrainBytes  uint64 // base level bytes rewritten by those compactions
	L0Drains         uint64
	L0DrainSublevels uint64 // L0 sublevels those compactions removed
	L0MoveCount      uint64 // drains that only relinked, writing nothing
	L0MoveBytes      uint64
	IntraL0Count     uint64 // compactions that rewrote L0 into itself
	IntraL0Bytes     uint64
	PeakSublevels    int64 // deepest L0 stack seen, sampled every few seconds
	BaseLevel        int   // shallowest level holding data, which is where L0 drains

	// Compaction totals.
	Compactions           int64
	MoveCompactions       int64 // relinked without rewriting, so effectively free
	MultiLevelCompactions int64
	CompactionDuration    time.Duration
	CancelledBytes        int64
	FailedCompactions     int64

	// Read side, which is what a deeper L0 trades away.
	BlockCacheHits, BlockCacheMisses int64
	FilterHits, FilterMisses         int64

	// Write stalls, and which of pebble's two gates caused them: the memtable
	// queue filling because flushing fell behind, or the L0 limit because
	// compaction did. The two call for different remedies.
	StallCount            int64
	StallDuration         time.Duration
	StallMemtableCount    int64
	StallMemtableDuration time.Duration
	StallL0Count          int64
	StallL0Duration       time.Duration

	Levels    []LevelStat    // only levels holding data or that saw work
	Sublevels []SublevelStat // L0's structure, deepest last
}

// DrainAmortisation returns how many bytes of L0 an L0->Lbase compaction pushes
// down per byte of base level it has to disturb. It is the reciprocal view of
// DrainWriteAmp and the quantity a deeper L0 is meant to improve.
func (s CompactionStats) DrainAmortisation() float64 {
	if s.LbaseDrainBytes == 0 {
		return 0
	}
	return float64(s.L0DrainBytes) / float64(s.LbaseDrainBytes)
}

// DrainWriteAmp returns the bytes an L0->Lbase compaction writes per byte of L0
// it carries down.
func (s CompactionStats) DrainWriteAmp() float64 {
	if s.L0DrainBytes == 0 {
		return 0
	}
	return float64(s.L0DrainBytes+s.LbaseDrainBytes) / float64(s.L0DrainBytes)
}

// WriteAmp returns the physical bytes written per byte accepted.
func (s CompactionStats) WriteAmp() float64 {
	if s.WALBytes == 0 {
		return 0
	}
	return float64(s.DiskBytes) / float64(s.WALBytes)
}

// CompactionStats reports how much this database has written and where those
// writes went. It is intended for benchmarks, which need the totals for a run
// rather than the sampled rates the metrics endpoint exposes.
func (d *Database) CompactionStats() CompactionStats {
	m := d.db.Metrics()

	cs := CompactionStats{
		WALBytes:              m.WAL.BytesIn,
		FlushCount:            m.Flush.Count,
		L0DrainBytes:          uint64(d.l0DrainBytes.Load()),
		LbaseDrainBytes:       uint64(d.lbaseDrainBytes.Load()),
		L0Drains:              uint64(d.l0DrainCount.Load()),
		L0DrainSublevels:      uint64(d.l0DrainSublevels.Load()),
		L0MoveCount:           uint64(d.l0MoveCount.Load()),
		L0MoveBytes:           uint64(d.l0MoveBytes.Load()),
		IntraL0Count:          uint64(d.intraL0Count.Load()),
		IntraL0Bytes:          uint64(d.intraL0Bytes.Load()),
		PeakSublevels:         d.peakSublevels.Load(),
		BaseLevel:             -1,
		Compactions:           m.Compact.Count,
		MoveCompactions:       m.Compact.MoveCount,
		MultiLevelCompactions: m.Compact.MultiLevelCount,
		CompactionDuration:    m.Compact.Duration,
		CancelledBytes:        m.Compact.CancelledBytes,
		FailedCompactions:     m.Compact.FailedCount,
		BlockCacheHits:        m.BlockCache.Hits,
		BlockCacheMisses:      m.BlockCache.Misses,
		FilterHits:            m.Filter.Hits,
		FilterMisses:          m.Filter.Misses,
		StallCount:            d.writeDelayCount.Load(),
		StallDuration:         time.Duration(d.writeDelayTime.Load()),
		StallMemtableCount:    d.stallMemtableCount.Load(),
		StallMemtableDuration: time.Duration(d.stallMemtableTime.Load()),
		StallL0Count:          d.stallL0Count.Load(),
		StallL0Duration:       time.Duration(d.stallL0Time.Load()),
	}
	var flushed, compacted uint64
	for level := range m.Levels {
		l := &m.Levels[level]
		flushed += l.TableBytesFlushed
		compacted += l.TableBytesCompacted
		cs.ReadBytes += l.TableBytesRead

		// The base level is the shallowest level below L0 holding data. Pebble
		// picks it the same way, except that it may sit one level shallower
		// while waiting for the first compaction to land there.
		if level > 0 && cs.BaseLevel < 0 && l.TablesCount > 0 {
			cs.BaseLevel = level
		}
		if l.TablesCount == 0 && l.TableBytesFlushed == 0 && l.TableBytesCompacted == 0 {
			continue
		}
		cs.Levels = append(cs.Levels, LevelStat{
			Level:          level,
			Files:          l.TablesCount,
			Size:           l.TablesSize,
			Sublevels:      l.Sublevels,
			BytesFlushed:   l.TableBytesFlushed,
			BytesCompacted: l.TableBytesCompacted,
			BytesRead:      l.TableBytesRead,
			BytesMoved:     l.TableBytesMoved,
		})
	}
	cs.FlushedBytes = flushed
	// The WAL's contribution is its logical bytes, deliberately not
	// m.WAL.BytesWritten. Despite the name that field is not a physical count:
	// pebble derives it from Levels[0].TableBytesIn plus the live WAL size, and
	// every intra-L0 compaction adds its input to TableBytesIn because L0 is
	// its output level. Using it here counted intra-L0 bytes twice, once in
	// this total and once in L0's compacted counter, which showed up as a
	// phantom WAL overhead exactly equal to the intra-L0 row. Pebble tracks no
	// physical WAL figure; the framing the logical one omits is under a percent.
	cs.DiskBytes = flushed + compacted + m.WAL.BytesIn
	cs.IntraL0WrittenBytes = m.Levels[0].TableBytesCompacted
	for level := 1; level < len(m.Levels); level++ {
		switch {
		case level == cs.BaseLevel:
			cs.LbaseWrittenBytes = m.Levels[level].TableBytesCompacted
		case level > cs.BaseLevel:
			cs.CascadeWrittenBytes += m.Levels[level].TableBytesCompacted
		default:
			// Shallower than the base level, which means the base level moved
			// down over the run and this one drained before it did. Its bytes
			// belong to no current stage, and dropping them leaves the
			// breakdown short of the total by however much passed through here
			// while it was the base.
			cs.AboveLbaseBytes += m.Levels[level].TableBytesCompacted
		}
	}
	cs.Sublevels = d.l0Sublevels()
	return cs
}

// SublevelStat describes one L0 sublevel.
type SublevelStat struct {
	Sublevel int
	Files    int64
	Size     int64
	Span     float64 // fraction of L0's key range this sublevel covers
}

// l0Sublevels reconstructs L0's sublevel structure. Pebble reports only the
// sublevel count, but the assignment is reproducible: files are placed oldest
// first, and each goes one sublevel above the highest it overlaps. Files sharing
// a sublevel therefore never overlap, which is why L0 can hold thousands of
// files at a depth of two if the workload writes across the key space in order.
func (d *Database) l0Sublevels() []SublevelStat {
	all, err := d.db.SSTables()
	if err != nil || len(all) == 0 || len(all[0]) == 0 {
		return nil
	}
	files := slices.Clone(all[0])
	slices.SortFunc(files, func(a, b pebble.SSTableInfo) int {
		if a.LargestSeqNum != b.LargestSeqNum {
			return cmp.Compare(a.LargestSeqNum, b.LargestSeqNum)
		}
		if a.SmallestSeqNum != b.SmallestSeqNum {
			return cmp.Compare(a.SmallestSeqNum, b.SmallestSeqNum)
		}
		return cmp.Compare(a.FileNum, b.FileNum)
	})

	// Files already placed in each sublevel, kept sorted by start key so that
	// the overlap test is a binary search rather than a scan.
	type placed struct{ lo, hi []byte }
	var levels [][]placed
	overlaps := func(ps []placed, lo, hi []byte) bool {
		i, _ := slices.BinarySearchFunc(ps, lo, func(p placed, k []byte) int {
			return bytes.Compare(p.lo, k)
		})
		// The candidate starting at or after lo, and the one before it, are the
		// only two that can overlap [lo, hi].
		if i < len(ps) && bytes.Compare(ps[i].lo, hi) <= 0 {
			return true
		}
		return i > 0 && bytes.Compare(ps[i-1].hi, lo) >= 0
	}

	var stats []SublevelStat
	var lo, hi []byte
	for _, f := range files {
		flo, fhi := f.Smallest.UserKey, f.Largest.UserKey
		if lo == nil || bytes.Compare(flo, lo) < 0 {
			lo = flo
		}
		if hi == nil || bytes.Compare(fhi, hi) > 0 {
			hi = fhi
		}
		sub := 0
		for s := len(levels) - 1; s >= 0; s-- {
			if overlaps(levels[s], flo, fhi) {
				sub = s + 1
				break
			}
		}
		for len(levels) <= sub {
			levels = append(levels, nil)
			stats = append(stats, SublevelStat{Sublevel: len(levels) - 1})
		}
		i, _ := slices.BinarySearchFunc(levels[sub], flo, func(p placed, k []byte) int {
			return bytes.Compare(p.lo, k)
		})
		levels[sub] = slices.Insert(levels[sub], i, placed{flo, fhi})
		stats[sub].Files++
		stats[sub].Size += int64(f.Size)
	}

	// Report each sublevel's coverage as a fraction of L0's whole key range,
	// which separates a sublevel tiling the key space from a narrow column.
	total := keyFraction(lo, hi, lo, hi)
	for s := range stats {
		var covered float64
		for _, p := range levels[s] {
			covered += keyFraction(p.lo, p.hi, lo, hi)
		}
		if total > 0 {
			stats[s].Span = covered / total
		}
	}
	return stats
}

// keyFraction approximates what share of [lo, hi] the range [a, b] covers, using
// the leading bytes of each key. Trie node keys are hashes, so the leading bytes
// are close to uniformly distributed and this is a fair estimate.
func keyFraction(a, b, lo, hi []byte) float64 {
	pos := func(k []byte) float64 {
		var v, scale float64 = 0, 1
		for i := 0; i < 8; i++ {
			scale /= 256
			if i < len(k) {
				v += float64(k[i]) * scale
			}
		}
		return v
	}
	span := pos(hi) - pos(lo)
	if span <= 0 {
		return 0
	}
	return (pos(b) - pos(a)) / span
}

// l0OverlapDepth returns the largest number of the given tables that overlap at
// any single point of the key space.
//
// Tables within one L0 sublevel never overlap each other, so for a set of L0
// tables this is exactly the number of sublevels they span, which is what an
// L0->Lbase compaction removes from the read path.
func l0OverlapDepth(tables []pebble.TableInfo) int {
	if len(tables) == 0 {
		return 0
	}
	starts := make([][]byte, len(tables))
	ends := make([][]byte, len(tables))
	for i, t := range tables {
		starts[i] = t.Smallest.UserKey
		ends[i] = t.Largest.UserKey
	}
	slices.SortFunc(starts, bytes.Compare)
	slices.SortFunc(ends, bytes.Compare)

	var depth, deepest, retired int
	for _, start := range starts {
		for retired < len(ends) && bytes.Compare(ends[retired], start) < 0 {
			depth--
			retired++
		}
		depth++
		deepest = max(deepest, depth)
	}
	return deepest
}

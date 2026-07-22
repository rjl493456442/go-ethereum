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

package pathdb

import (
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// commitStatsAccumulator accumulates the latency breakdown of the database
// update (state commit) across the process lifetime. Unlike the metrics
// timers (which are no-op if the metrics collection is disabled), it is
// always active, cumulative and O(1) in memory, making it suitable for an
// end-of-run summary such as geth import.
var commitStatsAccumulator struct {
	updates           atomic.Int64 // number of the database updates (state commits)
	difflayerNs       atomic.Int64 // summed diff layer construction time
	treeAddNs         atomic.Int64 // summed layer tree linking time (including the lookup indexing)
	treeCapNs         atomic.Int64 // summed layer tree capping time (including the disk layer commit)
	historyStateNs    atomic.Int64 // summed state history writing time
	historyTrienodeNs atomic.Int64 // summed trienode history writing time
	appendNs          atomic.Int64 // summed buffer appending time
	freezes           atomic.Int64 // number of the buffer freezing operations
	freezeNs          atomic.Int64 // summed buffer freezing time
	waitFlushNs       atomic.Int64 // summed blocking time on the previous background flush

	compactions atomic.Int64 // number of the background compaction runs
	compactNs   atomic.Int64 // summed background compaction time
	flushes     atomic.Int64 // number of the background buffer flushes
	flushNs     atomic.Int64 // summed background buffer flushing time
	flattenNs   atomic.Int64 // summed buffer flattening time (part of the flushing)
}

// CommitStats is a cumulative snapshot of the latency breakdown of the
// database updates (state commits) across the process lifetime.
type CommitStats struct {
	Updates             int64         // Number of the database updates (state commits)
	DiffLayerTime       time.Duration // Summed diff layer construction time
	TreeAddTime         time.Duration // Summed layer tree linking time (including the lookup indexing)
	TreeCapTime         time.Duration // Summed layer tree capping time (including the disk layer commit)
	HistoryStateTime    time.Duration // Summed state history writing time
	HistoryTrienodeTime time.Duration // Summed trienode history writing time
	BufferAppendTime    time.Duration // Summed buffer appending time
	Freezes             int64         // Number of the buffer freezing operations
	FreezeTime          time.Duration // Summed buffer freezing time
	WaitFlushTime       time.Duration // Summed blocking time on the previous background flush

	Compactions int64         // Number of the background compaction runs
	CompactTime time.Duration // Summed background compaction time
	Flushes     int64         // Number of the background buffer flushes
	FlushTime   time.Duration // Summed background buffer flushing time
	FlattenTime time.Duration // Summed buffer flattening time (part of the flushing)
}

// ReadCommitStats returns a cumulative snapshot of the latency breakdown of
// the database updates across the process lifetime.
func ReadCommitStats() CommitStats {
	return CommitStats{
		Updates:             commitStatsAccumulator.updates.Load(),
		DiffLayerTime:       time.Duration(commitStatsAccumulator.difflayerNs.Load()),
		TreeAddTime:         time.Duration(commitStatsAccumulator.treeAddNs.Load()),
		TreeCapTime:         time.Duration(commitStatsAccumulator.treeCapNs.Load()),
		HistoryStateTime:    time.Duration(commitStatsAccumulator.historyStateNs.Load()),
		HistoryTrienodeTime: time.Duration(commitStatsAccumulator.historyTrienodeNs.Load()),
		BufferAppendTime:    time.Duration(commitStatsAccumulator.appendNs.Load()),
		Freezes:             commitStatsAccumulator.freezes.Load(),
		FreezeTime:          time.Duration(commitStatsAccumulator.freezeNs.Load()),
		WaitFlushTime:       time.Duration(commitStatsAccumulator.waitFlushNs.Load()),
		Compactions:         commitStatsAccumulator.compactions.Load(),
		CompactTime:         time.Duration(commitStatsAccumulator.compactNs.Load()),
		Flushes:             commitStatsAccumulator.flushes.Load(),
		FlushTime:           time.Duration(commitStatsAccumulator.flushNs.Load()),
		FlattenTime:         time.Duration(commitStatsAccumulator.flattenNs.Load()),
	}
}

var (
	cleanNodeHitMeter   = metrics.NewRegisteredMeter("pathdb/clean/node/hit", nil)
	cleanNodeMissMeter  = metrics.NewRegisteredMeter("pathdb/clean/node/miss", nil)
	cleanNodeReadMeter  = metrics.NewRegisteredMeter("pathdb/clean/node/read", nil)
	cleanNodeWriteMeter = metrics.NewRegisteredMeter("pathdb/clean/node/write", nil)

	cleanStateHitMeter   = metrics.NewRegisteredMeter("pathdb/clean/state/hit", nil)
	cleanStateMissMeter  = metrics.NewRegisteredMeter("pathdb/clean/state/miss", nil)
	cleanStateReadMeter  = metrics.NewRegisteredMeter("pathdb/clean/state/read", nil)
	cleanStateWriteMeter = metrics.NewRegisteredMeter("pathdb/clean/state/write", nil)

	dirtyNodeHitMeter     = metrics.NewRegisteredMeter("pathdb/dirty/node/hit", nil)
	dirtyNodeMissMeter    = metrics.NewRegisteredMeter("pathdb/dirty/node/miss", nil)
	dirtyNodeReadMeter    = metrics.NewRegisteredMeter("pathdb/dirty/node/read", nil)
	dirtyNodeWriteMeter   = metrics.NewRegisteredMeter("pathdb/dirty/node/write", nil)
	dirtyNodeHitDepthHist = metrics.NewRegisteredHistogram("pathdb/dirty/node/depth", nil, metrics.NewExpDecaySample(1028, 0.015))

	stateAccountInexMeter     = metrics.NewRegisteredMeter("pathdb/state/account/inex/total", nil)
	stateStorageInexMeter     = metrics.NewRegisteredMeter("pathdb/state/storage/inex/total", nil)
	stateAccountInexDiskMeter = metrics.NewRegisteredMeter("pathdb/state/account/inex/disk", nil)
	stateStorageInexDiskMeter = metrics.NewRegisteredMeter("pathdb/state/storage/inex/disk", nil)

	stateAccountExistMeter     = metrics.NewRegisteredMeter("pathdb/state/account/exist/total", nil)
	stateStorageExistMeter     = metrics.NewRegisteredMeter("pathdb/state/storage/exist/total", nil)
	stateAccountExistDiskMeter = metrics.NewRegisteredMeter("pathdb/state/account/exist/disk", nil)
	stateStorageExistDiskMeter = metrics.NewRegisteredMeter("pathdb/state/storage/exist/disk", nil)

	dirtyStateHitMeter     = metrics.NewRegisteredMeter("pathdb/dirty/state/hit", nil)
	dirtyStateMissMeter    = metrics.NewRegisteredMeter("pathdb/dirty/state/miss", nil)
	dirtyStateReadMeter    = metrics.NewRegisteredMeter("pathdb/dirty/state/read", nil)
	dirtyStateWriteMeter   = metrics.NewRegisteredMeter("pathdb/dirty/state/write", nil)
	dirtyStateHitDepthHist = metrics.NewRegisteredHistogram("pathdb/dirty/state/depth", nil, metrics.NewExpDecaySample(1028, 0.015))

	nodeCleanFalseMeter = metrics.NewRegisteredMeter("pathdb/clean/false", nil)
	nodeDirtyFalseMeter = metrics.NewRegisteredMeter("pathdb/dirty/false", nil)
	nodeDiskFalseMeter  = metrics.NewRegisteredMeter("pathdb/disk/false", nil)
	nodeDiffFalseMeter  = metrics.NewRegisteredMeter("pathdb/diff/false", nil)

	compactTimeTimer = metrics.NewRegisteredResettingTimer("pathdb/compact/time", nil)
	bufferSetsGauge  = metrics.NewRegisteredGauge("pathdb/buffer/sets", nil)

	// Timers breaking down the latency of the database update (state commit),
	// all of them are measured on the block import critical path.
	updateDiffLayerTimer       = metrics.NewRegisteredResettingTimer("pathdb/update/difflayer/time", nil)        // Construction of the node set with origins
	updateTreeAddTimer         = metrics.NewRegisteredResettingTimer("pathdb/update/add/time", nil)              // Linking the new diff layer (including the lookup indexing)
	updateTreeCapTimer         = metrics.NewRegisteredResettingTimer("pathdb/update/cap/time", nil)              // Capping the layer tree (including the disk layer commit)
	commitHistoryStateTimer    = metrics.NewRegisteredResettingTimer("pathdb/commit/history/state/time", nil)    // Writing the state history into the freezer
	commitHistoryTrienodeTimer = metrics.NewRegisteredResettingTimer("pathdb/commit/history/trienode/time", nil) // Writing the trienode history into the freezer
	commitAppendTimer          = metrics.NewRegisteredResettingTimer("pathdb/commit/append/time", nil)           // Appending the bottom-most diff layer into the buffer
	commitFreezeTimer          = metrics.NewRegisteredResettingTimer("pathdb/commit/freeze/time", nil)           // Freezing the buffer and scheduling the background flush
	commitWaitFlushTimer       = metrics.NewRegisteredResettingTimer("pathdb/commit/waitflush/time", nil)        // Blocking on the previous background buffer flush
	flushFlattenTimer          = metrics.NewRegisteredResettingTimer("pathdb/flush/flatten/time", nil)           // Flattening the frozen buffer (background)

	commitTimeTimer     = metrics.NewRegisteredResettingTimer("pathdb/commit/time", nil)
	commitNodesMeter    = metrics.NewRegisteredMeter("pathdb/commit/nodes", nil)
	commitAccountsMeter = metrics.NewRegisteredMeter("pathdb/commit/accounts", nil)
	commitStoragesMeter = metrics.NewRegisteredMeter("pathdb/commit/slots", nil)
	commitBytesMeter    = metrics.NewRegisteredMeter("pathdb/commit/bytes", nil)

	gcTrieNodeMeter      = metrics.NewRegisteredMeter("pathdb/gc/node/count", nil)
	gcTrieNodeBytesMeter = metrics.NewRegisteredMeter("pathdb/gc/node/bytes", nil)
	gcAccountMeter       = metrics.NewRegisteredMeter("pathdb/gc/account/count", nil)
	gcAccountBytesMeter  = metrics.NewRegisteredMeter("pathdb/gc/account/bytes", nil)
	gcStorageMeter       = metrics.NewRegisteredMeter("pathdb/gc/storage/count", nil)
	gcStorageBytesMeter  = metrics.NewRegisteredMeter("pathdb/gc/storage/bytes", nil)

	stateHistoryBuildTimeMeter  = metrics.NewRegisteredResettingTimer("pathdb/history/state/time", nil)
	stateHistoryDataBytesMeter  = metrics.NewRegisteredMeter("pathdb/history/state/bytes/data", nil)
	stateHistoryIndexBytesMeter = metrics.NewRegisteredMeter("pathdb/history/state/bytes/index", nil)

	trienodeHistoryBuildTimeMeter  = metrics.NewRegisteredResettingTimer("pathdb/history/trienode/time", nil)
	trienodeHistoryDataBytesMeter  = metrics.NewRegisteredMeter("pathdb/history/trienode/bytes/data", nil)
	trienodeHistoryIndexBytesMeter = metrics.NewRegisteredMeter("pathdb/history/trienode/bytes/index", nil)

	stateIndexHistoryTimer         = metrics.NewRegisteredResettingTimer("pathdb/history/state/index/time", nil)
	stateUnindexHistoryTimer       = metrics.NewRegisteredResettingTimer("pathdb/history/state/unindex/time", nil)
	statePruneHistoryIndexTimer    = metrics.NewRegisteredResettingTimer("pathdb/history/state/prune/time", nil)
	trienodeIndexHistoryTimer      = metrics.NewRegisteredResettingTimer("pathdb/history/trienode/index/time", nil)
	trienodeUnindexHistoryTimer    = metrics.NewRegisteredResettingTimer("pathdb/history/trienode/unindex/time", nil)
	trienodePruneHistoryIndexTimer = metrics.NewRegisteredResettingTimer("pathdb/history/trienode/prune/time", nil)

	lookupAddLayerTimer    = metrics.NewRegisteredResettingTimer("pathdb/lookup/add/time", nil)
	lookupRemoveLayerTimer = metrics.NewRegisteredResettingTimer("pathdb/lookup/remove/time", nil)

	historicalAccountReadTimer  = metrics.NewRegisteredResettingTimer("pathdb/history/account/reads", nil)
	historicalStorageReadTimer  = metrics.NewRegisteredResettingTimer("pathdb/history/storage/reads", nil)
	historicalTrienodeReadTimer = metrics.NewRegisteredResettingTimer("pathdb/history/trienode/reads", nil)
)

// Metrics in generation
var (
	generatedAccountMeter     = metrics.NewRegisteredMeter("pathdb/generation/account/generated", nil)
	recoveredAccountMeter     = metrics.NewRegisteredMeter("pathdb/generation/account/recovered", nil)
	wipedAccountMeter         = metrics.NewRegisteredMeter("pathdb/generation/account/wiped", nil)
	missallAccountMeter       = metrics.NewRegisteredMeter("pathdb/generation/account/missall", nil)
	generatedStorageMeter     = metrics.NewRegisteredMeter("pathdb/generation/storage/generated", nil)
	recoveredStorageMeter     = metrics.NewRegisteredMeter("pathdb/generation/storage/recovered", nil)
	wipedStorageMeter         = metrics.NewRegisteredMeter("pathdb/generation/storage/wiped", nil)
	missallStorageMeter       = metrics.NewRegisteredMeter("pathdb/generation/storage/missall", nil)
	danglingStorageMeter      = metrics.NewRegisteredMeter("pathdb/generation/storage/dangling", nil)
	successfulRangeProofMeter = metrics.NewRegisteredMeter("pathdb/generation/proof/success", nil)
	failedRangeProofMeter     = metrics.NewRegisteredMeter("pathdb/generation/proof/failure", nil)

	accountProveCounter    = metrics.NewRegisteredCounter("pathdb/generation/duration/account/prove", nil)
	accountTrieReadCounter = metrics.NewRegisteredCounter("pathdb/generation/duration/account/trieread", nil)
	accountSnapReadCounter = metrics.NewRegisteredCounter("pathdb/generation/duration/account/snapread", nil)
	accountWriteCounter    = metrics.NewRegisteredCounter("pathdb/generation/duration/account/write", nil)
	storageProveCounter    = metrics.NewRegisteredCounter("pathdb/generation/duration/storage/prove", nil)
	storageTrieReadCounter = metrics.NewRegisteredCounter("pathdb/generation/duration/storage/trieread", nil)
	storageSnapReadCounter = metrics.NewRegisteredCounter("pathdb/generation/duration/storage/snapread", nil)
	storageWriteCounter    = metrics.NewRegisteredCounter("pathdb/generation/duration/storage/write", nil)
	storageCleanCounter    = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/clean", nil)
)

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
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// commitPhase identifies one timed section of Database.Update.
type commitPhase int

const (
	phaseLayerAdd     commitPhase = iota // linking the new diff layer into the tree
	phaseLookupAdd                       // indexing the new layer in the lookup set
	phaseLookupRemove                    // unindexing the stale layers from the lookup set
	phaseStateHistory                    // writing and indexing the state history
	phaseNodeHistory                     // writing and indexing the trienode history
	phaseBufferMerge                     // merging the diff layer into the write buffer
	phaseFlushWait                       // blocking on the previous background flush
	phaseCount
)

// commitPhaseNames are the log keys of the phases, in reporting order.
var commitPhaseNames = [phaseCount]string{
	phaseLayerAdd:     "layeradd",
	phaseLookupAdd:    "lookupadd",
	phaseLookupRemove: "lookupremove",
	phaseStateHistory: "statehistory",
	phaseNodeHistory:  "nodehistory",
	phaseBufferMerge:  "buffermerge",
	phaseFlushWait:    "flushwait",
}

// commitStats records the per-phase timings of a single Database.Update. The
// aggregate is already exposed as chain/triedb/commits, which says nothing
// about where the time went; this fills that gap for the slow outliers.
//
// A nil commitStats discards everything, so the commit paths that are not
// driven by Update carry no overhead.
type commitStats struct {
	phases [phaseCount]time.Duration

	diffSize   uint64 // byte size of the flattened diff layer
	bufferSize uint64 // write buffer occupancy after the merge
	flushed    bool   // whether a buffer flush was triggered
}

// record adds the time elapsed since start to the given phase.
func (s *commitStats) record(phase commitPhase, start time.Time) {
	if s == nil {
		return
	}
	s.phases[phase] += time.Since(start)
}

// setSizes records the size of the layer being flattened and of the write
// buffer it was merged into.
func (s *commitStats) setSizes(diff, buffer uint64) {
	if s == nil {
		return
	}
	s.diffSize, s.bufferSize = diff, buffer
}

// markFlushed flags that the commit triggered a write buffer flush.
func (s *commitStats) markFlushed() {
	if s == nil {
		return
	}
	s.flushed = true
}

// log reports the breakdown of a commit that exceeded the configured threshold.
func (s *commitStats) log(total time.Duration, root common.Hash, block uint64) {
	ctx := []any{
		"root", root,
		"block", block,
		"total", common.PrettyDuration(total),
	}
	other := total
	for phase, name := range commitPhaseNames {
		ctx = append(ctx, name, common.PrettyDuration(s.phases[phase]))
		other -= s.phases[phase]
	}
	ctx = append(ctx,
		"other", common.PrettyDuration(other),
		"diff", common.StorageSize(s.diffSize),
		"buffer", common.StorageSize(s.bufferSize),
		"flushed", s.flushed,
	)
	log.Warn("Slow pathdb commit", ctx...)
}

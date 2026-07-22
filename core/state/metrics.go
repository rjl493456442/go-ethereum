// Copyright 2021 The go-ethereum Authors
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

package state

import (
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// commitPhaseAccumulator accumulates the latency breakdown of the statedb
// commits across the process lifetime, for the end-of-run summary such as
// geth import. All phases are on the block import critical path.
var commitPhaseAccumulator struct {
	commits       atomic.Int64 // number of the statedb commits
	rootNs        atomic.Int64 // summed residual IntermediateRoot time within the commit
	destructNs    atomic.Int64 // summed account destruction handling time
	trieCommitNs  atomic.Int64 // summed trie commit time (account and storage tries, parallel wall-clock)
	updateBuildNs atomic.Int64 // summed state update construction time
	dbCommitNs    atomic.Int64 // summed database commit time (including the triedb update)
	readerNs      atomic.Int64 // summed post-commit reader reconstruction time
}

// StateCommitStats is a cumulative snapshot of the latency breakdown of the
// statedb commits across the process lifetime.
type StateCommitStats struct {
	Commits         int64         // Number of the statedb commits
	RootTime        time.Duration // Summed residual IntermediateRoot time within the commit
	DestructTime    time.Duration // Summed account destruction handling time
	TrieCommitTime  time.Duration // Summed trie commit time (account and storage tries, parallel wall-clock)
	UpdateBuildTime time.Duration // Summed state update construction time
	DBCommitTime    time.Duration // Summed database commit time (including the triedb update)
	ReaderTime      time.Duration // Summed post-commit reader reconstruction time
}

// ReadStateCommitStats returns a cumulative snapshot of the latency breakdown
// of the statedb commits across the process lifetime.
func ReadStateCommitStats() StateCommitStats {
	return StateCommitStats{
		Commits:         commitPhaseAccumulator.commits.Load(),
		RootTime:        time.Duration(commitPhaseAccumulator.rootNs.Load()),
		DestructTime:    time.Duration(commitPhaseAccumulator.destructNs.Load()),
		TrieCommitTime:  time.Duration(commitPhaseAccumulator.trieCommitNs.Load()),
		UpdateBuildTime: time.Duration(commitPhaseAccumulator.updateBuildNs.Load()),
		DBCommitTime:    time.Duration(commitPhaseAccumulator.dbCommitNs.Load()),
		ReaderTime:      time.Duration(commitPhaseAccumulator.readerNs.Load()),
	}
}

var (
	accountReadMeters        = metrics.NewRegisteredMeter("state/read/account", nil)
	storageReadMeters        = metrics.NewRegisteredMeter("state/read/storage", nil)
	accountUpdatedMeter      = metrics.NewRegisteredMeter("state/update/account", nil)
	storageUpdatedMeter      = metrics.NewRegisteredMeter("state/update/storage", nil)
	accountDeletedMeter      = metrics.NewRegisteredMeter("state/delete/account", nil)
	storageDeletedMeter      = metrics.NewRegisteredMeter("state/delete/storage", nil)
	accountTrieUpdatedMeter  = metrics.NewRegisteredMeter("state/update/accountnodes", nil)
	storageTriesUpdatedMeter = metrics.NewRegisteredMeter("state/update/storagenodes", nil)
	accountTrieDeletedMeter  = metrics.NewRegisteredMeter("state/delete/accountnodes", nil)
	storageTriesDeletedMeter = metrics.NewRegisteredMeter("state/delete/storagenodes", nil)

	// Metrics of the background state prefetcher (EIP-7928 access list driven).
	//
	// The lead timer and the interrupt meter together reveal the relation
	// between the prefetching and the transaction execution: a large lead
	// time indicates the prefetching completes way ahead of the execution;
	// while a high interrupt rate with non-trivial wait time indicates the
	// execution constantly outpaces the prefetching.
	prefetchTimeTimer      = metrics.NewRegisteredResettingTimer("state/prefetch/time", nil)     // Duration of the fully completed prefetch runs
	prefetchLeadTimer      = metrics.NewRegisteredResettingTimer("state/prefetch/lead", nil)     // How long the prefetch completed before being closed
	prefetchWaitTimer      = metrics.NewRegisteredResettingTimer("state/prefetch/wait", nil)     // How long the closer blocked on terminating the prefetch
	prefetchInterruptMeter = metrics.NewRegisteredMeter("state/prefetch/interrupt", nil)         // Number of prefetch runs terminated before completion
	prefetchTaskWeightHist = metrics.NewRegisteredHistogram("state/prefetch/weight", nil, metrics.NewExpDecaySample(1028, 0.015)) // Total task weight (accounts + slots) per run
)

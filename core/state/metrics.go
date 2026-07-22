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

import "github.com/ethereum/go-ethereum/metrics"

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

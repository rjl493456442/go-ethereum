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

package triedb

import "github.com/ethereum/go-ethereum/ethdb"

const (
	trieIngestWindow = 4 * 1024 * 1024
)

// partitionBatches keeps separate SSTs for the A and O keyspaces. Each
// stream receives strictly decreasing keys across consecutive windows. Flat
// repairs use an ordinary batch, which also handles all writes on backends
// without native ingestion.
type partitionBatches struct {
	flat     ethdb.Batch
	accounts ethdb.KeyValueWriter
	storage  ethdb.KeyValueWriter
	windows  []ethdb.Batch
}

func newPartitionBatches(db ethdb.Database, ingester ethdb.Ingester, timer *partitionTimer) *partitionBatches {
	flat := timer.wrapBatch(db.NewBatchWithSize(ethdb.IdealBatchSize))
	b := &partitionBatches{
		flat:     flat,
		accounts: flat,
		storage:  flat,
	}
	if ingester != nil {
		accounts := timer.wrapBatch(ingester.NewIngestBatch(trieIngestWindow))
		storage := timer.wrapBatch(ingester.NewIngestBatch(trieIngestWindow))
		b.accounts, b.storage = accounts, storage
		b.windows = []ethdb.Batch{accounts, storage}
	}
	return b
}

func (b *partitionBatches) close() {
	for _, window := range b.windows {
		window.Close()
	}
	b.flat.Close()
}

// flush installs full windows immediately. On the last call it also installs
// the short tails before the generator returns its partition root.
func (b *partitionBatches) flush(final bool) (bool, error) {
	var flushed bool
	for _, window := range b.windows {
		if size := window.ValueSize(); size == 0 || (!final && size < trieIngestWindow) {
			continue
		}
		if err := window.Write(); err != nil {
			return false, err
		}
		window.Reset()
		flushed = true
	}
	if size := b.flat.ValueSize(); size > ethdb.IdealBatchSize || (final && size > 0) {
		if err := b.flat.Write(); err != nil {
			return false, err
		}
		b.flat.Reset()
		flushed = true
	}
	return flushed, nil
}

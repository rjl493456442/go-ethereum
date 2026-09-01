// Copyright 2025 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

// timedKV wraps a key-value store and accumulates how long callers spend inside
// it. Work is spread over many goroutines, so the totals are aggregates across
// all of them and can exceed the wall clock.
type timedKV struct {
	ethdb.KeyValueStore

	writeTime, writeOps atomic.Int64
	readTime, readOps   atomic.Int64
	iterTime, iterOps   atomic.Int64
}

func (t *timedKV) Get(key []byte) ([]byte, error) {
	defer t.observe(&t.readTime, &t.readOps, time.Now())
	return t.KeyValueStore.Get(key)
}

func (t *timedKV) Has(key []byte) (bool, error) {
	defer t.observe(&t.readTime, &t.readOps, time.Now())
	return t.KeyValueStore.Has(key)
}

func (t *timedKV) Put(key, value []byte) error {
	defer t.observe(&t.writeTime, &t.writeOps, time.Now())
	return t.KeyValueStore.Put(key, value)
}

func (t *timedKV) Delete(key []byte) error {
	defer t.observe(&t.writeTime, &t.writeOps, time.Now())
	return t.KeyValueStore.Delete(key)
}

func (t *timedKV) NewBatch() ethdb.Batch {
	return &timedBatch{Batch: t.KeyValueStore.NewBatch(), kv: t}
}

func (t *timedKV) NewBatchWithSize(size int) ethdb.Batch {
	return &timedBatch{Batch: t.KeyValueStore.NewBatchWithSize(size), kv: t}
}

func (t *timedKV) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &timedIterator{Iterator: t.KeyValueStore.NewIterator(prefix, start), kv: t}
}

func (t *timedKV) observe(dst, count *atomic.Int64, start time.Time) {
	dst.Add(int64(time.Since(start)))
	count.Add(1)
}

// timedBatch times only Write. Staging into a batch is pure memory work and
// belongs with the computation, not with the database.
type timedBatch struct {
	ethdb.Batch
	kv *timedKV
}

func (b *timedBatch) Write() error {
	defer b.kv.observe(&b.kv.writeTime, &b.kv.writeOps, time.Now())
	return b.Batch.Write()
}

type timedIterator struct {
	ethdb.Iterator
	kv *timedKV
}

func (i *timedIterator) Next() bool {
	defer i.kv.observe(&i.kv.iterTime, &i.kv.iterOps, time.Now())
	return i.Iterator.Next()
}

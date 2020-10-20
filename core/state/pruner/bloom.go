// Copyright 2020 The go-ethereum Authors
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

package pruner

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/steakknife/bloomfilter"
)

// stateBloomHasher is a wrapper around a byte blob to satisfy the interface API
// requirements of the bloom library used. It's used to convert a trie hash or
// contract code hash into a 64 bit mini hash.
type stateBloomHasher []byte

func (f stateBloomHasher) Write(p []byte) (n int, err error) { panic("not implemented") }
func (f stateBloomHasher) Sum(b []byte) []byte               { panic("not implemented") }
func (f stateBloomHasher) Reset()                            { panic("not implemented") }
func (f stateBloomHasher) BlockSize() int                    { panic("not implemented") }
func (f stateBloomHasher) Size() int                         { return 8 }
func (f stateBloomHasher) Sum64() uint64                     { return binary.BigEndian.Uint64(f) }

// StateBloom is a bloom filter used during fast sync to quickly decide if a trie
// node or contract code already exists on disk or not. It self populates from the
// provided disk database on creation in a background thread and will only start
// returning live results once that's finished.

// StateBloom is a bloom filter used during the state convesion(snapshot->state).
// The keys of all generated entries will be recorded here so that in the pruning
// stage the entries belong to the specific version can be avoided for deletion.
//
// The false-positive is allowed here. The "false-positive" entries means they
// actually don't belong to the specific version but they are not deleted in the
// pruning. The downside of the false-positive allowance is we may leave some "dangling"
// nodes in the disk. But in practice the it's very unlike the dangling node is
// state root. So in theory this pruned state shouldn't be visited anymore. Another
// potential issue is for fast sync. If we do another fast sync upon the pruned
// database, it's problematic which will stop the expansion during the syncing.
// TODO address it @rjl493456442 @holiman @karalabe.
//
// After the entire state is generated, the bloom filter should be persisted into
// the disk. It indicates the whole generation procedure is finished.
type StateBloom struct {
	bloom *bloomfilter.Filter
	done  uint32
}

// NewStateBloom creates a brand new state bloom for state generation.
// The optimal bloom filter will be created by the passing "max entries"
// and the "estimated" maximum collision rate.
func NewStateBloom(entries uint64, collision float64) (*StateBloom, error) {
	bloom, err := bloomfilter.NewOptimal(entries, collision)
	if err != nil {
		return nil, err
	}
	log.Info("Initialized state bloom", "size", common.StorageSize(float64(bloom.M()/8)))
	return &StateBloom{
		bloom: bloom,
		done:  0,
	}, nil
}

// NewStateBloomFromDisk loads the state bloom from the given file.
// In this case the assumption is held the bloom filter is complete.
func NewStateBloomFromDisk(filename string) (*StateBloom, error) {
	bloom, _, err := bloomfilter.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return &StateBloom{
		bloom: bloom,
		done:  1,
	}, nil
}

// Commit flushes the bloom filter content into the disk and marks the bloom
// as complete.
func (bloom *StateBloom) Commit(filename string) error {
	if atomic.CompareAndSwapUint32(&bloom.done, 0, 1) {
		// Firstly, flush the bloom filter to the temporary file
		_, err := bloom.bloom.WriteFile(filename + ".tmp")
		if err != nil {
			return err
		}
		// Secondly, rename the file.
		if err := rename(filename+".tmp", filename); err != nil {
			return err
		}
		// Lastly, fsync the directory to ensure all pending
		// rename operations are transferred to disk
		if err := syncDir(filepath.Dir(filename)); err != nil {
			return err
		}
		return nil
	}
	return errors.New("bloom filter is committed")
}

// Put implements the KeyValueWriter interface. But here only the key is needed.
func (bloom *StateBloom) Put(key []byte, value []byte) error {
	// If the key length is not 32bytes, ensure it's contract code
	// entry with new scheme.
	if len(key) != common.HashLength {
		isCode, codeKey := rawdb.IsCodeKey(key)
		if !isCode {
			return errors.New("invalid entry")
		}
		bloom.bloom.Add(stateBloomHasher(codeKey))
		return nil
	}
	bloom.bloom.Add(stateBloomHasher(key))
	return nil
}

// Delete removes the key from the key-value data store.
func (bloom *StateBloom) Delete(key []byte) error { panic("not supported") }

// Contain is the wrapper of the underlying contains function which
// reports whether the key is contained.
// - If it says yes, the key may be contained
// - If it says no, the key is definitely not contained.
func (bloom *StateBloom) Contain(key []byte) (bool, error) {
	return bloom.bloom.Contains(stateBloomHasher(key)), nil
}

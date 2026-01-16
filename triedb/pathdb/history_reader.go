// Copyright 2025 The go-ethereum Authors
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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/

package pathdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// historyReader is the structure to access historic state data.
type historyReader struct {
	disk    ethdb.KeyValueReader
	freezer ethdb.AncientReader
}

// newHistoryReader constructs the history reader with the supplied db.
func newHistoryReader(disk ethdb.KeyValueReader, freezer ethdb.AncientReader) *historyReader {
	return &historyReader{
		disk:    disk,
		freezer: freezer,
	}
}

// readAccountMetadata resolves the account metadata within the specified
// state history.
func (r *historyReader) readAccountMetadata(address common.Address, historyID uint64) ([]byte, error) {
	blob := rawdb.ReadStateAccountIndex(r.freezer, historyID)
	if len(blob) == 0 {
		return nil, fmt.Errorf("account index is truncated, historyID: %d", historyID)
	}
	if len(blob)%accountIndexSize != 0 {
		return nil, fmt.Errorf("account index is corrupted, historyID: %d, size: %d", historyID, len(blob))
	}
	n := len(blob) / accountIndexSize

	pos := sort.Search(n, func(i int) bool {
		h := blob[accountIndexSize*i : accountIndexSize*i+common.AddressLength]
		return bytes.Compare(h, address.Bytes()) >= 0
	})
	if pos == n {
		return nil, fmt.Errorf("account %#x is not found", address)
	}
	offset := accountIndexSize * pos
	if address != common.BytesToAddress(blob[offset:offset+common.AddressLength]) {
		return nil, fmt.Errorf("account %#x is not found", address)
	}
	return blob[offset : accountIndexSize*(pos+1)], nil
}

// readStorageMetadata resolves the storage slot metadata within the specified
// state history.
func (r *historyReader) readStorageMetadata(storageKey common.Hash, storageHash common.Hash, historyID uint64, slotOffset, slotNumber int) ([]byte, error) {
	data, err := rawdb.ReadStateStorageIndex(r.freezer, historyID, slotIndexSize*slotOffset, slotIndexSize*slotNumber)
	if err != nil {
		msg := fmt.Sprintf("id: %d, slot-offset: %d, slot-length: %d", historyID, slotOffset, slotNumber)
		return nil, fmt.Errorf("storage indices corrupted, %s, %w", msg, err)
	}
	// TODO(rj493456442) get rid of the metadata resolution
	var (
		m      meta
		target common.Hash
	)
	blob := rawdb.ReadStateHistoryMeta(r.freezer, historyID)
	if err := m.decode(blob); err != nil {
		return nil, err
	}
	if m.version == stateHistoryV0 {
		target = storageHash
	} else {
		target = storageKey
	}
	pos := sort.Search(slotNumber, func(i int) bool {
		slotID := data[slotIndexSize*i : slotIndexSize*i+common.HashLength]
		return bytes.Compare(slotID, target.Bytes()) >= 0
	})
	if pos == slotNumber {
		return nil, fmt.Errorf("storage metadata is not found, slot key: %#x, historyID: %d", storageKey, historyID)
	}
	offset := slotIndexSize * pos
	if target != common.BytesToHash(data[offset:offset+common.HashLength]) {
		return nil, fmt.Errorf("storage metadata is not found, slot key: %#x, historyID: %d", storageKey, historyID)
	}
	return data[offset : slotIndexSize*(pos+1)], nil
}

// readAccount retrieves the account data from the specified state history.
func (r *historyReader) readAccount(address common.Address, historyID uint64) ([]byte, error) {
	metadata, err := r.readAccountMetadata(address, historyID)
	if err != nil {
		return nil, err
	}
	length := int(metadata[common.AddressLength])                                                     // one byte for account data length
	offset := int(binary.BigEndian.Uint32(metadata[common.AddressLength+1 : common.AddressLength+5])) // four bytes for the account data offset

	data, err := rawdb.ReadStateAccountHistory(r.freezer, historyID, offset, length)
	if err != nil {
		return nil, fmt.Errorf("account data is truncated, address: %#x, historyID: %d, size: %d, offset: %d, len: %d", address, historyID, len(data), offset, length)
	}
	return data, nil
}

// readStorage retrieves the storage slot data from the specified state history.
func (r *historyReader) readStorage(address common.Address, storageKey common.Hash, storageHash common.Hash, historyID uint64) ([]byte, error) {
	metadata, err := r.readAccountMetadata(address, historyID)
	if err != nil {
		return nil, err
	}
	// slotIndexOffset:
	//   The offset of storage indices associated with the specified account.
	// slotIndexNumber:
	//   The number of storage indices associated with the specified account.
	slotIndexOffset := int(binary.BigEndian.Uint32(metadata[common.AddressLength+5 : common.AddressLength+9]))
	slotIndexNumber := int(binary.BigEndian.Uint32(metadata[common.AddressLength+9 : common.AddressLength+13]))

	slotMetadata, err := r.readStorageMetadata(storageKey, storageHash, historyID, slotIndexOffset, slotIndexNumber)
	if err != nil {
		return nil, err
	}
	length := int(slotMetadata[common.HashLength])                                                  // one byte for slot data length
	offset := int(binary.BigEndian.Uint32(slotMetadata[common.HashLength+1 : common.HashLength+5])) // four bytes for slot data offset

	data, err := rawdb.ReadStateStorageHistory(r.freezer, historyID, offset, length)
	if err != nil {
		return nil, fmt.Errorf("storage data is truncated, address: %#x, key: %#x, historyID: %d, size: %d, offset: %d, len: %d", address, storageKey, historyID, len(data), offset, length)
	}
	return data, nil
}

// read retrieves the state element data associated with the stateID.
// stateID: represents the ID of the state of the specified version;
// lastID: represents the ID of the latest/newest state history;
// latestValue: represents the state value at the current disk layer with ID == lastID;
func (r *historyReader) read(state stateIdentQuery, stateID uint64, lastID uint64, latestValue []byte) ([]byte, error) {
	tail, err := r.freezer.Tail()
	if err != nil {
		return nil, err
	} // firstID = tail+1

	// stateID+1 == firstID (tail+1) is allowed, as all the subsequent state histories
	// are present with no gap inside.
	if stateID < tail {
		return nil, fmt.Errorf("historical state has been pruned, first: %d, state: %d", tail+1, stateID)
	}

	// To serve the request, all state histories from stateID+1 to lastID
	// must be indexed. It's not supposed to happen unless system is very
	// wrong.
	metadata := loadIndexMetadata(r.disk, toHistoryType(state.typ))
	if metadata == nil || metadata.Last < lastID {
		indexed := "null"
		if metadata != nil {
			indexed = fmt.Sprintf("%d", metadata.Last)
		}
		return nil, fmt.Errorf("state history is not fully indexed, requested: %d, indexed: %s", stateID, indexed)
	}

	// Construct an index reader to locate the corresponding history for state
	// retrieval. While it is theoretically possible to cache resolved readers,
	// in practice the state is typically accessed only once for a specific
	// historical state (e.g. by a chain tracer or an RPC request).
	//
	// Moreover, the historical state reader operates asynchronously with the
	// history indexer, so cached readers may become stale quickly. For the sake
	// of simplicity, readers are not cached.
	ir, err := newIndexReader(r.disk, state.stateIdent, 0)
	if err != nil {
		return nil, err
	}
	historyID, err := ir.readGreaterThan(stateID)
	if err != nil {
		return nil, err
	}
	// The state was not found in the state histories, as it has not been modified
	// since stateID. Use the data from the associated disk layer instead.
	if historyID == math.MaxUint64 {
		return latestValue, nil
	}
	if state.typ == typeAccount {
		return r.readAccount(state.address, historyID)
	}
	return r.readStorage(state.address, state.storageKey, state.storageHash, historyID)
}

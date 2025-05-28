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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

var stateRecordPrefix = []byte("Q")

// StateRecord represents a snapshot of state metric at a specific block
type StateRecord struct {
	Number       uint64      // Block number corresponding to this state snapshot
	Hash         common.Hash // Block hash corresponding to this state snapshot
	Timestamp    uint64      // Timestamp indicating when the block was produced
	TotalGasUsed *big.Int    // Cumulative gas used across all transactions in the blockchain up to this block

	Accounts    uint64 // Total number of accounts present in the state at this block
	AccountSize uint64 // Combined size of all accounts in the state, with 20 bytes address as the identifier
	Storages    uint64 // Total number of storage entries across all accounts in the state at this block
	StorageSize uint64 // Combined size of all storage entries, with 32 bytes key as the identifier

	Trienodes    uint64 // Total number of trie nodes present in the state at this block
	TrienodeSize uint64 // Combined size of all trie nodes, with varying size node path as the identifier (up to 64 bytes)

	Codes     uint64 // Total number of contract codes present in the state at this block, with 32 bytes hash as the identifier
	CodeSizes uint64 // Combined size of all contract codes in the state, with 20 bytes address as the identifier
}

func writeStateRecord(db ethdb.KeyValueWriter, record *StateRecord) {
	enc, err := rlp.EncodeToBytes(record)
	if err != nil {
		panic(err)
	}
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, record.Number)

	err = db.Put(append(append(stateRecordPrefix, num...), record.Hash.Bytes()...), enc)
	if err != nil {
		panic(err)
	}
}

func readStateRecord(db ethdb.KeyValueReader, number uint64, hash common.Hash) *StateRecord {
	if number == 0 {
		return &StateRecord{Number: 0, Hash: hash, TotalGasUsed: big.NewInt(0)}
	}
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, number)

	blob, err := db.Get(append(append(stateRecordPrefix, num...), hash.Bytes()...))
	if err != nil {
		log.Error("Failed to read state record from database", "hash", hash, "err", err)
		return nil
	}
	var record StateRecord
	err = rlp.DecodeBytes(blob, &record)
	if err != nil {
		log.Error("Failed to decode state record", "hash", hash, "err", err)
		return nil
	}
	return &record
}

func ReadRecords(db ethdb.KeyValueStore) ([]*StateRecord, error) {
	it := db.NewIterator(stateRecordPrefix, nil)
	defer it.Release()

	var records []*StateRecord
	for it.Next() {
		if len(it.Key()) != len(stateRecordPrefix)+8+common.HashLength {
			continue
		}
		var r StateRecord
		err := rlp.DecodeBytes(it.Value(), &r)
		if err != nil {
			return nil, err
		}
		records = append(records, &r)
	}
	return records, nil
}

type stateTracker struct {
	db       ethdb.KeyValueStore
	snapshot *snapshot.Tree
	chain    *BlockChain
}

func newStateTracker(chain *BlockChain, snapshot *snapshot.Tree) *stateTracker {
	t := &stateTracker{
		db:       chain.db,
		snapshot: snapshot,
		chain:    chain,
	}
	t.init()
	go t.run()
	return t
}

func (t *stateTracker) process(ev ChainEventWithUpdate) {
	parent := readStateRecord(t.db, ev.Header.Number.Uint64()-1, ev.Header.ParentHash)
	if parent == nil {
		return
	}
	var (
		accountDiff int
		storageDiff int
		nodeDiff    int
		codeDiff    int

		accounts = parent.Accounts
		storages = parent.Storages
		trinodes = parent.Trienodes
		codes    = parent.Codes
	)
	for addr, oldValue := range ev.Update.AccountsOrigin {
		addrHash := crypto.Keccak256Hash(addr.Bytes())
		newValue, exists := ev.Update.Accounts[addrHash]
		if !exists {
			panic("state update is invalid")
		}
		if len(newValue) == 0 {
			accounts -= 1
			accountDiff -= common.AddressLength
		}
		if len(oldValue) == 0 {
			accounts += 1
			accountDiff += common.AddressLength
		}
		accountDiff += len(newValue) - len(oldValue)
	}
	for addr, slots := range ev.Update.StoragesOrigin {
		addrHash := crypto.Keccak256Hash(addr.Bytes())
		subset, exists := ev.Update.Storages[addrHash]
		if !exists {
			panic("state update is invalid")
		}
		for key, oldValue := range slots {
			var (
				exists   bool
				newValue []byte
			)
			if ev.Update.RawStorageKey {
				newValue, exists = subset[crypto.Keccak256Hash(key.Bytes())]
			} else {
				newValue, exists = subset[key]
			}
			if !exists {
				panic("state update is invalid")
			}
			if len(newValue) == 0 {
				storages -= 1
				storageDiff -= common.HashLength
			}
			if len(oldValue) == 0 {
				storages += 1
				storageDiff += common.HashLength
			}
			storageDiff += len(newValue) - len(oldValue)
		}
	}
	for _, subset := range ev.Update.Nodes.Sets {
		for path, n := range subset.Nodes {
			if len(n.Blob) == 0 {
				trinodes -= 1
				nodeDiff -= len(path)
			}
			if len(n.Origin) == 0 {
				trinodes += 1
				nodeDiff += len(path)
			}
			nodeDiff += len(n.Blob) - len(n.Origin)
		}
	}
	for _, code := range ev.Update.Codes {
		codes += 1
		codeDiff += len(code.Blob) + common.HashLength // no deduplication
	}
	if int(parent.AccountSize)+accountDiff < 0 {
		panic(fmt.Sprintf("state update is invalid, account size %d, account diff %d", parent.AccountSize, accountDiff))
	}
	if int(parent.StorageSize)+storageDiff < 0 {
		panic(fmt.Sprintf("state update is invalid, storage size %d, account diff %d", parent.StorageSize, storageDiff))
	}
	if int(parent.TrienodeSize)+nodeDiff < 0 {
		panic(fmt.Sprintf("state update is invalid, trienode size %d, account diff %d", parent.TrienodeSize, nodeDiff))
	}
	r := &StateRecord{
		Number:       ev.Header.Number.Uint64(),
		Hash:         ev.Header.Hash(),
		Timestamp:    ev.Header.Time,
		TotalGasUsed: new(big.Int).Add(parent.TotalGasUsed, big.NewInt(int64(ev.Header.GasUsed))),

		Accounts:     accounts,
		AccountSize:  uint64(int(parent.AccountSize) + accountDiff),
		Storages:     storages,
		StorageSize:  uint64(int(parent.StorageSize) + storageDiff),
		Trienodes:    trinodes,
		TrienodeSize: uint64(int(parent.TrienodeSize) + nodeDiff),
		Codes:        codes,
		CodeSizes:    parent.CodeSizes + uint64(codeDiff),
	}
	writeStateRecord(t.db, r)
}

func (t *stateTracker) run() {
	var (
		ch  = make(chan ChainEventWithUpdate)
		sub = t.chain.SubscribeChainEventWithUpdate(ch)
	)
	defer sub.Unsubscribe()

	for {
		select {
		case ev := <-ch:
			t.process(ev)

		case <-sub.Err():
			return
		}
	}
}

func (t *stateTracker) init() {
	head := t.chain.CurrentBlock()
	if r := readStateRecord(t.db, head.Number.Uint64(), head.Hash()); r != nil {
		return
	}
	it, err := t.snapshot.AccountIterator(head.Root, common.Hash{})
	if err != nil {
		log.Error("Failed to get account iterator", "err", err)
		return
	}
	defer it.Release()

	var (
		accounts    int
		accountSize int
		storages    int
		storageSize int

		start  = time.Now()
		logged = time.Now()
	)
	for it.Next() {
		blob := it.Account()
		hash := it.Hash()

		accounts += 1
		accountSize += len(blob) + common.AddressLength

		account, err := types.FullAccount(blob)
		if err != nil {
			panic(err)
		}
		if account.Root != types.EmptyRootHash {
			stIt, err := t.snapshot.StorageIterator(head.Root, hash, common.Hash{})
			if err != nil {
				panic(err)
			}
			for stIt.Next() {
				storages += 1
				storageSize += len(stIt.Slot()) + common.HashLength
			}
			stIt.Release()
		}

		// Print log
		if time.Since(logged) > time.Second*8 {
			logged = time.Now()
			log.Info("Initializing state tracker",
				"accounts", accounts, "accountSize", common.StorageSize(accountSize),
				"storages", storages, "storageSize", common.StorageSize(storageSize),
				"elapsed", common.PrettyDuration(time.Since(start)))
		}
	}

	var (
		codes    int
		codeSize int

		trienodes    int
		trienodeSize int
	)
	cit := t.db.NewIterator(rawdb.CodePrefix, nil)
	for cit.Next() {
		codes += 1
		codeSize += common.HashLength + len(cit.Value())
	}
	cit.Release()

	ait := t.db.NewIterator(rawdb.TrieNodeAccountPrefix, nil)
	for ait.Next() {
		if rawdb.IsAccountTrieNode(ait.Key()) {
			trienodes += 1
			trienodeSize += len(ait.Key()[1:]) + len(ait.Value())
		}
	}
	ait.Release()

	sit := t.db.NewIterator(rawdb.TrieNodeStoragePrefix, nil)
	for sit.Next() {
		if rawdb.IsStorageTrieNode(sit.Key()) {
			trienodes += 1
			trienodeSize += len(sit.Key()[1+common.HashLength:]) + len(sit.Value())
		}
	}
	sit.Release()

	totalGasUsed := big.NewInt(0)
	for i := uint64(1); i < head.Number.Uint64(); i++ {
		header := t.chain.GetHeaderByNumber(i)
		if header == nil {
			panic(fmt.Sprintf("header is missing %d", i))
		}
		totalGasUsed.Add(totalGasUsed, new(big.Int).SetUint64(header.GasUsed))
	}
	r := &StateRecord{
		Number:       head.Number.Uint64(),
		Hash:         head.Hash(),
		Timestamp:    head.Time,
		TotalGasUsed: totalGasUsed,
		Accounts:     uint64(accounts),
		AccountSize:  uint64(accountSize),
		Storages:     uint64(storages),
		StorageSize:  uint64(storageSize),
		Trienodes:    uint64(trienodes),
		TrienodeSize: uint64(trienodeSize),
		Codes:        uint64(codes),
		CodeSizes:    uint64(codeSize),
	}
	writeStateRecord(t.db, r)

	log.Info("Initialized state tracker", "number", head.Number.Uint64(),
		"accounts", accounts, "accountSize", common.StorageSize(accountSize),
		"storages", storages, "storageSize", common.StorageSize(storageSize),
		"trienodes", trienodes, "trienodeSize", common.StorageSize(trienodeSize),
		"codes", codes, "codeSize", codeSize,
		"elapsed", common.PrettyDuration(time.Since(start)))
}

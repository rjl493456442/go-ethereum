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

package state

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/codedb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// StorageKeyType represents the type of storage key.
type StorageKeyType uint8

const (
	// typeStorageKeyHash indicates the storage identifier is the raw storage
	// key. This type is always preferred, decoupling from the key hashing
	// scheme.
	//
	// This type is only used after the Cancun fork which disallows the account
	// destruction, ensuring the raw storage key is always accessible.
	typeRawStorageKey StorageKeyType = 0

	// typeStorageKeyHash indicates that the storage identifier is the hash of the
	// storage key. This type was used prior to the Cancun fork, when account
	// destruction was still permitted, and the raw storage key was not accessible
	// through the storage iterator.
	//
	// This type is regarded as the legacy type, keeping it for the backward
	// compatibility.
	typeStorageKeyHash StorageKeyType = 1
)

// String returns the string format representation.
func (sk StorageKeyType) String() string {
	switch sk {
	case typeRawStorageKey:
		return "raw"
	case typeStorageKeyHash:
		return "hash"
	default:
		return fmt.Sprintf("unknown type: %d", sk)
	}
}

// Update represents the difference between two states resulting from state
// execution. It contains information about mutated contract codes, accounts,
// storage slots and trie nodes, along with their original values.
type Update struct {
	Accounts       map[common.Hash][]byte    // Accounts stores mutated accounts in 'slim RLP' encoding
	AccountsOrigin map[common.Address][]byte // AccountsOrigin stores the original values of mutated accounts in 'slim RLP' encoding

	// Storages stores mutated slots in 'prefix-zero-trimmed' RLP format.
	// The value is keyed by account hash and **storage slot key hash**.
	Storages map[common.Hash]map[common.Hash][]byte

	// StoragesOrigin stores the original values of mutated slots in
	// 'prefix-zero-trimmed' RLP format.
	// (a) the value is keyed by account hash and **storage slot key** if StorageKeyType is raw;
	// (b) the value is keyed by account hash and **storage slot key hash** if StorageKeyType is hash;
	StoragesOrigin map[common.Address]map[common.Hash][]byte
	StorageKeyType StorageKeyType // Storage identifier type

	Codes map[common.Address]contractCode // Codes contains the set of dirty codes
	Nodes *trienode.MergedNodeSet         // Aggregated dirty nodes caused by state changes
}

// Writer defines the interface for committing Ethereum state data, including
// contract codes, accounts, storage values, and trie nodes at the end of a state
// transition.
//
// Implementations of this interface may choose different part of state for
// committing. For instance, code writer only cares about the mutated contract
// code for writing.
//
// The Write* methods enqueue the corresponding state data for writing, while
// Commit finalizes all pending changes, completing the state transition.
type Writer interface {
	// Write is a unified function for enqueuing state data across different
	// categories. Implementations can selectively process the types of state
	// data they are interested in.
	//
	// WriteCodes inserts a list of modified (dirty) contract codes identified by
	// their corresponding code hashes. The operation may be buffered and flushed
	// asynchronously depending on the implementation.
	//
	// WriteAccounts enqueues the updated account data for committing. The accounts
	// map contains the 'slim RLP' encoded account data keyed by their address hash;
	// while `accountsOrigin` stores the original 'slim RLP' encoded account data
	// keyed by the actual account addresses.
	//
	// WriteStorages enqueues the updated storage data for committing. The storages
	// map contains the 'prefix-zero-trimmed' encoded storage data keyed by their
	// contract address hash and the **storage slot key hash**.
	//
	// The storageOrigin map contains the original 'prefix-zero-trimmed' encoded
	// storage data before the state transition.
	//
	// - the storage is keyed by account hash and **storage slot key** if rawStorageKey is true;
	// - the storage is keyed by account hash and **storage slot key hash** if rawStorageKey is false;
	//
	// WriteTrieNodes records the trie node mutations produced during state transition.
	// The `nodes` represents a merged set of trie nodes belonging to different tries
	// along with the original value before the mutation.
	Write(update *Update)

	// Commit flushes all pending writes and finalizes the state transition,
	// committing the changes to the underlying storage. It returns an error
	// if the commit fails.
	Commit() error
}

type codeDBWriter struct {
	w *codedb.Writer
}

func (w *codeDBWriter) Write(update *Update) {
	for _, code := range update.Codes {
		w.w.Put(code.hash, code.blob)
	}
}

func (w *codeDBWriter) Commit() error {
	return w.w.Commit()
}

type snapWriter struct {
	originRoot common.Hash
	root       common.Hash
	snap       *snapshot.Tree

	accounts map[common.Hash][]byte
	storages map[common.Hash]map[common.Hash][]byte
}

func newSnapWriter(originRoot common.Hash, root common.Hash, snap *snapshot.Tree) (*snapWriter, error) {
	if snap.Snapshot(originRoot) == nil {
		return nil, fmt.Errorf("base snapshot #%x is not available", originRoot)
	}
	return &snapWriter{
		originRoot: originRoot,
		root:       root,
		snap:       snap,
	}, nil
}

func (w *snapWriter) Write(update *Update) {
	w.accounts = update.Accounts
	w.storages = update.Storages
}

func (w *snapWriter) Commit() error {
	if err := w.snap.Update(w.root, w.originRoot, w.accounts, w.storages); err != nil {
		return err
	}
	// Keep 128 diff layers in the memory, persistent layer is 129th.
	// - head layer is paired with HEAD state
	// - head-1 layer is paired with HEAD-1 state
	// - head-127 layer(bottom-most diff layer) is paired with HEAD-127 state
	return w.snap.Cap(w.root, TriesInMemory)
}

type trieDBWriter struct {
	originRoot  common.Hash
	root        common.Hash
	blockNumber uint64
	triedb      *triedb.Database

	nodes  *trienode.MergedNodeSet
	states *triedb.StateSet
}

func newTrieDBWriter(originRoot common.Hash, root common.Hash, blockNumber uint64, triedb *triedb.Database) (*trieDBWriter, error) {
	_, err := triedb.NodeReader(originRoot)
	if err != nil {
		return nil, err
	}
	return &trieDBWriter{
		originRoot:  originRoot,
		root:        root,
		blockNumber: blockNumber,
		triedb:      triedb,
	}, nil
}

func (w *trieDBWriter) Write(update *Update) {
	w.nodes = update.Nodes
	w.states = &triedb.StateSet{
		Accounts:       update.Accounts,
		AccountsOrigin: update.AccountsOrigin,
		Storages:       update.Storages,
		StoragesOrigin: update.StoragesOrigin,
		RawStorageKey:  update.StorageKeyType == typeRawStorageKey,
	}
}

func (w *trieDBWriter) Commit() error {
	return w.triedb.Update(w.root, w.originRoot, w.blockNumber, w.nodes, w.states)
}

// multiWriter is the aggregation of a list of Writer interface,
// providing state access by leveraging all readers. The checking priority
// is determined by the position in the reader list.
//
// multiStateReader is safe for concurrent read and assumes all underlying
// readers are thread-safe as well.
type multiWriter struct {
	writers  []Writer // List of state readers, sorted by checking priority
	optional []Writer
}

// newMultiStateReader constructs a multiStateReader instance with the given
// readers. The priority among readers is assumed to be sorted. Note, it must
// contain at least one reader for constructing a multiStateReader.
func newMultiWriter(writers []Writer, optional ...Writer) (*multiWriter, error) {
	return &multiWriter{
		writers:  writers,
		optional: optional,
	}, nil
}

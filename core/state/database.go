// Copyright 2017 The go-ethereum Authors
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
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/gballet/go-verkle"
)

const (
	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000

	// Cache size granted for caching clean code.
	codeCacheSize = 64 * 1024 * 1024
)

// Database wraps access to tries and contract code.
type Database interface {
	// OpenTrie opens the main account trie.
	OpenTrie(root common.Hash) (Trie, error)

	// OpenStorageTrie opens the storage trie of an account.
	OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, main Trie) (Trie, error)

	// CopyTrie returns an independent copy of the given trie.
	CopyTrie(Trie) Trie

	// ContractCode retrieves a particular contract's code.
	ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error)

	// DiskDB returns the underlying key-value disk database.
	DiskDB() ethdb.KeyValueStore

	// TrieDB returns the underlying trie database for managing trie nodes.
	TrieDB() *trie.Database

	EndVerkleTransition()
}

// Trie is a Ethereum Merkle Patricia trie.
type Trie interface {
	// GetKey returns the sha3 preimage of a hashed key that was previously used
	// to store a value.
	//
	// TODO(fjl): remove this when StateTrie is removed
	GetKey([]byte) []byte

	// GetStorage returns the value for key stored in the trie. The value bytes
	// must not be modified by the caller. If a node was not found in the database,
	// a trie.MissingNodeError is returned.
	GetStorage(addr common.Address, key []byte) ([]byte, error)

	// GetAccount abstracts an account read from the trie. It retrieves the
	// account blob from the trie with provided account address and decodes it
	// with associated decoding algorithm. If the specified account is not in
	// the trie, nil will be returned. If the trie is corrupted(e.g. some nodes
	// are missing or the account blob is incorrect for decoding), an error will
	// be returned.
	GetAccount(address common.Address) (*types.StateAccount, error)

	// UpdateStorage associates key with value in the trie. If value has length zero,
	// any existing value is deleted from the trie. The value bytes must not be modified
	// by the caller while they are stored in the trie. If a node was not found in the
	// database, a trie.MissingNodeError is returned.
	UpdateStorage(addr common.Address, key, value []byte) error

	// UpdateAccount abstracts an account write to the trie. It encodes the
	// provided account object with associated algorithm and then updates it
	// in the trie with provided address.
	UpdateAccount(address common.Address, account *types.StateAccount) error

	// UpdateContractCode abstracts code write to the trie. It is expected
	// to be moved to the stateWriter interface when the latter is ready.
	UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error

	// DeleteStorage removes any existing value for key from the trie. If a node
	// was not found in the database, a trie.MissingNodeError is returned.
	DeleteStorage(addr common.Address, key []byte) error

	// DeleteAccount abstracts an account deletion from the trie.
	DeleteAccount(address common.Address) error

	// Hash returns the root hash of the trie. It does not write to the database and
	// can be used even if the trie doesn't have one.
	Hash() common.Hash

	// Commit collects all dirty nodes in the trie and replace them with the
	// corresponding node hash. All collected nodes(including dirty leaves if
	// collectLeaf is true) will be encapsulated into a nodeset for return.
	// The returned nodeset can be nil if the trie is clean(nothing to commit).
	// Once the trie is committed, it's not usable anymore. A new trie must
	// be created with new root and updated trie database for following usage
	Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error)

	// NodeIterator returns an iterator that returns nodes of the trie. Iteration
	// starts at the key after the given start key. And error will be returned
	// if fails to create node iterator.
	NodeIterator(startKey []byte) (trie.NodeIterator, error)

	// Prove constructs a Merkle proof for key. The result contains all encoded nodes
	// on the path to the value at key. The value itself is also included in the last
	// node and can be retrieved by verifying the proof.
	//
	// If the trie does not contain a value for key, the returned proof contains all
	// nodes of the longest existing prefix of the key (at least the root), ending
	// with the node that proves the absence of the key.
	Prove(key []byte, proofDb ethdb.KeyValueWriter) error

	// IsVerkle returns true if the trie is verkle-tree based
	IsVerkle() bool
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use, but does not retain any recent trie nodes in memory. To keep some
// historical state in memory, use the NewDatabaseWithConfig constructor.
func NewDatabase(db ethdb.Database) Database {
	return NewDatabaseWithConfig(db, nil)
}

// NewDatabaseWithConfig creates a backing store for state. The returned database
// is safe for concurrent use and retains a lot of collapsed RLP trie nodes in a
// large memory cache.
func NewDatabaseWithConfig(db ethdb.Database, config *trie.Config) Database {
	return &cachingDB{
		disk:          db,
		codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
		codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		triedb:        trie.NewDatabase(db, config),
		addrToPoint:   utils.NewPointCache(),
	}
}

// NewDatabaseWithNodeDB creates a state database with an already initialized node database.
func NewDatabaseWithNodeDB(db ethdb.Database, triedb *trie.Database) Database {
	return &cachingDB{
		disk:          db,
		codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
		codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		triedb:        triedb,
		addrToPoint:   utils.NewPointCache(),
		ended:         triedb.Verkle(),
	}
}

func (db *cachingDB) EndVerkleTransition() {
	if !db.started {
		db.started = true
	}

	fmt.Println(`
	__________.__                       .__                .__                   __       .__                       .__                    .___         .___
	\__    ___|  |__   ____        ____ |  |   ____ ______ |  |__ _____    _____/  |_     |  |__ _____    ______    |  | _____    ____   __| _/____   __| _/
	  |    |  |  |  \_/ __ \     _/ __ \|  | _/ __ \\____ \|  |  \\__  \  /    \   __\    |  |  \\__  \  /  ___/    |  | \__  \  /    \ / __ _/ __ \ / __ |
	  |    |  |   Y  \  ___/     \  ___/|  |_\  ___/|  |_> |   Y  \/ __ \|   |  |  |      |   Y  \/ __ \_\___ \     |  |__/ __ \|   |  / /_/ \  ___// /_/ |
	  |____|  |___|  /\___        \___  |____/\___  |   __/|___|  (____  |___|  |__|      |___|  (____  /_____/     |____(____  |___|  \____ |\___  \____ |
                                                    |__|`)
	db.ended = true
}

func (db *cachingDB) getTranslation(orig common.Hash) common.Hash {
	db.translatedRootsLock.RLock()
	defer db.translatedRootsLock.RUnlock()
	for i, o := range db.origRoots {
		if o == orig {
			return db.translatedRoots[i]
		}
	}
	return common.Hash{}
}

type cachingDB struct {
	disk          ethdb.KeyValueStore
	codeSizeCache *lru.Cache[common.Hash, int]
	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	triedb        *trie.Database

	// Verkle specific fields
	// TODO ensure that this info is in the DB
	started, ended      bool
	translatedRoots     [32]common.Hash // hash of the translated root, for opening
	origRoots           [32]common.Hash
	translatedRootsLock sync.RWMutex

	addrToPoint *utils.PointCache
}

func (db *cachingDB) openMPTTrie(root common.Hash) (Trie, error) {
	tr, err := trie.NewStateTrie(trie.StateTrieID(root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (db *cachingDB) openVKTrie(root common.Hash) (Trie, error) {
	// TODO translation from mpt root to verkle root?
	payload, err := db.DiskDB().Get(trie.FlatDBVerkleNodeKeyPrefix)
	if err != nil {
		return trie.NewVerkleTrie(verkle.New(), db.triedb, db.addrToPoint, db.ended)
	}
	r, err := verkle.ParseNode(payload, 0)
	if err != nil {
		panic(err)
	}
	return trie.NewVerkleTrie(r, db.triedb, db.addrToPoint, db.ended)
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *cachingDB) OpenTrie(root common.Hash) (Trie, error) {
	if db.ended {
		vkt, err := db.openVKTrie(root)
		if err != nil {
			return nil, err
		}

		return vkt, nil
	}
	return db.openMPTTrie(root)
}

func (db *cachingDB) openStorageMPTrie(stateRoot common.Hash, address common.Address, root common.Hash, _ Trie) (Trie, error) {
	tr, err := trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// OpenStorageTrie opens the storage trie of an account
func (db *cachingDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	if db.ended {
		return self, nil
	}
	mpt, err := db.openStorageMPTrie(stateRoot, address, root, nil)
	return mpt, err
}

// CopyTrie returns an independent copy of the given trie.
func (db *cachingDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.StateTrie:
		return t.Copy()
	case *trie.VerkleTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ContractCode retrieves a particular contract's code.
func (db *cachingDB) ContractCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	code, _ := db.codeCache.Get(codeHash)
	if len(code) > 0 {
		return code, nil
	}
	code = rawdb.ReadCode(db.disk, codeHash)
	if len(code) > 0 {
		db.codeCache.Add(codeHash, code)
		db.codeSizeCache.Add(codeHash, len(code))
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeWithPrefix retrieves a particular contract's code. If the
// code can't be found in the cache, then check the existence with **new**
// db scheme.
func (db *cachingDB) ContractCodeWithPrefix(address common.Address, codeHash common.Hash) ([]byte, error) {
	code, _ := db.codeCache.Get(codeHash)
	if len(code) > 0 {
		return code, nil
	}
	code = rawdb.ReadCodeWithPrefix(db.disk, codeHash)
	if len(code) > 0 {
		db.codeCache.Add(codeHash, code)
		db.codeSizeCache.Add(codeHash, len(code))
		return code, nil
	}
	return nil, errors.New("not found")
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *cachingDB) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	if cached, ok := db.codeSizeCache.Get(codeHash); ok {
		return cached, nil
	}
	code, err := db.ContractCode(addr, codeHash)
	return len(code), err
}

// DiskDB returns the underlying key-value disk database.
func (db *cachingDB) DiskDB() ethdb.KeyValueStore {
	return db.disk
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *cachingDB) TrieDB() *trie.Database {
	return db.triedb
}

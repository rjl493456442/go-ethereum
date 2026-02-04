package state

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
)

type trienodeReaderWithCache struct {
	reader database.NodeReader

	accountNodes map[string][]byte
	accountLock  sync.RWMutex

	storageBuckets [16]struct {
		lock     sync.RWMutex
		storages map[common.Hash]map[string][]byte
	}
}

func newTrienodeReaderWithCache(r database.NodeReader) *trienodeReaderWithCache {
	rc := &trienodeReaderWithCache{
		reader:       r,
		accountNodes: make(map[string][]byte),
	}
	for i := range rc.storageBuckets {
		rc.storageBuckets[i].storages = make(map[common.Hash]map[string][]byte)
	}
	return rc
}

func (r *trienodeReaderWithCache) accountNode(path []byte, hash common.Hash) ([]byte, bool, error) {
	pathstr := string(path)
	r.accountLock.RLock()
	if n, ok := r.accountNodes[pathstr]; ok {
		r.accountLock.RUnlock()
		return n, true, nil
	}
	r.accountLock.RUnlock()

	n, err := r.reader.Node(common.Hash{}, path, hash)
	if err != nil {
		return nil, false, err
	}
	r.accountLock.Lock()
	r.accountNodes[pathstr] = n
	r.accountLock.Unlock()
	return n, false, nil
}

func (r *trienodeReaderWithCache) storageNode(owner common.Hash, path []byte, hash common.Hash) ([]byte, bool, error) {
	var (
		value  []byte
		ok     bool
		bucket = &r.storageBuckets[owner[0]&0x0f]
	)
	// Try to resolve the requested storage slot in the local cache
	bucket.lock.RLock()
	slots, ok := bucket.storages[owner]
	if ok {
		value, ok = slots[string(path)]
	}
	bucket.lock.RUnlock()
	if ok {
		return value, true, nil
	}
	// Try to resolve the requested storage slot from the underlying reader
	value, err := r.reader.Node(owner, path, hash)
	if err != nil {
		return nil, false, err
	}
	bucket.lock.Lock()
	slots, ok = bucket.storages[owner]
	if !ok {
		slots = make(map[string][]byte)
		bucket.storages[owner] = slots
	}
	slots[string(path)] = value
	bucket.lock.Unlock()

	return value, false, nil
}

func (r *trienodeReaderWithCache) node(owner common.Hash, path []byte, hash common.Hash) ([]byte, bool, error) {
	if owner == (common.Hash{}) {
		return r.accountNode(path, hash)
	}
	return r.storageNode(owner, path, hash)
}

type trienodeReaderWithStats struct {
	reader *trienodeReaderWithCache

	accountHits   atomic.Int64
	accountMisses atomic.Int64
	storageHits   atomic.Int64
	storageMisses atomic.Int64
}

// newReaderWithStats constructs the reader with additional statistics tracked.
func newTrienodeReaderWithStats(sr *trienodeReaderWithCache) *trienodeReaderWithStats {
	return &trienodeReaderWithStats{
		reader: sr,
	}
}

func (r *trienodeReaderWithStats) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	data, hit, err := r.reader.node(owner, path, hash)
	if err != nil {
		return nil, err
	}
	if hit {
		if owner == (common.Hash{}) {
			r.accountHits.Add(1)
		} else {
			r.storageHits.Add(1)
		}
	} else {
		if owner == (common.Hash{}) {
			r.accountMisses.Add(1)
		} else {
			r.storageMisses.Add(1)
		}
	}
	return data, nil
}

func (db *CachingDB) TrieReaderWithStats(root common.Hash) (database.NodeReader, database.NodeReader, error) {
	r, err := db.TrieDB().NodeReader(root)
	if err != nil {
		return nil, nil, err
	}
	cached := newTrienodeReaderWithCache(r)

	return newTrienodeReaderWithStats(cached), newTrienodeReaderWithStats(cached), nil
}

type emptyTrienodeReader struct {
}

func (r *emptyTrienodeReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return nil, errors.New("not found")
}

type trienodeReaderOpener struct {
	*triedb.Database
	reader database.NodeReader
}

func (o *trienodeReaderOpener) NodeReader(root common.Hash) (database.NodeReader, error) {
	return o.reader, nil
}

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
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// stateBloomFileName is the filename of state bloom filter.
	stateBloomFileName = "statebloom.bf.gz"

	// bloomFilterEntries is the estimated value of the number of trie nodes
	// and codes contained in the state. It's designed for mainnet but also
	// suitable for other small testnets.
	bloomFilterEntries = 600 * 1024 * 1024

	// bloomFalsePositiveRate is the acceptable probability of bloom filter
	// false-positive. It's around 0.05%.
	bloomFalsePositiveRate = 0.0005

	// rangeCompactionThreshold is the minimal deleted entry number for
	// triggering range compaction. It's a quite arbitrary number but just
	// to avoid triggering range compaction because of small deletion.
	rangeCompactionThreshold = 100000
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256(nil)
)

type Pruner struct {
	db             ethdb.Database
	stateBloom     *StateBloom
	stateBloomPath string
	headHeader     *types.Header
	snaptree       *snapshot.Tree
}

// NewPruner creates the pruner instance.
func NewPruner(db ethdb.Database, headHeader *types.Header, homedir string) (*Pruner, error) {
	snaptree, err := snapshot.New(db, trie.NewDatabase(db), 256, headHeader.Root, false, false)
	if err != nil {
		return nil, err // The relevant snapshot(s) might not exist
	}
	// The passed parameters for constructing the bloom filter
	// is quite arbitrary. The trie nodes on mainnet is around
	// 600M nowsday. So the parameters are designed for mainnet
	// but it's also suitable for smaller networks.
	stateBloom, err := NewStateBloom(bloomFilterEntries, bloomFalsePositiveRate)
	if err != nil {
		return nil, err
	}
	return &Pruner{
		db:             db,
		stateBloom:     stateBloom,
		stateBloomPath: filepath.Join(homedir, stateBloomFileName),
		headHeader:     headHeader,
		snaptree:       snaptree,
	}, nil
}

func prune(maindb ethdb.Database, stateBloom *StateBloom, blacklist map[common.Hash]struct{}, start time.Time) error {
	// Extract all node refs belong to the genesis. We have to keep the
	// genesis all the time.
	marker, err := extractGenesis(maindb)
	if err != nil {
		return err
	}
	// Delete all stale trie nodes in the disk. With the help of state bloom
	// the trie nodes(and codes) belong to the active state will be filtered
	// out. A very small part of stale tries will also be filtered because of
	// the false-positive rate of bloom filter. But the assumption is held here
	// that the false-positive is low enough(~0.05%). The probablity of the
	// dangling node is the state root is super low. So the dangling nodes in
	// theory will never ever be visited again.
	var (
		count  int
		size   common.StorageSize
		pstart = time.Now()
		logged = time.Now()
		batch  = maindb.NewBatch()
		iter   = maindb.NewIterator(nil, nil)

		rangestart, rangelimit []byte
	)
	for iter.Next() {
		key := iter.Key()

		// All state entries don't belong to specific state and genesis are deleted here
		// - trie node
		// - legacy contract code
		// - new-scheme contract code
		isCode, codeKey := rawdb.IsCodeKey(key)
		if len(key) == common.HashLength || isCode {
			checkKey := key
			if isCode {
				checkKey = codeKey
			}
			// Filter out the state belongs to the genesis
			if _, ok := marker[common.BytesToHash(checkKey)]; ok {
				continue
			}
			// Filter out the state belongs the pruning target
			ok, err := stateBloom.Contain(checkKey)
			if err != nil {
				return err // Something very wrong
			}
			if ok {
				continue
			}
			// Filter out the state belongs to the "blacklist". Usually
			// the root of the "useless" states are contained here.
			if blacklist != nil {
				if _, ok := blacklist[common.BytesToHash(checkKey)]; ok {
					log.Info("Filter out state in blacklist", "hash", common.BytesToHash(checkKey))
					continue
				}
			}
			size += common.StorageSize(len(key) + len(iter.Value()))
			batch.Delete(key)

			if batch.ValueSize() >= ethdb.IdealBatchSize {
				batch.Write()
				batch.Reset()
			}
			count += 1
			if count%1000 == 0 && time.Since(logged) > 8*time.Second {
				log.Info("Pruning state data", "count", count, "size", size, "elapsed", common.PrettyDuration(time.Since(pstart)))
				logged = time.Now()
			}
			if rangestart == nil || bytes.Compare(rangestart, key) > 0 {
				if rangestart == nil {
					rangestart = make([]byte, common.HashLength)
				}
				copy(rangestart, key)
			}
			if rangelimit == nil || bytes.Compare(rangelimit, key) < 0 {
				if rangelimit == nil {
					rangelimit = make([]byte, common.HashLength)
				}
				copy(rangelimit, key)
			}
		}
	}
	if batch.ValueSize() > 0 {
		batch.Write()
		batch.Reset()
	}
	iter.Release() // Please release the iterator here, otherwise will block the compactor
	log.Info("Pruned state data", "count", count, "size", size, "elapsed", common.PrettyDuration(time.Since(pstart)))

	// Start compactions, will remove the deleted data from the disk immediately.
	// Note for small pruning, the compaction is skipped.
	if count >= rangeCompactionThreshold {
		cstart := time.Now()
		log.Info("Start compacting the database")
		if err := maindb.Compact(rangestart, rangelimit); err != nil {
			log.Error("Failed to compact the whole database", "error", err)
		}
		log.Info("Compacted the whole database", "elapsed", common.PrettyDuration(time.Since(cstart)))
	}
	log.Info("Successfully prune the state", "pruned", size, "elasped", common.PrettyDuration(time.Since(start)))
	return nil
}

// Prune deletes all historical state nodes except the nodes belong to the
// specified state version. If user doesn't specify the state version, use
// the persisted snapshot disk layer as the target.
func (p *Pruner) Prune(root common.Hash) error {
	// If the target state root is not specified, use the HEAD-127 as the
	// target. The reason for picking it is:
	// - in most of the normal cases, the related state is available
	// - the probability of this layer being reorg is very low
	var blacklist = make(map[common.Hash]struct{})
	if root == (common.Hash{}) {
		layer := p.snaptree.SnapshotInDepth(p.headHeader.Root, 127, func(depth int, hash common.Hash) {
			if depth != 0 {
				blacklist[hash] = struct{}{}
			}
		})
		if layer == nil {
			return errors.New("HEAD-127 layer is not available")
		}
		root = layer.Root()
		log.Info("Pick HEAD-127 as the pruning target", "root", root, "height", p.headHeader.Number.Uint64()-127)
	}
	// Ensure the root is really present. The weak assumption
	// is the presence of root can indicate the presence of the
	// entire trie.
	if blob := rawdb.ReadTrieNode(p.db, root); len(blob) == 0 {
		return fmt.Errorf("associated state[%x] is not present", root)
	}
	start := time.Now()
	// Traverse the target state, re-construct the whole state trie and
	// commit to the given bloom filter.
	if err := snapshot.CommitAndVerifyState(p.snaptree, root, p.db, p.stateBloom); err != nil {
		return err
	}
	if err := p.stateBloom.Commit(p.stateBloomPath); err != nil {
		return err
	}
	if err := prune(p.db, p.stateBloom, blacklist, start); err != nil {
		return err
	}
	os.RemoveAll(p.stateBloomPath)
	return nil
}

// RecoverPruning will resume the pruning procedure during the system restart.
// This function is used in this case: user tries to prune state data, but the
// system was interrupted midway because of crash or manual-kill. In this case
// if the bloom filter for filtering active state is already constructed, the
// pruning can be resumed. What's more if the bloom filter is constructed, the
// pruning **has to be resumed**. Otherwise a lot of dangling nodes may be left
// in the disk.
func RecoverPruning(homedir string, db ethdb.Database) error {
	stateBloomPath := filepath.Join(homedir, stateBloomFileName)
	if _, err := os.Stat(stateBloomPath); os.IsNotExist(err) {
		return nil // nothing to recover
	}
	stateBloom, err := NewStateBloomFromDisk(stateBloomPath)
	if err != nil {
		return err
	}
	if err := prune(db, stateBloom, nil, time.Now()); err != nil {
		return err
	}
	os.RemoveAll(stateBloomPath)
	return nil
}

// extractGenesis loads the genesis state and creates the nodes marker.
// So that it can be used as an present indicator for all genesis trie nodes.
func extractGenesis(db ethdb.Database) (map[common.Hash]struct{}, error) {
	genesisHash := rawdb.ReadCanonicalHash(db, 0)
	if genesisHash == (common.Hash{}) {
		return nil, errors.New("missing genesis hash")
	}
	genesis := rawdb.ReadBlock(db, genesisHash, 0)
	if genesis == nil {
		return nil, errors.New("missing genesis block")
	}
	t, err := trie.New(genesis.Root(), trie.NewDatabase(db))
	if err != nil {
		return nil, err
	}
	marker := make(map[common.Hash]struct{})
	accIter := t.NodeIterator(nil)
	for accIter.Next(true) {
		node := accIter.Hash()

		// Embeded nodes don't have hash.
		if node != (common.Hash{}) {
			marker[node] = struct{}{}
		}
		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			var acc struct {
				Nonce    uint64
				Balance  *big.Int
				Root     common.Hash
				CodeHash []byte
			}
			if err := rlp.DecodeBytes(accIter.LeafBlob(), &acc); err != nil {
				return nil, err
			}
			if acc.Root != emptyRoot {
				storageTrie, err := trie.NewSecure(acc.Root, trie.NewDatabase(db))
				if err != nil {
					return nil, err
				}
				storageIter := storageTrie.NodeIterator(nil)
				for storageIter.Next(true) {
					node := storageIter.Hash()
					if node != (common.Hash{}) {
						marker[node] = struct{}{}
					}
				}
				if storageIter.Error() != nil {
					return nil, storageIter.Error()
				}
			}
			if !bytes.Equal(acc.CodeHash, emptyCode) {
				marker[common.BytesToHash(acc.CodeHash)] = struct{}{}
			}
		}
	}
	if accIter.Error() != nil {
		return nil, accIter.Error()
	}
	return marker, nil
}

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

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb/internal"
	"golang.org/x/sync/errgroup"
)

// ErrCancelled is returned when GenerateTrie is aborted via its cancel
// channel before completing.
var ErrCancelled = internal.ErrCancelled

// GenerateStats reports per-run counters from GenerateTrie. Scanned is
// the number of accounts walked, Updated is how many had a stale Root
// field that was rewritten to match the recomputed storage root, and
// Deleted is the number of dangling storage slots removed.
type GenerateStats struct {
	Scanned int64
	Updated int64
	Deleted int64
}

// numPartitions is the number of slices the account hash space is divided
// into by GenerateTrie.
const numPartitions = 16

// Each partition covers 1/16 of the account hash space. We track progress
// by interpreting the top 8 bytes of an account hash as a uint64, so each
// partition spans 2^64 / 16 = 2^60.
const partitionRangeSize = uint64(1) << 60

// genCounters bundles the progress counters threaded through a GenerateTrie run.
type genCounters struct {
	accounts       atomic.Int64 // accounts scanned
	slots          atomic.Int64 // storage slots scanned
	accountUpdated atomic.Int64 // accounts whose stale storage Root was rewritten
	storageDeleted atomic.Int64 // dangling storage slots removed

	accountTrieNodes atomic.Int64 // generated account trie nodes
	accountTrieBytes atomic.Int64 // generated account trie bytes
	storageTrieNodes atomic.Int64 // generated storage trie nodes
	storageTrieBytes atomic.Int64 // generated storage trie bytes

	progress [numPartitions]atomic.Uint64 // distance scanned from each partition's starting bound
}

// rangeIterators bundles the per-partition account and storage iterators.
type rangeIterators struct {
	newIterator func(prefix, start []byte) ethdb.Iterator
	acct        *internal.HoldableIterator
	stor        *internal.HoldableIterator
}

func openRangeIterators(db ethdb.Database, start, end common.Hash, ingester ethdb.Ingester) *rangeIterators {
	r := &rangeIterators{
		newIterator: db.NewIterator,
	}
	accountStart, storageStart := start[:], start[:]

	if ingester != nil {
		r.newIterator = ingester.NewReverseIterator
		accountStart = end[:]
		storageStart = append(end[:], common.MaxHash[:]...)
	}
	r.acct = r.open(rawdb.SnapshotAccountPrefix, accountStart, common.HashLength)
	r.stor = r.open(rawdb.SnapshotStoragePrefix, storageStart, 2*common.HashLength)
	return r
}

func (r *rangeIterators) release() {
	r.acct.Release()
	r.stor.Release()
}

// refresh releases the iterators' read state after a flush so compaction can
// reclaim obsolete files. Each replacement starts at the next unconsumed key,
// including a storage record held for the next account.
func (r *rangeIterators) refresh(flushed bool, err error) error {
	if err != nil || !flushed {
		return err
	}
	r.acct, err = r.reopen(r.acct, rawdb.SnapshotAccountPrefix, common.HashLength)
	if err != nil {
		return err
	}
	r.stor, err = r.reopen(r.stor, rawdb.SnapshotStoragePrefix, 2*common.HashLength)
	return err
}

// open creates a length-filtered iterator starting at an inclusive key relative
// to the prefix, in the direction chosen for this generation.
func (r *rangeIterators) open(prefix, start []byte, suffixLen int) *internal.HoldableIterator {
	it := r.newIterator(prefix, start)
	return internal.NewHoldableIterator(rawdb.NewKeyLengthIterator(it, len(prefix)+suffixLen))
}

func (r *rangeIterators) reopen(old *internal.HoldableIterator, prefix []byte, suffixLen int) (*internal.HoldableIterator, error) {
	if !old.Next() {
		if err := old.Error(); err != nil {
			return old, err
		}
		old.Release()
		return internal.NewHoldableIterator(memorydb.New().NewIterator(nil, nil)), nil
	}
	// The backend can invalidate Key on Release.
	next := common.CopyBytes(old.Key()[len(prefix):])
	old.Release()
	return r.open(prefix, next, suffixLen), nil
}

// generatePartition walks accounts whose first nibble equals `partition`,
// reconciling each account's Root with its flat storage and building
// both per-account storage subtries and the partition's slice of the
// account trie. Returns the partition's stripped subtree root blob, or
// nil if the partition had no accounts at all.
func generatePartition(ctx context.Context, cancel <-chan struct{}, db ethdb.Database, scheme string, partition byte, rangeStart, rangeEnd common.Hash, c *genCounters, ingester ethdb.Ingester) ([]byte, error) {
	iters := openRangeIterators(db, rangeStart, rangeEnd, ingester)
	defer iters.release()

	batches := newPartitionBatches(db, ingester)
	defer batches.close()

	newStorageTrie := trie.NewStackTrie
	newAccountTrie := trie.NewPartialStackTrie
	if ingester != nil {
		newStorageTrie = trie.NewReverseStackTrie
		newAccountTrie = trie.NewReversePartialStackTrie
	}
	// Account-trie builder for this partition. It is fed account keys with
	// their leading nibble stripped and emits nodes at their absolute path
	// (prefixed with the partition nibble), so they line up with the full
	// trie without any post-hoc surgery.
	//
	// The subtree root is the only node emitted at path [partition]; we both
	// persist it (so the top-level branch can reference it) and capture its
	// bytes for assembleRoot, which needs them to either reference it or,
	// in the single-partition case, fold the leading nibble back in.
	var root []byte
	acctTrie := newAccountTrie(partition, func(path []byte, hash common.Hash, blob []byte) {
		if len(path) == 1 {
			root = common.CopyBytes(blob)
		}
		c.accountTrieNodes.Add(1)

		if scheme == rawdb.PathScheme {
			c.accountTrieBytes.Add(int64(len(path) + len(blob)))
		} else {
			c.accountTrieBytes.Add(int64(common.HashLength + len(blob)))
		}
		rawdb.WriteTrieNode(batches.accounts, common.Hash{}, path, hash, blob, scheme)
	})

	// Iterate through all the accounts.
	for iters.acct.Next() {
		select {
		case <-cancel:
			return nil, ErrCancelled
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		key := iters.acct.Key()
		var accountHash common.Hash
		copy(accountHash[:], key[len(rawdb.SnapshotAccountPrefix):])
		if accountHash[0]>>4 != partition {
			break
		}
		c.accounts.Add(1)
		position := binary.BigEndian.Uint64(accountHash[:8]) % partitionRangeSize
		if ingester != nil {
			position = partitionRangeSize - 1 - position
		}
		c.progress[partition].Store(position)

		// Decode the account object
		account, err := types.FullAccount(iters.acct.Value())
		if err != nil {
			return nil, fmt.Errorf("decode account %x: %w", accountHash, err)
		}

		// Build the account's storage trie from the flat storage snapshot.
		// StackTrie's callback adds completed nodes to the current write window.
		storageTrie := newStorageTrie(func(path []byte, hash common.Hash, blob []byte) {
			c.storageTrieNodes.Add(1)

			if scheme == rawdb.PathScheme {
				c.storageTrieBytes.Add(int64(len(path) + common.HashLength + len(blob)))
				rawdb.WriteStorageTrieNode(batches.storage, accountHash, path, blob)
			} else {
				c.storageTrieBytes.Add(int64(common.HashLength + len(blob)))
				rawdb.WriteTrieNode(batches.storage, accountHash, path, hash, blob, scheme)
			}
		})

		// Compute the storage root by consuming matching slots from the
		// shared storage iterator. The inner loop terminates on Hold()
		// (slot belongs to a later account) or exhaustion.
		lastDanglingAccount := make([]byte, common.HashLength)
		for iters.stor.Next() {
			// Re-check cancel.
			select {
			case <-cancel:
				return nil, ErrCancelled
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			var (
				sk             = iters.stor.Key()
				storageAccount = sk[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength]
				cmp            = bytes.Compare(storageAccount, accountHash[:])
			)
			if ingester != nil {
				cmp = -cmp
			}
			// The account iterator already passed this slot's owner. Remove the
			// dangling entry from the flat state.
			if cmp < 0 {
				if !bytes.Equal(lastDanglingAccount, storageAccount) {
					copy(lastDanglingAccount, storageAccount)
					log.Error("Unexpected storage entries for dangling account", "expected", accountHash, "got", common.BytesToHash(storageAccount))
				}
				c.storageDeleted.Add(1)
				slotHash := sk[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]
				rawdb.DeleteStorageSnapshot(batches.flat, common.BytesToHash(storageAccount), common.BytesToHash(slotHash))
				if err := iters.refresh(batches.flush(false)); err != nil {
					return nil, fmt.Errorf("flush batch (dangling): %w", err)
				}
				continue
			}

			// The slot belongs to a later account. We're done with the current
			// account's slots, but we don't want to lose this slot. The slot might
			// belong to the next iteration of the account for-loop (or a later one).
			// Hold() the iterator so the next Next() call will re-serve this same
			// entry instead of advancing past it.
			if cmp > 0 {
				iters.stor.Hold()
				break
			}

			// The slot belongs to this account so we add it to the StackTrie.
			slotHash := sk[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]
			if err := storageTrie.Update(slotHash, iters.stor.Value()); err != nil {
				return nil, fmt.Errorf("storage stack trie update for %x: %w", accountHash, err)
			}
			c.slots.Add(1)
			if err := iters.refresh(batches.flush(false)); err != nil {
				return nil, fmt.Errorf("flush batch (storage): %w", err)
			}
		}
		if err := iters.stor.Error(); err != nil {
			return nil, fmt.Errorf("storage iterator: %w", err)
		}
		computed := storageTrie.Hash()

		// If account.Root was stale, rewrite the flat-state entry. Then feed
		// the account, now with the correct Root, into this partition's
		// account trie.
		if computed != account.Root {
			account.Root = computed
			rawdb.WriteAccountSnapshot(batches.flat, accountHash, types.SlimAccountRLP(*account))
			c.accountUpdated.Add(1)
		}
		fullAccount, err := rlp.EncodeToBytes(account)
		if err != nil {
			return nil, fmt.Errorf("encode account %x: %w", accountHash, err)
		}
		if err := acctTrie.Update(accountHash[:], fullAccount); err != nil {
			return nil, fmt.Errorf("account stack trie update for %x: %w", accountHash, err)
		}
		if err := iters.refresh(batches.flush(false)); err != nil {
			return nil, fmt.Errorf("flush batch (account): %w", err)
		}
	}
	if err := iters.acct.Error(); err != nil {
		return nil, fmt.Errorf("account iterator: %w", err)
	}

	// The account iterator is exhausted (or has advanced past this partition),
	// but the storage iterator may still hold slots whose account hash falls
	// within this partition's range. Those slots belong to no existing account
	// and should be cleared.
	lastDanglingTail := make([]byte, common.HashLength)
	for iters.stor.Next() {
		select {
		case <-cancel:
			return nil, ErrCancelled
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		sk := iters.stor.Key()
		acct := sk[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength]
		if acct[0]>>4 != partition {
			break
		}
		if !bytes.Equal(lastDanglingTail, acct) {
			copy(lastDanglingTail, acct)
			log.Error("Unexpected storage entries for dangling account", "addrhash", common.BytesToHash(acct))
		}
		c.storageDeleted.Add(1)
		slotHash := sk[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]
		rawdb.DeleteStorageSnapshot(batches.flat, common.BytesToHash(acct), common.BytesToHash(slotHash))
		if err := iters.refresh(batches.flush(false)); err != nil {
			return nil, fmt.Errorf("flush batch (dangling tail): %w", err)
		}
	}
	if err := iters.stor.Error(); err != nil {
		return nil, fmt.Errorf("storage iterator (dangling): %w", err)
	}

	// Finalize the partition's account trie. For a non-empty partition this
	// emits the subtree root at path [partition], populating rootBlob. An empty
	// partition never emits any node and leaves rootBlob at nil.
	acctTrie.Hash()

	if _, err := batches.flush(true); err != nil {
		return nil, fmt.Errorf("final partition batch write: %w", err)
	}
	return root, nil
}

// hashRanges returns hash pairs [start, end] that evenly partition the
// 256-bit hash space. The last partition absorbs the remainder so rounding
// doesn't leave hashes uncovered.
func hashRanges(total int) [][2]common.Hash {
	step := new(big.Int).Sub(
		new(big.Int).Div(
			new(big.Int).Exp(common.Big2, common.Big256, nil),
			big.NewInt(int64(total)),
		),
		common.Big1,
	)
	ranges := make([][2]common.Hash, total)
	var next common.Hash
	for i := range total {
		last := common.BigToHash(new(big.Int).Add(next.Big(), step))
		if i == total-1 {
			last = common.MaxHash
		}
		ranges[i] = [2]common.Hash{next, last}
		next = common.BigToHash(new(big.Int).Add(last.Big(), common.Big1))
	}
	return ranges
}

// GenerateTrie builds all tries (storage + account) from flat snapshot
// data in the database. The account hash space is partitioned into 16
// slices aligned with the first-nibble branching of the MPT root. Each
// partition is processed by its own goroutine, which walks its slice,
// reconciles stale account.Root fields with flat storage, builds the
// per-account storage tries and the partition's slice of the account
// trie. Once every partition has produced its subtree root, the top-level
// branch is assembled and its hash verified against the expected root.
//
// Nodes become visible as their windows are written. An interrupted run
// leaves no resume state and the next run rebuilds every partition.
func GenerateTrie(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}) (GenerateStats, error) {
	return GenerateTrieWithProgress(db, scheme, root, cancel, nil)
}

// GenerateTrieWithProgress is GenerateTrie with live progress reporting.
// Path-scheme stores with reverse iteration and SST ingestion use them to
// generate and install disjoint ranges of trie nodes.
func GenerateTrieWithProgress(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64) (GenerateStats, error) {
	var ingester ethdb.Ingester
	if scheme == rawdb.PathScheme {
		ingester = rawdb.Ingester(db)
	}
	return generateTrie(db, scheme, root, cancel, prog, ingester)
}

// GenerateTrieWithBatch forces ordinary database batches for comparison with
// the default writer in the offline generation benchmark.
func GenerateTrieWithBatch(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}) (GenerateStats, error) {
	return generateTrie(db, scheme, root, cancel, nil, nil)
}

func generateTrie(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64, ingester ethdb.Ingester) (GenerateStats, error) {
	var (
		start        = time.Now()
		c            genCounters
		progressDone = make(chan struct{})

		// partitionBlobs[i] holds the root node for partition i, or nil if
		// the partition is empty.
		partitionBlobs [numPartitions][]byte
	)
	go tickProgress(progressDone, start, &c, prog)
	defer close(progressDone)

	if ingester != nil {
		// Persist the flat input, generation journal and trie range deletions
		// before any worker installs nodes. A restart can then safely rebuild
		// from that input after a partially ingested generation.
		if err := db.SyncKeyValue(); err != nil {
			return GenerateStats{}, fmt.Errorf("sync before trie ingestion: %w", err)
		}
	}
	// Run every partition concurrently, each producing the subtree root
	// blob that assembleRoot needs.
	var (
		ranges  = hashRanges(numPartitions)
		eg, ctx = errgroup.WithContext(context.Background())
	)
	for i, r := range ranges {
		partition := byte(i)
		rangeStart, rangeEnd := r[0], r[1]
		eg.Go(func() error {
			start := time.Now()
			blob, err := generatePartition(ctx, cancel, db, scheme, partition, rangeStart, rangeEnd, &c, ingester)
			if err != nil {
				return err
			}
			log.Info("Partition done", "partition", partition, "elapsed", common.PrettyDuration(time.Since(start)))

			c.progress[partition].Store(partitionRangeSize)
			partitionBlobs[partition] = blob
			return nil
		})
	}

	// Wait until all the partitions are fully generated
	if err := eg.Wait(); err != nil {
		return GenerateStats{}, err
	}
	if prog != nil {
		prog.Store(100)
	}
	// Assemble the top-level root from the partition blobs and verify it
	// matches the expected root.
	got, err := assembleRoot(db, scheme, partitionBlobs)
	if err != nil {
		return GenerateStats{}, fmt.Errorf("assemble root: %w", err)
	}
	if got != root {
		return GenerateStats{}, fmt.Errorf("state root mismatch: got %x, want %x", got, root)
	}
	if ingester != nil {
		// Flat repairs, the top-level root and any single-partition root cleanup
		// use ordinary writes. Make these durable before journaling completion.
		if err := db.SyncKeyValue(); err != nil {
			return GenerateStats{}, err
		}
	}
	log.Info("Generated state trie",
		"ingest", ingester != nil,
		"accounts", c.accounts.Load(), "slots", c.slots.Load(),
		"account-nodes", c.accountTrieNodes.Load(), "storage-nodes", c.storageTrieNodes.Load(),
		"account-nodebytes", common.StorageSize(c.accountTrieBytes.Load()), "storage-nodebytes", common.StorageSize(c.storageTrieBytes.Load()),
		"updated-accounts", c.accountUpdated.Load(), "dangling-slots", c.storageDeleted.Load(),
		"elapsed", common.PrettyDuration(time.Since(start)))

	return GenerateStats{
		Scanned: c.accounts.Load(),
		Updated: c.accountUpdated.Load(),
		Deleted: c.storageDeleted.Load(),
	}, nil
}

// assembleRoot computes the canonical state root from the 16 partition subtree
// root blobs and persists the top-level node. Each partition was built with its
// leading nibble stripped, so its root blob is already the exact node the parent
// branch mounts in that slot, and the partition has already written it (and all
// its descendants) at their absolute paths. What's left depends on how many
// partitions ended up populated:
//
//   - 0 populated: the state is empty, the root is types.EmptyRootHash and
//     nothing is written.
//
//   - 1 populated: there is no top-level branch; the canonical root is that
//     lone partition's subtree with its leading nibble folded back in (see
//     trie.MountPartitionRoot). The new root node is written. If the fold
//     orphaned the old subtree root the partition left at [n], that node is
//     also deleted.
//
//   - 2+ populated: the canonical root is a 17-slot branch mounting each
//     partition's subtree root by hash. The subtree roots are already on disk,
//     so we only encode, hash, and persist the branch itself.
func assembleRoot(db ethdb.Database, scheme string, partitionBlobs [numPartitions][]byte) (common.Hash, error) {
	var (
		populated int
		partition int // last populated index, read only when populated == 1
		children  [17][]byte
	)

	// Loop through all partitions and count how many are populated, while
	// pre-filling the branch children array for the common 2+ case.
	for i := range numPartitions {
		if partitionBlobs[i] != nil {
			populated++
			partition = i
			children[i] = crypto.Keccak256(partitionBlobs[i])
		}
	}

	// No populated partitions: the state is empty.
	if populated == 0 {
		return types.EmptyRootHash, nil
	}

	// One populated partition: no top-level branch, so fold its leading nibble
	// back into the subtree root.
	if populated == 1 {
		rootHash, rootBlob, isOrphaned, err := trie.MountPartitionRoot(partitionBlobs[partition], byte(partition))
		if err != nil {
			return common.Hash{}, fmt.Errorf("mount partition %d: %w", partition, err)
		}
		batch := db.NewBatch()
		rawdb.WriteTrieNode(batch, common.Hash{}, nil, rootHash, rootBlob, scheme)
		if isOrphaned {
			// The folded root at nil does not reference [partition], so the copy
			// generatePartition wrote there is now unreferenced. Delete it so the
			// on-disk node set matches the canonical trie.
			staleHash := crypto.Keccak256Hash(partitionBlobs[partition])
			rawdb.DeleteTrieNode(batch, common.Hash{}, []byte{byte(partition)}, staleHash, scheme)
		}
		return rootHash, batch.Write()
	}

	// populated >= 2: mount each partition's subtree root (already persisted at
	// path [i]) into a 17-slot branch by hash, using the children array filled
	// above. Those hash references are valid because account-trie subtree roots
	// are always >= 32 bytes.
	rootBlob, rootHash, err := trie.AssembleBranch(children)
	if err != nil {
		return common.Hash{}, err
	}
	rawdb.WriteTrieNode(db, common.Hash{}, nil, rootHash, rootBlob, scheme)
	return rootHash, nil
}

// tickProgress logs an aggregate progress line every 30 seconds until done
// is closed. Cheap: a handful of atomic loads and one log line per tick.
func tickProgress(done <-chan struct{}, start time.Time, c *genCounters, prog *atomic.Uint64) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			elapsed := time.Since(start)
			fraction := progressFraction(&c.progress)

			// Notify the external subscriber about the generation progress
			if prog != nil {
				prog.Store(uint64(100 * fraction))
			}
			eta := "n/a"
			if fraction > 0.005 {
				eta = common.PrettyDuration(time.Duration(float64(elapsed) * (1.0/fraction - 1.0))).String()
			}
			log.Info("Generating trie",
				"progress", fmt.Sprintf("%.1f%%", fraction*100), "eta", eta,
				"accounts", c.accounts.Load(), "slots", c.slots.Load(),
				"account-updated", c.accountUpdated.Load(), "dangling-slots", c.storageDeleted.Load(),
				"elapsed", common.PrettyDuration(elapsed),
				"acct/s", uint64(float64(c.accounts.Load())/elapsed.Seconds()))
		}
	}
}

// progressFraction averages each partition's iterator position (as a fraction
// of its hash range) into an overall completion estimate in [0, 1]. Keccak
// hashes are uniform, so keyspace position is a good proxy for work done.
func progressFraction(progress *[numPartitions]atomic.Uint64) float64 {
	var total float64
	for i := range progress {
		total += float64(progress[i].Load()) / float64(partitionRangeSize)
	}
	return total / float64(numPartitions)
}

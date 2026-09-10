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
// into by GenerateTrie: one per two leading nibbles, so a partition is the
// subtrie under a depth-2 path and its root mounts into the depth-1 branch
// above it. There are more partitions than workers; a pool of workers takes
// them in order, so at any moment the concurrent writers are ascending through
// consecutive slices of the hash space.
//
// Why not one partition per worker: a worker's stream of trie nodes lands as
// narrow sstables that overlap nothing older only if it stays within its own
// slice, and a flush cuts files by size, not by slice. With many workers each
// slice's share of a flush shrinks below the file size, files straddle slices
// and pebble has to rewrite them into the base level instead of relinking
// them. Fixing the partition count at 256 and choosing the worker count
// separately keeps the two concerns apart.
const numPartitions = 256

// DefaultGenerateWorkers is how many partitions GenerateTrie builds at once.
// The trie work is CPU-bound per worker; the writes it produces are bounded by
// the disk's compaction bandwidth, and past that point extra workers only wait
// on the L0 write stall. Also keep workers <= memtable size / L0 file size, or
// a slice's share of one flush falls below a file and relinking breaks.
const DefaultGenerateWorkers = 16

// GeneratePartitions returns the number of partitions GenerateTrie divides the
// account hash space into.
func GeneratePartitions() int { return numPartitions }

// Each partition covers 1/256 of the account hash space. We track progress
// by interpreting the top 8 bytes of an account hash as a uint64, so each
// partition spans 2^64 / 256 = 2^56. partitionFinished is stored in a
// partition's position when it completes.
const (
	partitionRangeSize = uint64(1) << 56
	partitionFinished  = ^uint64(0)
)

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

	progress [numPartitions]atomic.Uint64 // per-partition keyspace position
}

// rangeIterators bundles the per-partition account and storage iterators.
type rangeIterators struct {
	db   ethdb.Database
	acct *internal.HoldableIterator
	stor *internal.HoldableIterator
}

func openRangeIterators(db ethdb.Database, start common.Hash) *rangeIterators {
	return &rangeIterators{
		db:   db,
		acct: openFlatIterator(db, rawdb.SnapshotAccountPrefix, start[:], common.HashLength),
		stor: openFlatIterator(db, rawdb.SnapshotStoragePrefix, start[:], 2*common.HashLength),
	}
}

// reopen releases both iterators and reopens them at their current
// positions. Invoked after each batch flush so pebble compactions aren't
// blocked by long-lived iterator snapshots. Follows the same pattern as
// triedb/pathdb/context.go.
func (r *rangeIterators) reopen() {
	r.acct = reopenFlatIterator(r.db, r.acct, rawdb.SnapshotAccountPrefix, common.HashLength)
	r.stor = reopenFlatIterator(r.db, r.stor, rawdb.SnapshotStoragePrefix, 2*common.HashLength)
}

func (r *rangeIterators) release() {
	r.acct.Release()
	r.stor.Release()
}

// flushIfFull writes and resets the batch once it grows past IdealBatchSize,
// then reopens the iterators.
func (r *rangeIterators) flushIfFull(batch ethdb.Batch, where string) error {
	if batch.ValueSize() <= ethdb.IdealBatchSize {
		return nil
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("flush batch (%s): %w", where, err)
	}
	batch.Reset()
	r.reopen()
	return nil
}

// openFlatIterator opens a length-filtered HoldableIterator over a snapshot
// prefix, seeked to the given start key (relative to the prefix).
func openFlatIterator(db ethdb.Database, prefix, start []byte, suffixLen int) *internal.HoldableIterator {
	it := db.NewIterator(prefix, start)
	return internal.NewHoldableIterator(rawdb.NewKeyLengthIterator(it, len(prefix)+suffixLen))
}

// reopenFlatIterator releases `old` and returns a new HoldableIterator
// positioned at the same key, or an empty iterator if `old` is exhausted.
func reopenFlatIterator(db ethdb.Database, old *internal.HoldableIterator, prefix []byte, suffixLen int) *internal.HoldableIterator {
	if !old.Next() {
		old.Release()
		return internal.NewHoldableIterator(memorydb.New().NewIterator(nil, nil))
	}
	// pebble's Key() slice is invalidated by Release. Copy first so the new
	// iterator's lower bound isn't seeded from freed memory.
	next := common.CopyBytes(old.Key())
	old.Release()
	return openFlatIterator(db, prefix, next[len(prefix):], suffixLen)
}

// generatePartition walks accounts whose first nibble equals `partition`,
// reconciling each account's Root with its flat storage and building
// both per-account storage subtries and the partition's slice of the
// account trie. Returns the partition's stripped subtree root blob, or
// nil if the partition had no accounts at all.
func generatePartition(ctx context.Context, cancel <-chan struct{}, db ethdb.Database, scheme string, partition byte, rangeStart, rangeEnd common.Hash, c *genCounters) ([]byte, error) {
	iters := openRangeIterators(db, rangeStart)
	defer iters.release()

	batch := db.NewBatchWithSize(ethdb.IdealBatchSize)

	// Account-trie builder for this partition. It is fed account keys with
	// their two leading nibbles stripped and emits nodes at their absolute
	// path (prefixed with those nibbles), so they line up with the full trie
	// without any post-hoc surgery.
	//
	// The subtree root is the only node emitted at the partition's own path;
	// we both persist it (so the branch above can reference it) and capture
	// its bytes for assembleRoot, which needs them to either reference it or,
	// when it is the only populated partition under its parent, fold the
	// nibble back in.
	var (
		root   []byte
		prefix = []byte{partition >> 4, partition & 0x0f}
	)
	acctTrie := trie.NewPartialStackTrieAt(prefix, func(path []byte, hash common.Hash, blob []byte) {
		if len(path) == len(prefix) {
			root = common.CopyBytes(blob)
		}
		c.accountTrieNodes.Add(1)

		if scheme == rawdb.PathScheme {
			c.accountTrieBytes.Add(int64(len(path) + len(blob)))
		} else {
			c.accountTrieBytes.Add(int64(common.HashLength + len(blob)))
		}
		rawdb.WriteTrieNode(batch, common.Hash{}, path, hash, blob, scheme)
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
		if bytes.Compare(accountHash[:], rangeEnd[:]) > 0 {
			break
		}
		c.accounts.Add(1)
		c.progress[partition].Store(binary.BigEndian.Uint64(accountHash[:8]))

		// Decode the account object
		account, err := types.FullAccount(iters.acct.Value())
		if err != nil {
			return nil, fmt.Errorf("decode account %x: %w", accountHash, err)
		}

		// Build the account's storage trie from the flat storage snapshot.
		// StackTrie's onTrieNode callback persists nodes as they finalize.
		storageTrie := trie.NewStackTrie(func(path []byte, hash common.Hash, blob []byte) {
			c.storageTrieNodes.Add(1)

			if scheme == rawdb.PathScheme {
				c.storageTrieBytes.Add(int64(len(path) + common.HashLength + len(blob)))
			} else {
				c.storageTrieBytes.Add(int64(common.HashLength + len(blob)))
			}
			rawdb.WriteTrieNode(batch, accountHash, path, hash, blob, scheme)
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
			// The slot belongs to an account whose hash is smaller than the one
			// currently being processed. This should be theoretically impossible,
			// so log it loudly and delete the dangling entry from the flat state.
			if cmp < 0 {
				if !bytes.Equal(lastDanglingAccount, storageAccount) {
					copy(lastDanglingAccount, storageAccount)
					log.Error("Unexpected storage entries for dangling account", "expected", accountHash, "got", common.BytesToHash(storageAccount))
				}
				c.storageDeleted.Add(1)
				slotHash := sk[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]
				rawdb.DeleteStorageSnapshot(batch, common.BytesToHash(storageAccount), common.BytesToHash(slotHash))
				if err := iters.flushIfFull(batch, "dangling"); err != nil {
					return nil, err
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
			if err := iters.flushIfFull(batch, "storage"); err != nil {
				return nil, err
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
			rawdb.WriteAccountSnapshot(batch, accountHash, types.SlimAccountRLP(*account))
			c.accountUpdated.Add(1)
		}
		fullAccount, err := rlp.EncodeToBytes(account)
		if err != nil {
			return nil, fmt.Errorf("encode account %x: %w", accountHash, err)
		}
		if err := acctTrie.Update(accountHash[:], fullAccount); err != nil {
			return nil, fmt.Errorf("account stack trie update for %x: %w", accountHash, err)
		}
		if err := iters.flushIfFull(batch, "account"); err != nil {
			return nil, err
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
		if bytes.Compare(acct, rangeEnd[:]) > 0 {
			break
		}
		if !bytes.Equal(lastDanglingTail, acct) {
			copy(lastDanglingTail, acct)
			log.Error("Unexpected storage entries for dangling account", "addrhash", common.BytesToHash(acct))
		}
		c.storageDeleted.Add(1)
		slotHash := sk[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]
		rawdb.DeleteStorageSnapshot(batch, common.BytesToHash(acct), common.BytesToHash(slotHash))
		if err := iters.flushIfFull(batch, "dangling tail"); err != nil {
			return nil, err
		}
	}
	if err := iters.stor.Error(); err != nil {
		return nil, fmt.Errorf("storage iterator (dangling): %w", err)
	}

	// Finalize the partition's account trie. For a non-empty partition this
	// emits the subtree root at the partition's path, populating root. An empty
	// partition never emits any node and leaves root at nil.
	acctTrie.Hash()

	if err := batch.Write(); err != nil {
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
// data in the database. The account hash space is partitioned into 256
// slices aligned with the two-nibble branching below the MPT root, built
// by a pool of workers. Each worker walks its slice,
// reconciles stale account.Root fields with flat storage, builds the
// per-account storage tries and the partition's slice of the account
// trie. Once every partition has produced its subtree root, the top-level
// branch is assembled and its hash verified against the expected root.
//
// Generation is all or nothing: an interrupted run leaves no resume
// state and the next run builds every partition from scratch.
func GenerateTrie(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}) (GenerateStats, error) {
	return generateTrie(db, scheme, root, cancel, nil, DefaultGenerateWorkers)
}

// GenerateTrieWithWorkers is GenerateTrieWithProgress with the number of
// partitions built concurrently chosen by the caller.
func GenerateTrieWithWorkers(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64, workers int) (GenerateStats, error) {
	return generateTrie(db, scheme, root, cancel, prog, workers)
}

// GenerateTrieWithProgress is GenerateTrie with live progress reporting.
func GenerateTrieWithProgress(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64) (GenerateStats, error) {
	return generateTrie(db, scheme, root, cancel, prog, DefaultGenerateWorkers)
}

func generateTrie(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64, workers int) (GenerateStats, error) {
	if workers < 1 {
		workers = 1
	}
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

	// Run the partitions through a pool of workers, in order, each producing
	// the subtree root blob that assembleRoot needs.
	var (
		ranges  = hashRanges(numPartitions)
		eg, ctx = errgroup.WithContext(context.Background())
	)
	eg.SetLimit(workers)
	for i, r := range ranges {
		partition := byte(i)
		rangeStart, rangeEnd := r[0], r[1]
		eg.Go(func() error {
			start := time.Now()
			blob, err := generatePartition(ctx, cancel, db, scheme, partition, rangeStart, rangeEnd, &c)
			if err != nil {
				return err
			}
			log.Debug("Partition done", "partition", partition, "elapsed", common.PrettyDuration(time.Since(start)))

			c.progress[partition].Store(partitionFinished)
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
	log.Info("Generated state trie",
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

// assembleRoot computes the canonical state root from the partition subtree
// root blobs and persists every node above them. Partitions are the subtries
// under depth-2 paths, so assembly is two levels of the same step: each depth-1
// node is put together from its 16 partitions, then the root from the 16
// depth-1 nodes. See assembleNode for what one step entails.
func assembleRoot(db ethdb.Database, scheme string, partitionBlobs [numPartitions][]byte) (common.Hash, error) {
	batch := db.NewBatch()
	var level1 [16][]byte
	for n := range 16 {
		var children [16][]byte
		copy(children[:], partitionBlobs[n*16:(n+1)*16])
		blob, err := assembleNode(batch, scheme, []byte{byte(n)}, children)
		if err != nil {
			return common.Hash{}, err
		}
		level1[n] = blob
	}
	rootBlob, err := assembleNode(batch, scheme, nil, level1)
	if err != nil {
		return common.Hash{}, err
	}
	if rootBlob == nil {
		// Nothing anywhere: the state is empty and nothing is written.
		return types.EmptyRootHash, nil
	}
	return crypto.Keccak256Hash(rootBlob), batch.Write()
}

// assembleNode builds the node at path `at` from the subtree root blobs of its
// 16 children, found at paths at+[i], writes what it creates, and returns the
// node's blob, or nil if no child is populated. Each child was built with its
// nibble stripped, so its blob is already the exact node the parent mounts in
// that slot, and the child has already written it (and all its descendants) at
// their absolute paths. What is left depends on how many are populated:
//
//   - 0: the subtree is empty; nothing is written and nil is returned.
//
//   - 1: there is no branch at `at`; the node there is that lone child's
//     subtree with its nibble folded back in (see trie.MountPartitionRoot).
//     If the fold orphaned the node the child left at at+[i], it is deleted.
//
//   - 2+: the node at `at` is a 17-slot branch mounting each child by hash.
//     The children are already on disk, so only the branch is written. The
//     hash references are valid because account-trie subtree roots are always
//     at least 32 bytes.
func assembleNode(batch ethdb.Batch, scheme string, at []byte, children [16][]byte) ([]byte, error) {
	var (
		populated int
		last      int // last populated index, read only when populated == 1
		refs      [17][]byte
	)
	for i := range children {
		if children[i] != nil {
			populated++
			last = i
			refs[i] = crypto.Keccak256(children[i])
		}
	}
	switch populated {
	case 0:
		return nil, nil
	case 1:
		hash, blob, isOrphaned, err := trie.MountPartitionRoot(children[last], byte(last))
		if err != nil {
			return nil, fmt.Errorf("mount subtree %x%x: %w", at, last, err)
		}
		rawdb.WriteTrieNode(batch, common.Hash{}, at, hash, blob, scheme)
		if isOrphaned {
			// The folded node at `at` does not reference at+[last], so the copy
			// the child wrote there is now unreferenced. Delete it so the
			// on-disk node set matches the canonical trie.
			stale := crypto.Keccak256Hash(children[last])
			rawdb.DeleteTrieNode(batch, common.Hash{}, append(bytes.Clone(at), byte(last)), stale, scheme)
		}
		return blob, nil
	default:
		blob, hash, err := trie.AssembleBranch(refs)
		if err != nil {
			return nil, err
		}
		rawdb.WriteTrieNode(batch, common.Hash{}, at, hash, blob, scheme)
		return blob, nil
	}
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
	for i := range numPartitions {
		p := progress[i].Load()
		switch {
		case p == partitionFinished:
			total += 1.0
		case p == 0:
			// not started yet
		default:
			rangeStart := uint64(i) * partitionRangeSize
			if p > rangeStart {
				rel := p - rangeStart
				if rel > partitionRangeSize {
					rel = partitionRangeSize
				}
				total += float64(rel) / float64(partitionRangeSize)
			}
		}
	}
	return total / float64(numPartitions)
}

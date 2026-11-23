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
	"runtime"
	"sync"
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
	Timings *GenerateTimings // populated by GenerateTrieTo
}

// Each partition covers one leading byte. Within it, the next seven bytes
// give the position used by the progress meter.
const numPartitions = 256
const partitionRangeSize = uint64(1) << 56

// Each worker drains a contiguous range in descending order. An idle worker
// takes the low half of the largest remaining range, leaving the donor's next
// partition adjacent to the one it is processing.
type partitionScheduler struct {
	mu     sync.Mutex
	ranges []struct{ lo, hi int } // unclaimed partitions in [lo, hi)
}

func newPartitionScheduler(workers int) *partitionScheduler {
	s := &partitionScheduler{ranges: make([]struct{ lo, hi int }, workers)}
	for i := range s.ranges {
		s.ranges[i].lo = i * numPartitions / workers
		s.ranges[i].hi = (i + 1) * numPartitions / workers
	}
	return s
}

func (s *partitionScheduler) next(worker int) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &s.ranges[worker]
	if r.lo == r.hi {
		donor := r
		for i := range s.ranges {
			other := &s.ranges[i]
			if other.hi-other.lo > donor.hi-donor.lo {
				donor = other
			}
		}
		if donor == r {
			return 0, false
		}
		r.lo = donor.lo
		r.hi = donor.lo + (donor.hi-donor.lo+1)/2
		donor.lo = r.hi
	}
	r.hi--
	return r.hi, true
}

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
	timers   *generationTimers            // separate-database benchmark only
}

// rangeIterators bundles the per-partition account and storage iterators.
type rangeIterators struct {
	newIterator    func(prefix, start []byte) ethdb.Iterator
	refreshOnFlush bool
	acct           *internal.HoldableIterator
	stor           *internal.HoldableIterator
}

// trieGenerator reads ordered flat records using newIterator and writes nodes
// to dst. In-place generation also repairs the flat records in that database.
type trieGenerator struct {
	dst         ethdb.Database
	newIterator func(prefix, start []byte) ethdb.Iterator
	ingester    ethdb.Ingester
	inPlace     bool
}

func (g *trieGenerator) openRangeIterators(partition byte, timer *partitionTimer) *rangeIterators {
	r := &rangeIterators{
		newIterator:    g.newIterator,
		refreshOnFlush: g.inPlace,
	}
	if timer != nil {
		r.newIterator = func(prefix, start []byte) ethdb.Iterator {
			t := time.Now()
			it := g.newIterator(prefix, start)
			timer.flat += time.Since(t)
			return &timedFlatIterator{Iterator: it, timer: timer}
		}
	}
	start := common.Hash{partition}
	accountStart, storageStart := start[:], start[:]

	if g.ingester != nil {
		end := common.MaxHash
		end[0] = partition
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

// During in-place generation, refresh releases the iterators' read state after
// a flush so compaction can reclaim obsolete files. Each replacement starts
// at the next unconsumed key, including a slot held for the next account.
func (r *rangeIterators) refresh(flushed bool, err error) error {
	if err != nil || !flushed || !r.refreshOnFlush {
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

// generatePartition walks accounts whose first byte equals partition,
// reconciling each account's Root with its flat storage and building
// both per-account storage subtries and the partition's slice of the
// account trie. Returns the partition's stripped subtree root blob, or
// nil if the partition had no accounts at all.
func (g *trieGenerator) generatePartition(ctx context.Context, cancel <-chan struct{}, scheme string, partition byte, c *genCounters, batches *partitionBatches, timer *partitionTimer) ([]byte, error) {
	start := time.Now()
	iters := g.openRangeIterators(partition, timer)
	defer iters.release()

	// Keep the subtree root until assembly determines whether to reference it
	// or fold it into a short node. All deeper nodes have canonical paths.
	var root []byte
	acctTrie := trie.NewBytePartitionStackTrie(partition, g.ingester != nil, func(path []byte, hash common.Hash, blob []byte) {
		if len(path) == 2 {
			root = common.CopyBytes(blob)
			return
		}
		c.writeAccountNode(batches.accounts, scheme, path, hash, blob)
	})

	// Iterate through all the accounts.
	var accounts, slots, maxAccountSlots int64
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
		if accountHash[0] != partition {
			break
		}
		accounts++
		c.accounts.Add(1)
		position := binary.BigEndian.Uint64(accountHash[:8]) % partitionRangeSize
		if g.ingester != nil {
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
		storageTrie := acctTrie.NewStorageTrie(func(path []byte, hash common.Hash, blob []byte) {
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
		slotStart := slots
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
			if g.ingester != nil {
				cmp = -cmp
			}
			// The account iterator already passed this slot's owner. Remove the
			// dangling entry from the flat state.
			if cmp < 0 {
				if !g.inPlace {
					continue
				}
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
			slots++
			if err := iters.refresh(batches.flush(false)); err != nil {
				return nil, fmt.Errorf("flush batch (storage): %w", err)
			}
		}
		if err := iters.stor.Error(); err != nil {
			return nil, fmt.Errorf("storage iterator: %w", err)
		}
		computed := storageTrie.Hash()
		maxAccountSlots = max(maxAccountSlots, slots-slotStart)

		// Reconcile the root before encoding the account into the trie.
		// In-place generation also repairs the account's flat-state entry.
		if computed != account.Root {
			account.Root = computed
			if g.inPlace {
				rawdb.WriteAccountSnapshot(batches.flat, accountHash, types.SlimAccountRLP(*account))
				c.accountUpdated.Add(1)
			}
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
	for g.inPlace && iters.stor.Next() {
		select {
		case <-cancel:
			return nil, ErrCancelled
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		sk := iters.stor.Key()
		acct := sk[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength]
		if acct[0] != partition {
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

	// An empty partition never emits a root.
	acctTrie.Hash()

	log.Info("Partition done", "partition", partition, "accounts", accounts, "slots", slots,
		"largest-account-slots", maxAccountSlots, "elapsed", common.PrettyDuration(time.Since(start)))
	return root, nil
}

// GenerateTrie builds all tries (storage + account) from flat snapshot
// data in the database. Workers take byte-prefix partitions, reconcile
// account.Root with flat storage and build storage and account subtrees.
// The top two levels are assembled after all partitions finish.
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
	g := trieGenerator{dst: db, newIterator: db.NewIterator, inPlace: true}
	if scheme == rawdb.PathScheme {
		g.ingester = rawdb.Ingester(db)
		if g.ingester != nil {
			g.newIterator = g.ingester.NewReverseIterator
		}
	}
	return g.generate(scheme, root, cancel, prog)
}

// GenerateTrieWithBatch builds the trie in place using ordinary batches.
func GenerateTrieWithBatch(db ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}) (GenerateStats, error) {
	g := trieGenerator{dst: db, newIterator: db.NewIterator, inPlace: true}
	return g.generate(scheme, root, cancel, nil)
}

// GenerateTrieTo builds trie nodes from a stable flat-state source into a
// separate, empty destination and syncs it after verifying the root. Stale
// account roots are recomputed in memory and dangling slots are skipped.
// For path-based storage, a descending source iterator and an ingest-capable
// destination enable SST ingestion. Set batch to force ordinary batches.
// The returned timings break down the benchmark's cumulative worker time.
func GenerateTrieTo(src ethdb.Iteratee, dst ethdb.Database, scheme string, root common.Hash, cancel <-chan struct{}, batch bool) (GenerateStats, error) {
	g := trieGenerator{dst: dst, newIterator: src.NewIterator}
	if reverse, ok := src.(ethdb.ReverseIteratee); ok && !batch && scheme == rawdb.PathScheme {
		g.ingester = rawdb.Ingester(dst)
		if g.ingester != nil {
			g.newIterator = reverse.NewReverseIterator
		}
	}
	return g.generate(scheme, root, cancel, nil)
}

func (g *trieGenerator) generate(scheme string, root common.Hash, cancel <-chan struct{}, prog *atomic.Uint64) (GenerateStats, error) {
	var (
		start        = time.Now()
		c            genCounters
		progressDone = make(chan struct{})

		// partitionBlobs[i] holds the root node for partition i, or nil if
		// the partition is empty.
		partitionBlobs [numPartitions][]byte
	)
	if !g.inPlace {
		c.timers = new(generationTimers)
	}
	go tickProgress(progressDone, start, &c, prog)
	defer close(progressDone)

	if g.inPlace && g.ingester != nil {
		// Persist the flat input, generation journal and trie range deletions
		// before any worker installs nodes. A restart can then safely rebuild
		// from that input after a partially ingested generation.
		if err := g.dst.SyncKeyValue(); err != nil {
			return GenerateStats{}, fmt.Errorf("sync before trie ingestion: %w", err)
		}
	}
	var (
		workers = min(runtime.GOMAXPROCS(0), numPartitions)
		queue   = newPartitionScheduler(workers)
		eg, ctx = errgroup.WithContext(context.Background())
	)
	log.Info("Scheduling trie generation", "partitions", numPartitions, "workers", workers)
	for worker := range workers {
		eg.Go(func() error {
			var timer *partitionTimer
			if c.timers != nil {
				timer = &partitionTimer{all: c.timers, start: time.Now()}
				defer timer.publish()
			}
			batches := newPartitionBatches(g.dst, g.ingester, timer)
			defer batches.close()
			previous := -1
			for {
				select {
				case <-cancel:
					return ErrCancelled
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				i, ok := queue.next(worker)
				// A window may span adjacent descending prefixes, but never a
				// gap that could contain another worker's SSTs.
				if !ok || (previous >= 0 && i+1 != previous) {
					if _, err := batches.flush(true); err != nil {
						return fmt.Errorf("flush partition range: %w", err)
					}
				}
				if !ok {
					return nil
				}
				blob, err := g.generatePartition(ctx, cancel, scheme, byte(i), &c, batches, timer)
				if err != nil {
					return err
				}
				c.progress[i].Store(partitionRangeSize)
				partitionBlobs[i] = blob
				previous = i
			}
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
	finalizeStart := time.Now()
	got, err := assembleRoot(g.dst, scheme, &partitionBlobs, &c)
	if err != nil {
		return GenerateStats{}, fmt.Errorf("assemble root: %w", err)
	}
	if got != root {
		return GenerateStats{}, fmt.Errorf("state root mismatch: got %x, want %x", got, root)
	}
	if !g.inPlace || g.ingester != nil {
		// Include the ordinary writes from assembling the top two levels.
		if err := g.dst.SyncKeyValue(); err != nil {
			return GenerateStats{}, err
		}
	}
	var timings *GenerateTimings
	if c.timers != nil {
		finalize := time.Since(finalizeStart)
		total := c.timers.snapshot()
		total.Finalize = finalize
		timings = &total
	}
	log.Info("Generated state trie",
		"ingest", g.ingester != nil,
		"accounts", c.accounts.Load(), "slots", c.slots.Load(),
		"account-nodes", c.accountTrieNodes.Load(), "storage-nodes", c.storageTrieNodes.Load(),
		"account-nodebytes", common.StorageSize(c.accountTrieBytes.Load()), "storage-nodebytes", common.StorageSize(c.storageTrieBytes.Load()),
		"updated-accounts", c.accountUpdated.Load(), "dangling-slots", c.storageDeleted.Load(),
		"elapsed", common.PrettyDuration(time.Since(start)))

	return GenerateStats{
		Scanned: c.accounts.Load(),
		Updated: c.accountUpdated.Load(),
		Deleted: c.storageDeleted.Load(),
		Timings: timings,
	}, nil
}

// writeAccountNode tracks the canonical nodes written by both workers and assembly.
func (c *genCounters) writeAccountNode(dst ethdb.KeyValueWriter, scheme string, path []byte, hash common.Hash, blob []byte) {
	c.accountTrieNodes.Add(1)
	keySize := len(path)
	if scheme == rawdb.HashScheme {
		keySize = common.HashLength
	}
	c.accountTrieBytes.Add(int64(keySize + len(blob)))
	rawdb.WriteTrieNode(dst, common.Hash{}, path, hash, blob, scheme)
}

// assembleRoot joins the byte-prefix subtrees one nibble at a time. Defer
// writing each group root until its parent decides whether it survives path
// compression. Only canonical nodes enter the final batch.
func assembleRoot(db ethdb.Database, scheme string, partitions *[numPartitions][]byte, c *genCounters) (common.Hash, error) {
	batch := db.NewBatch()
	defer batch.Close()
	var groups [16][]byte
	for i := range groups {
		blob, err := joinAccountSubtrees(batch, scheme, []byte{byte(i)}, partitions[i*16:(i+1)*16], c)
		if err != nil {
			return common.Hash{}, err
		}
		groups[i] = blob
	}
	root, err := joinAccountSubtrees(batch, scheme, nil, groups[:], c)
	if err != nil {
		return common.Hash{}, err
	}
	if root == nil {
		return types.EmptyRootHash, nil
	}
	hash := crypto.Keccak256Hash(root)
	c.writeAccountNode(batch, scheme, nil, hash, root)
	return hash, batch.Write()
}

// joinAccountSubtrees returns the root at path, writing only the children
// referenced by it. Account RLPs contain a storage root and code hash, so
// every non-empty account subtree is large enough to be referenced by hash.
func joinAccountSubtrees(dst ethdb.KeyValueWriter, scheme string, path []byte, blobs [][]byte, c *genCounters) ([]byte, error) {
	var count, last int
	for i, blob := range blobs {
		if blob != nil {
			count++
			last = i
		}
	}
	if count == 0 {
		return nil, nil
	}
	if count == 1 {
		child := blobs[last]
		_, root, folded, err := trie.MountPartitionRoot(child, byte(last))
		if err != nil {
			return nil, err
		}
		if !folded {
			c.writeAccountNode(dst, scheme, append(path, byte(last)), crypto.Keccak256Hash(child), child)
		}
		return root, nil
	}
	var children [17][]byte
	for i, blob := range blobs {
		if blob != nil {
			hash := crypto.Keccak256Hash(blob)
			c.writeAccountNode(dst, scheme, append(path, byte(i)), hash, blob)
			children[i] = hash[:]
		}
	}
	root, _, err := trie.AssembleBranch(children)
	return root, err
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
			if c.timers != nil {
				total := c.timers.snapshot()
				log.Info("Cumulative trie worker time through completed batches", "flat-iteration", common.PrettyDuration(total.FlatIteration),
					"trie-build", common.PrettyDuration(total.TrieBuild), "node-write", common.PrettyDuration(total.NodeWrite),
					"of-which-commit", common.PrettyDuration(total.NodeCommit))
			}
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

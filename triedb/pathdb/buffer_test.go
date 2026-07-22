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

package pathdb

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// randomBufferSets constructs a random node set and state set, with the keys
// drawn from a bounded space to produce overlaps across different sets.
func randomBufferSets(r *rand.Rand, n int) (*nodeSet, *stateSet) {
	var (
		nodes    = make(map[common.Hash]map[string]*trienode.Node)
		accounts = make(map[common.Hash][]byte)
		storages = make(map[common.Hash]map[common.Hash][]byte)
	)
	nodes[common.Hash{}] = make(map[string]*trienode.Node)
	for i := 0; i < n; i++ {
		var (
			acct = bufTestKey("account", r.Intn(n))
			path = fmt.Sprintf("path-%d", r.Intn(n))
			blob = bufTestBlob(r, 32)
		)
		// Trie nodes, in both the account trie and storage tries
		nodes[common.Hash{}][path] = trienode.New(crypto.Keccak256Hash(blob), blob)

		if _, ok := nodes[acct]; !ok {
			nodes[acct] = make(map[string]*trienode.Node)
		}
		nodes[acct][path] = trienode.New(crypto.Keccak256Hash(blob), blob)

		// Flat states
		accounts[acct] = bufTestBlob(r, 16)
		if _, ok := storages[acct]; !ok {
			storages[acct] = make(map[common.Hash][]byte)
		}
		storages[acct][bufTestKey("slot", r.Intn(n))] = bufTestBlob(r, 16)
	}
	return newNodeSet(nodes), newStates(accounts, storages, false)
}

func bufTestKey(kind string, i int) common.Hash {
	return crypto.Keccak256Hash([]byte(fmt.Sprintf("%s-%d", kind, i)))
}

func bufTestBlob(r *rand.Rand, n int) []byte {
	buf := make([]byte, n)
	r.Read(buf)
	return buf
}

// waitCompaction blocks until the background compaction settles down with
// no more compactable runs left.
func waitCompaction(t *testing.T, b *buffer) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		b.compactMu.Lock()
		idle := !b.compacting
		b.compactMu.Unlock()

		if idle {
			if i, _ := compactRun(b.view.Load().sets); i < 0 {
				return
			}
			b.tryCompact()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("compaction did not settle down")
}

// verifyBuffer cross-checks the buffer content against the reference sets.
func verifyBuffer(t *testing.T, b *buffer, nodes map[common.Hash]map[string]*trienode.Node, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte) {
	t.Helper()
	for owner, subset := range nodes {
		for path, n := range subset {
			got, ok := b.node(owner, []byte(path))
			if !ok {
				t.Fatalf("node not found, %x %v", owner, path)
			}
			if !bytes.Equal(got.Blob, n.Blob) {
				t.Fatalf("node mismatch, %x %v", owner, path)
			}
		}
	}
	for hash, blob := range accounts {
		got, ok := b.account(hash)
		if !ok {
			t.Fatalf("account not found, %x", hash)
		}
		if !bytes.Equal(got, blob) {
			t.Fatalf("account mismatch, %x", hash)
		}
	}
	list := b.accountList()
	if len(list) != len(accounts) {
		t.Fatalf("account list length mismatch, want %d, got %d", len(accounts), len(list))
	}
	if !slices.IsSortedFunc(list, common.Hash.Cmp) {
		t.Fatal("account list is not sorted")
	}
	for addrHash, slots := range storages {
		for slotHash, blob := range slots {
			got, ok := b.storage(addrHash, slotHash)
			if !ok {
				t.Fatalf("storage not found, %x %x", addrHash, slotHash)
			}
			if !bytes.Equal(got, blob) {
				t.Fatalf("storage mismatch, %x %x", addrHash, slotHash)
			}
		}
		list := b.storageList(addrHash)
		if len(list) != len(slots) {
			t.Fatalf("storage list length mismatch, want %d, got %d", len(slots), len(list))
		}
		if !slices.IsSortedFunc(list, common.Hash.Cmp) {
			t.Fatal("storage list is not sorted")
		}
	}
}

// TestCompactRun verifies the run selection of the background compaction,
// especially the run must never span sets with different rawStorageKey flags.
func TestCompactRun(t *testing.T) {
	newSet := func(rank int, rawKey bool) *bufferSet {
		return &bufferSet{
			rank:   rank,
			layers: 1,
			nodes:  newNodeSet(nil),
			states: newStates(nil, nil, rawKey),
		}
	}
	var cases = []struct {
		sets     []*bufferSet
		expStart int
		expEnd   int
	}{
		// Empty set list, no compactable run
		{nil, -1, -1},

		// Not enough sets to reach the threshold
		{
			[]*bufferSet{newSet(0, false), newSet(0, false), newSet(0, false)},
			-1, -1,
		},
		// A run of 8 rank0 sets with the identical flag
		{
			[]*bufferSet{
				newSet(0, false), newSet(0, false), newSet(0, false), newSet(0, false),
				newSet(0, false), newSet(0, false), newSet(0, false), newSet(0, false),
			},
			0, 8,
		},
		// 8 rank0 sets crossing the flag boundary, no compactable run
		{
			[]*bufferSet{
				newSet(0, false), newSet(0, false), newSet(0, false), newSet(0, false),
				newSet(0, true), newSet(0, true), newSet(0, true), newSet(0, true),
			},
			-1, -1,
		},
		// The sets after the flag boundary reach the threshold on their own
		{
			[]*bufferSet{
				newSet(0, false), newSet(0, false), newSet(0, false),
				newSet(0, true), newSet(0, true), newSet(0, true), newSet(0, true),
				newSet(0, true), newSet(0, true), newSet(0, true), newSet(0, true),
			},
			3, 11,
		},
		// Rank boundary splits the run regardless of the flag
		{
			[]*bufferSet{
				newSet(1, true), newSet(1, true), newSet(1, true), newSet(1, true),
				newSet(0, true), newSet(0, true), newSet(0, true), newSet(0, true),
				newSet(0, true), newSet(0, true), newSet(0, true), newSet(0, true),
			},
			4, 12,
		},
	}
	for i, c := range cases {
		start, end := compactRun(c.sets)
		if start != c.expStart || end != c.expEnd {
			t.Fatalf("unexpected run in case %d, want (%d, %d), got (%d, %d)", i, c.expStart, c.expEnd, start, end)
		}
	}
}

// TestBufferCompaction commits a batch of overlapped sets into the buffer,
// ensuring the buffer content remains correct while the accumulated sets
// are folded together in background.
func TestBufferCompaction(t *testing.T) {
	var (
		r = rand.New(rand.NewSource(0x1337))
		b = newBuffer(math.MaxInt, nil, nil, 0)

		// Reference data, aggregated from all the committed sets
		refNodes    = make(map[common.Hash]map[string]*trienode.Node)
		refAccounts = make(map[common.Hash][]byte)
		refStorages = make(map[common.Hash]map[common.Hash][]byte)
	)
	const commits = 100

	for i := 0; i < commits; i++ {
		nodes, states := randomBufferSets(r, 64)

		// Aggregate the flattened content as the reference
		for owner, subset := range nodes.storageNodes {
			if _, ok := refNodes[owner]; !ok {
				refNodes[owner] = make(map[string]*trienode.Node)
			}
			for path, n := range subset {
				refNodes[owner][path] = n
			}
		}
		if _, ok := refNodes[common.Hash{}]; !ok {
			refNodes[common.Hash{}] = make(map[string]*trienode.Node)
		}
		for path, n := range nodes.accountNodes {
			refNodes[common.Hash{}][path] = n
		}
		for hash, blob := range states.accountData {
			refAccounts[hash] = blob
		}
		for addrHash, slots := range states.storageData {
			if _, ok := refStorages[addrHash]; !ok {
				refStorages[addrHash] = make(map[common.Hash][]byte)
			}
			for slotHash, blob := range slots {
				refStorages[addrHash][slotHash] = blob
			}
		}
		b.commit(nodes, states)
	}
	if b.layers() != commits {
		t.Fatalf("layer count mismatch, want %d, got %d", commits, b.layers())
	}
	// Wait until the background compaction settles down and ensure the
	// accumulated sets were indeed folded together.
	waitCompaction(t, b)
	if sets := len(b.view.Load().sets); sets >= commits/2 {
		t.Fatalf("buffer sets were not compacted, %d sets left", sets)
	}
	verifyBuffer(t, b, refNodes, refAccounts, refStorages)

	// Flatten the buffer into a single set and cross check the content again
	nodes, states := b.flatten()
	if sets := len(b.view.Load().sets); sets != 1 {
		t.Fatalf("unexpected set count after flattening, %d", sets)
	}
	if b.layers() != commits {
		t.Fatalf("layer count mismatch after flattening, want %d, got %d", commits, b.layers())
	}
	verifyBuffer(t, b, refNodes, refAccounts, refStorages)

	// Sanity check the flattened set is consistent with the buffer view
	if nodes.size == 0 || states.size == 0 {
		t.Fatal("empty set after flattening")
	}
	if b.size() != nodes.size+states.size {
		t.Fatalf("size mismatch after flattening, want %d, got %d", nodes.size+states.size, b.size())
	}
}

// TestBufferConcurrentRead commits sets into the buffer while a batch of
// concurrent readers keep resolving the state content, ensuring the readers
// always observe fully committed data (mostly for catching data race with
// the background compaction).
func TestBufferConcurrentRead(t *testing.T) {
	var (
		b = newBuffer(math.MaxInt, nil, nil, 0)

		wg     sync.WaitGroup
		closed = make(chan struct{})
	)
	// Each key is written exactly once with a deterministic value derived
	// from the key itself, so the readers can verify the value regardless
	// of the commit progress.
	value := func(hash common.Hash) []byte {
		return crypto.Keccak256(hash.Bytes())
	}
	// Spin up the concurrent readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-closed:
					return
				default:
				}
				acct := bufTestKey("concurrent-account", rr.Intn(4096))
				if blob, ok := b.account(acct); ok {
					if !bytes.Equal(blob, value(acct)) {
						t.Errorf("unexpected account data, %x", acct)
						return
					}
				}
				if blob, ok := b.storage(acct, acct); ok {
					if !bytes.Equal(blob, value(acct)) {
						t.Errorf("unexpected storage data, %x", acct)
						return
					}
				}
				if n, ok := b.node(acct, []byte("path")); ok {
					if !bytes.Equal(n.Blob, value(acct)) {
						t.Errorf("unexpected node data, %x", acct)
						return
					}
				}
			}
		}(int64(i))
	}
	// Keep committing sets with distinct keys
	for i := 0; i < 256; i++ {
		var (
			nodes    = make(map[common.Hash]map[string]*trienode.Node)
			accounts = make(map[common.Hash][]byte)
			storages = make(map[common.Hash]map[common.Hash][]byte)
		)
		for j := 0; j < 16; j++ {
			acct := bufTestKey("concurrent-account", i*16+j)
			blob := value(acct)
			accounts[acct] = blob
			storages[acct] = map[common.Hash][]byte{acct: blob}
			nodes[acct] = map[string]*trienode.Node{"path": trienode.New(crypto.Keccak256Hash(blob), blob)}
		}
		b.commit(newNodeSet(nodes), newStates(accounts, storages, false))
	}
	close(closed)
	wg.Wait()

	waitCompaction(t, b)
	if b.layers() != 256 {
		t.Fatalf("layer count mismatch, want %d, got %d", 256, b.layers())
	}
}

// BenchmarkBufferCommit measures the latency of committing a single set into
// a buffer which already holds a significant amount of aggregated sets.
func BenchmarkBufferCommit(bench *testing.B) {
	var (
		r      = rand.New(rand.NewSource(0x1337))
		b      = newBuffer(math.MaxInt, nil, nil, 0)
		nodes  = make([]*nodeSet, 64)
		states = make([]*stateSet, 64)
	)
	for i := 0; i < 64; i++ {
		nodes[i], states[i] = randomBufferSets(r, 1024)
	}
	// Suspend the compaction to prevent unbounded growth of the background
	// merging work, the commit latency is the only measurement target.
	b.pauseCompaction()
	defer b.resumeCompaction()

	bench.ResetTimer()
	for i := 0; i < bench.N; i++ {
		// Periodically reset the buffer to keep the length of the internal
		// set list in a reasonable range.
		if i%len(nodes) == 0 {
			b.reset()
		}
		b.commit(nodes[i%len(nodes)], states[i%len(states)])
	}
}

// BenchmarkBufferRead measures the state lookup latency with the various
// number of aggregated sets remained in the buffer.
func BenchmarkBufferRead(bench *testing.B) {
	for _, sets := range []int{1, 8, 32} {
		bench.Run(fmt.Sprintf("sets-%d", sets), func(bench *testing.B) {
			r := rand.New(rand.NewSource(0x1337))
			b := newBuffer(math.MaxInt, nil, nil, 0)

			// Suspend the compaction to pin the set count
			b.pauseCompaction()
			defer b.resumeCompaction()

			var keys []common.Hash
			for i := 0; i < sets; i++ {
				nodes, states := randomBufferSets(r, 1024)
				for hash := range states.accountData {
					keys = append(keys, hash)
				}
				b.commit(nodes, states)
			}
			bench.ResetTimer()
			for i := 0; i < bench.N; i++ {
				b.account(keys[i%len(keys)])
			}
		})
	}
}

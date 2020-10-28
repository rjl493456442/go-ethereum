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

// Tests that abnormal program termination (i.e.crash) and restart can recovery
// the snapshot properly if the snapshot is enabled.

package core

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// snapshotTest is a test case for snapshot recovery. It can be used for
// simulating these scenarios:
// (i)   Geth restarts normally with valid legacy snapshot
// (ii)  Geth restarts normally with valid new-format snapshot
// (iii) Geth restarts after the crash, with broken legacy snapshot
// (iv)  Geth restarts after the crash, with broken new-format snapshot
// (v)   Geth restarts normally, but it's requested to be rewound to a lower point via SetHead
// (vi)  Geth restarts normally with a stale snapshot
type snapshotTest struct {
	legacy  bool   // Flag whether the loaded snapshot is in legacy format
	crash   bool   // Flag whether the Geth restarts from the previous crash
	gapped  int    // Number of blocks to insert without enabling snapshot
	setHead uint64 // Block number to set head back to

	chainBlocks   int    // Number of blocks to generate for the canonical chain
	snapshotBlock uint64 // Block number of the relevant snapshot disk layer
	commitBlock   uint64 // Block number for which to commit the state to disk

	expCanonicalBlocks int    // Number of canonical blocks expected to remain in the database (excl. genesis)
	expHeadHeader      uint64 // Block number of the expected head header
	expHeadFastBlock   uint64 // Block number of the expected head fast sync block
	expHeadBlock       uint64 // Block number of the expected head full block
	expSnapshotBottom  uint64 // The block height corresponding to the snapshot disk layer
}

// Tests a Geth restart with valid snapshot. Before the shutdown, all snapshot
// journal will be persisted correctly. In this case no snapshot recovery is
// required.
func TestRestartWithNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              false,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       8,
		expSnapshotBottom:  0, // Initial disk layer built from genesis
	})
}

// Tests a Geth restart with valid but "legacy" snapshot. Before the shutdown,
// all snapshot journal will be persisted correctly. In this case no snapshot
// recovery is required.
func TestRestartWithLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              false,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       8,
		expSnapshotBottom:  0, // Initial disk layer built from genesis
	})
}

// Tests a Geth was crashed and restarts with a broken snapshot.
// In this case the chain head should be rewound to the point
// with available state. And also the new head should must be lower
// than disk layer. But there is no committed point so the chain
// should be rewound to genesis and the disk layer should be left
// for recovery.
func TestNoCommitCrashWithNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        0,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
		expSnapshotBottom:  4, // Last committed disk layer, wait recovery
	})
}

// Tests a Geth was crashed and restarts with a broken snapshot.
// In this case the chain head should be rewound to the point
// with available state. And also the new head should must be lower
// than disk layer. But there is only a low committed point so the
// chain should be rewound to committed point and the disk layer
// should be left for recovery.
func TestLowCommitCrashWithNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        2,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       2,
		expSnapshotBottom:  4, // Last committed disk layer, wait recovery
	})
}

// Tests a Geth was crashed and restarts with a broken snapshot.
// In this case the chain head should be rewound to the point
// with available state. And also the new head should must be lower
// than disk layer. But there is only a high committed point so the
// chain should be rewound to genesis and the disk layer should be
// left for recovery.
func TestHighCommitCrashWithNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        6,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
		expSnapshotBottom:  4, // Last committed disk layer, wait recovery
	})
}

// Tests a Geth was crashed and restarts with a broken and "legacy format"
// snapshot. In this case the entire legacy snapshot should be discared
// and rebuild from the new chain head. The new head here refers to the
// genesis because there is no committed point.
func TestNoCommitCrashWithLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        0,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
		expSnapshotBottom:  0, // Rebuilt snapshot from the latest HEAD(genesis)
	})
}

// Tests a Geth was crashed and restarts with a broken and "legacy format"
// snapshot. In this case the entire legacy snapshot should be discared
// and rebuild from the new chain head. The new head here refers to the
// block-2 because it's committed into the disk.
func TestLowCommitCrashWithLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        2,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       2,
		expSnapshotBottom:  2, // Rebuilt snapshot from the latest HEAD
	})
}

// Tests a Geth was crashed and restarts with a broken and "legacy format"
// snapshot. In this case the entire legacy snapshot should be discared
// and rebuild from the new chain head.
// The new head here refers to the the genesis, the reason is:
// - the state of block-6 is committed into the disk
// - the legacy disk layer of block-4 is committed into the disk
// - the head is rewound the genesis in order to find an available
//   state lower than disk layer
func TestHighCommitCrashWithLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              true,
		gapped:             0,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      4,
		commitBlock:        6,
		expCanonicalBlocks: 8,
		expHeadHeader:      8,
		expHeadFastBlock:   8,
		expHeadBlock:       0,
		expSnapshotBottom:  0, // Rebuilt snapshot from the latest HEAD(genesis)
	})
}

// Tests a Geth was running with snapshot enabled. Then restarts without
// enabling snapshot and after that re-enable the snapshot again. In this
// case the snapshot should be rebuilt with latest chain head.
func TestGappedNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              false,
		gapped:             2,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 10,
		expHeadHeader:      10,
		expHeadFastBlock:   10,
		expHeadBlock:       10,
		expSnapshotBottom:  10, // Rebuilt snapshot from the latest HEAD
	})
}

// Tests a Geth was running with leagcy snapshot enabled. Then restarts
// without enabling snapshot and after that re-enable the snapshot again.
// In this case the snapshot should be rebuilt with latest chain head.
func TestGappedLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              false,
		gapped:             2,
		setHead:            0,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 10,
		expHeadHeader:      10,
		expHeadFastBlock:   10,
		expHeadBlock:       10,
		expSnapshotBottom:  10, // Rebuilt snapshot from the latest HEAD
	})
}

// Tests the Geth was running with snapshot enabled and resetHead is applied.
// In this case the head is rewound to the target(with state available). After
// that the chain is restarted and the original disk layer is kept.
func TestSetHeadWithNewSnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             false,
		crash:              false,
		gapped:             0,
		setHead:            4,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 4,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
		expSnapshotBottom:  0, // The inital disk layer is built from the genesis
	})
}

// Tests the Geth was running with snapshot(legacy-format) enabled and resetHead
// is applied. In this case the head is rewound to the target(with state available).
// After that the chain is restarted and the original disk layer is kept.
func TestSetHeadWithLegacySnapshot(t *testing.T) {
	testSnapshot(t, &snapshotTest{
		legacy:             true,
		crash:              false,
		gapped:             0,
		setHead:            4,
		chainBlocks:        8,
		snapshotBlock:      0,
		commitBlock:        0,
		expCanonicalBlocks: 4,
		expHeadHeader:      4,
		expHeadFastBlock:   4,
		expHeadBlock:       4,
		expSnapshotBottom:  0, // The inital disk layer is built from the genesis
	})
}

func testSnapshot(t *testing.T, tt *snapshotTest) {
	// It's hard to follow the test case, visualize the input
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	// fmt.Println(tt.dump(true))

	// Create a temporary persistent database
	datadir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Failed to create temporary datadir: %v", err)
	}
	os.RemoveAll(datadir)

	db, err := rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "")
	if err != nil {
		t.Fatalf("Failed to create persistent database: %v", err)
	}
	defer db.Close() // Might double close, should be fine

	// Initialize a fresh chain
	var (
		genesis = new(Genesis).MustCommit(db)
		engine  = ethash.NewFullFaker()
		gendb   = rawdb.NewMemoryDatabase()

		// Snapshot is enabled, the first snapshot is created from the Genesis.
		// The snapshot memory allowance is 256MB, it means no snapshot flush
		// will happen during the block insertion.
		cacheConfig = defaultCacheConfig
	)
	chain, err := NewBlockChain(db, cacheConfig, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create chain: %v", err)
	}
	blocks, _ := GenerateChain(params.TestChainConfig, genesis, engine, gendb, tt.chainBlocks, func(i int, b *BlockGen) {})

	// Insert the blocks with configured settings.
	var breakpoints []uint64
	if tt.commitBlock > tt.snapshotBlock {
		breakpoints = append(breakpoints, tt.snapshotBlock, tt.commitBlock)
	} else {
		breakpoints = append(breakpoints, tt.commitBlock, tt.snapshotBlock)
	}
	var startPoint uint64
	for _, point := range breakpoints {
		if _, err := chain.InsertChain(blocks[startPoint:point]); err != nil {
			t.Fatalf("Failed to import canonical chain start: %v", err)
		}
		startPoint = point

		if tt.commitBlock > 0 && tt.commitBlock == point {
			chain.stateCache.TrieDB().Commit(blocks[point-1].Root(), true, nil)
		}
		if tt.snapshotBlock > 0 && tt.snapshotBlock == point {
			if tt.legacy {
				// Here we commit the snapshot disk root to simulate
				// committing the legacy snapshot.
				rawdb.WriteSnapshotRoot(db, blocks[point-1].Root())
			} else {
				chain.snaps.Cap(blocks[point-1].Root(), 0)
				diskRoot, blockRoot := chain.snaps.DiskRoot(), blocks[point-1].Root()
				if !bytes.Equal(diskRoot.Bytes(), blockRoot.Bytes()) {
					t.Fatalf("Failed to flush disk layer change, want %x, got %x", blockRoot, diskRoot)
				}
			}
		}
	}
	if _, err := chain.InsertChain(blocks[startPoint:]); err != nil {
		t.Fatalf("Failed to import canonical chain tail: %v", err)
	}
	// Set the flag for writing legacy journal if ncessary
	if tt.legacy {
		chain.writeLegacyJournal = true
	}
	// Pull the plug on the database, simulating a hard crash
	if tt.crash {
		db.Close()

		// Start a new blockchain back up and see where the repair leads us
		db, err = rawdb.NewLevelDBDatabaseWithFreezer(datadir, 0, 0, datadir, "")
		if err != nil {
			t.Fatalf("Failed to reopen persistent database: %v", err)
		}
		defer db.Close()

		// The interesting thing is: instead of start the blockchain after
		// the crash, we do restart twice here: one after the crash and one
		// after the normal stop. It's used to ensure the broken snapshot
		// can be detected all the time.
		chain, err = NewBlockChain(db, nil, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		chain.Stop()

		chain, err = NewBlockChain(db, nil, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		defer chain.Stop()
	} else if tt.gapped > 0 {
		// Insert blocks without enabling snapshot if gapping is required.
		chain.Stop()

		gappedBlocks, _ := GenerateChain(params.TestChainConfig, blocks[len(blocks)-1], engine, gendb, tt.gapped, func(i int, b *BlockGen) {})

		// Insert a few more blocks without enabling snapshot
		var cacheConfig = &CacheConfig{
			TrieCleanLimit: 256,
			TrieDirtyLimit: 256,
			TrieTimeLimit:  5 * time.Minute,
			SnapshotLimit:  0,
		}
		chain, err = NewBlockChain(db, cacheConfig, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		chain.InsertChain(gappedBlocks)
		chain.Stop()

		chain, err = NewBlockChain(db, nil, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		defer chain.Stop()
	} else if tt.setHead != 0 {
		// Rewind the chain if setHead operation is required.
		chain.SetHead(tt.setHead)
		chain.Stop()

		chain, err = NewBlockChain(db, nil, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		defer chain.Stop()
	} else {
		chain.Stop()
		// Restart the chain normally
		chain, err = NewBlockChain(db, nil, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, nil)
		if err != nil {
			t.Fatalf("Failed to recreate chain: %v", err)
		}
		defer chain.Stop()
	}

	// Iterate over all the remaining blocks and ensure there are no gaps
	verifyNoGaps(t, chain, true, blocks)
	verifyCutoff(t, chain, true, blocks, tt.expCanonicalBlocks)

	if head := chain.CurrentHeader(); head.Number.Uint64() != tt.expHeadHeader {
		t.Errorf("Head header mismatch: have %d, want %d", head.Number, tt.expHeadHeader)
	}
	if head := chain.CurrentFastBlock(); head.NumberU64() != tt.expHeadFastBlock {
		t.Errorf("Head fast block mismatch: have %d, want %d", head.NumberU64(), tt.expHeadFastBlock)
	}
	if head := chain.CurrentBlock(); head.NumberU64() != tt.expHeadBlock {
		t.Errorf("Head block mismatch: have %d, want %d", head.NumberU64(), tt.expHeadBlock)
	}
	// Check the disk layer, ensure they are matched
	block := chain.GetBlockByNumber(tt.expSnapshotBottom)
	if block == nil {
		t.Errorf("The correspnding block[%d] of snapshot disk layer is missing", tt.expSnapshotBottom)
	} else if !bytes.Equal(chain.snaps.DiskRoot().Bytes(), block.Root().Bytes()) {
		t.Errorf("The snapshot disk layer root is incorrect, want %x, get %x", block.Root(), chain.snaps.DiskRoot())
	}
}

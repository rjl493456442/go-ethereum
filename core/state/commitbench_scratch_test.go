package state

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func benchAddr(i int) common.Address {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return common.BytesToAddress(crypto.Keccak256(b[:])[:20])
}

func benchSlot(c, i int) common.Hash {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(c))
	binary.BigEndian.PutUint64(b[8:], uint64(i))
	return crypto.Keccak256Hash(b[:])
}

type durs []time.Duration

func (d durs) stat() string {
	s := slices.Clone(d)
	slices.Sort(s)
	var sum time.Duration
	for _, v := range s {
		sum += v
	}
	pct := func(p float64) time.Duration { return s[int(float64(len(s)-1)*p)] }
	return fmt.Sprintf("mean %8.0fµs p50 %8.0fµs p95 %8.0fµs max %8.0fµs",
		float64(sum.Microseconds())/float64(len(s)), float64(pct(0.5).Microseconds()), float64(pct(0.95).Microseconds()), float64(s[len(s)-1].Microseconds()))
}

func TestCommitBreakdownScratch(t *testing.T) {
	if os.Getenv("COMMIT_BENCH") == "" {
		t.Skip("set COMMIT_BENCH")
	}
	metrics.Enable()

	var (
		nAccounts  = envInt("CB_ACCOUNTS", 2_000_000)
		nBig       = envInt("CB_BIG", 40)        // large contracts
		bigSlots   = envInt("CB_BIG_SLOTS", 50_000)
		nSmall     = envInt("CB_SMALL", 2000)    // small contracts
		smallSlots = envInt("CB_SMALL_SLOTS", 200)
		blkAcc     = envInt("CB_BLK_ACC", 1500)  // eoa touched per block
		blkBig     = envInt("CB_BLK_BIG", 25)    // big contracts touched per block
		blkBigSl   = envInt("CB_BLK_BIG_SL", 60) // slots per big contract
		blkSmall   = envInt("CB_BLK_SMALL", 350) // small contracts touched
		blkSmallSl = envInt("CB_BLK_SMALL_SL", 6)
		blkNew     = envInt("CB_BLK_NEW", 30)  // new accounts per block
		blkCode    = envInt("CB_BLK_CODE", 2)  // new contracts per block
		warmup     = envInt("CB_WARMUP", 200)  // blocks before measurement
		measure    = envInt("CB_BLOCKS", 400)  // measured blocks
		bufferMB   = envInt("CB_BUFFER_MB", 256)
	)
	rules := params.Rules{IsEIP158: true, IsByzantium: true, IsCancun: true}

	dir := os.Getenv("CB_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	kv, err := pebble.New(dir, 1024, 1024, "", false)
	if err != nil {
		t.Fatal(err)
	}
	disk := rawdb.NewDatabase(kv)
	defer disk.Close()

	conf := *pathdb.Defaults
	conf.WriteBufferSize = bufferMB * 1024 * 1024
	conf.TrieCleanSize = 512 * 1024 * 1024
	conf.StateCleanSize = 512 * 1024 * 1024
	tdb := triedb.NewDatabase(disk, &triedb.Config{PathDB: &conf})
	defer tdb.Close()
	sdb := NewDatabase(tdb, NewCodeDB(disk))

	// Populate
	start := time.Now()
	root := types.EmptyRootHash
	var block uint64
	commit := func(st *StateDB) {
		block++
		st.IntermediateRoot(rules)
		r, err := st.Commit(rules, block)
		if err != nil {
			t.Fatal(err)
		}
		root = r
	}
	const chunk = 200_000
	for i := 0; i < nAccounts; i += chunk {
		st, _ := New(root, sdb)
		for j := i; j < min(i+chunk, nAccounts); j++ {
			st.SetBalance(benchAddr(j), uint256.NewInt(uint64(j)+1), tracing.BalanceChangeUnspecified)
			st.SetNonce(benchAddr(j), 1, tracing.NonceChangeUnspecified)
		}
		commit(st)
	}
	contract := func(c int) common.Address { return benchAddr(1<<40 + c) }
	code := make([]byte, 8000)
	rand.New(rand.NewSource(1)).Read(code)
	fill := func(c, slots int) {
		for i := 0; i < slots; i += chunk {
			st, _ := New(root, sdb)
			if i == 0 {
				st.SetNonce(contract(c), 1, tracing.NonceChangeUnspecified)
				st.SetCode(contract(c), append(slices.Clone(code[:100]), byte(c), byte(c>>8)), tracing.CodeChangeUnspecified)
			}
			for j := i; j < min(i+chunk, slots); j++ {
				st.SetState(contract(c), benchSlot(c, j), common.BytesToHash([]byte{1, byte(j)}))
			}
			commit(st)
		}
	}
	{
		// batch big contracts into chunk-size commits
		st, _ := New(root, sdb)
		n := 0
		for c := 0; c < nBig; c++ {
			st.SetNonce(contract(c), 1, tracing.NonceChangeUnspecified)
			st.SetCode(contract(c), append(slices.Clone(code[:100]), byte(c), byte(c>>8)), tracing.CodeChangeUnspecified)
			for j := 0; j < bigSlots; j++ {
				st.SetState(contract(c), benchSlot(c, j), common.BytesToHash([]byte{1, byte(j)}))
				n++
				if n%chunk == 0 {
					commit(st)
					st, _ = New(root, sdb)
				}
			}
		}
		for c := nBig; c < nBig+nSmall; c++ {
			st.SetNonce(contract(c), 1, tracing.NonceChangeUnspecified)
			st.SetCode(contract(c), append(slices.Clone(code[:100]), byte(c), byte(c>>8)), tracing.CodeChangeUnspecified)
			for j := 0; j < smallSlots; j++ {
				st.SetState(contract(c), benchSlot(c, j), common.BytesToHash([]byte{1, byte(j)}))
				n++
				if n%chunk == 0 {
					commit(st)
					st, _ = New(root, sdb)
				}
			}
		}
		commit(st)
	}
	_ = fill
	if err := tdb.Commit(root, false); err != nil {
		t.Fatal(err)
	}
	t.Logf("populated in %v, %d blocks, root %x", time.Since(start), block, root)

	// Simulate
	rng := rand.New(rand.NewSource(42))
	nextNew := nAccounts
	var (
		commitMs, trieMs, accMs, stoMs, dbMs, hashMs, wall durs
	)
	runBlock := func(record bool) {
		st, err := New(root, sdb)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < blkAcc; i++ {
			st.AddBalance(benchAddr(rng.Intn(nAccounts)), uint256.NewInt(1), tracing.BalanceChangeUnspecified)
		}
		for _, c := range rng.Perm(nBig)[:min(blkBig, nBig)] {
			for j := 0; j < blkBigSl; j++ {
				k := rng.Intn(bigSlots + bigSlots/50) // ~2% new slots
				st.SetState(contract(c), benchSlot(c, k), common.BigToHash(new(uint256.Int).SetUint64(rng.Uint64()).ToBig()))
			}
		}
		for i := 0; i < blkSmall; i++ {
			c := nBig + rng.Intn(nSmall)
			for j := 0; j < blkSmallSl; j++ {
				k := rng.Intn(smallSlots + smallSlots/50)
				st.SetState(contract(c), benchSlot(c, k), common.BigToHash(new(uint256.Int).SetUint64(rng.Uint64()).ToBig()))
			}
		}
		for i := 0; i < blkNew; i++ {
			st.SetBalance(benchAddr(nextNew), uint256.NewInt(1), tracing.BalanceChangeUnspecified)
			nextNew++
		}
		for i := 0; i < blkCode; i++ {
			a := benchAddr(nextNew)
			nextNew++
			c := slices.Clone(code)
			binary.BigEndian.PutUint64(c, uint64(nextNew))
			st.SetNonce(a, 1, tracing.NonceChangeUnspecified)
			st.SetCode(a, c, tracing.CodeChangeUnspecified)
		}
		hs := time.Now()
		st.IntermediateRoot(rules)
		h := time.Since(hs)
		block++
		cs := time.Now()
		r, err := st.Commit(rules, block)
		if err != nil {
			t.Fatal(err)
		}
		w := time.Since(cs)
		root = r
		if record {
			commitMs = append(commitMs, max(st.AccountCommits, st.StorageCommits)+st.DatabaseCommits)
			trieMs = append(trieMs, max(st.AccountCommits, st.StorageCommits))
			accMs = append(accMs, st.AccountCommits)
			stoMs = append(stoMs, st.StorageCommits)
			dbMs = append(dbMs, st.DatabaseCommits)
			hashMs = append(hashMs, h)
			wall = append(wall, w)
		}
	}
	for i := 0; i < warmup; i++ {
		runBlock(false)
	}
	// discard warmup timer data
	metrics.DefaultRegistry.Each(func(name string, m any) {
		if rt, ok := m.(*metrics.ResettingTimer); ok {
			rt.Snapshot()
		}
	})
	if p := os.Getenv("CB_CPUPROF"); p != "" {
		f, _ := os.Create(p)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	for i := 0; i < measure; i++ {
		runBlock(true)
	}
	pprof.StopCPUProfile()

	t.Logf("commit_ms     %s", commitMs.stat())
	t.Logf("  trie phase  %s", trieMs.stat())
	t.Logf("    account   %s", accMs.stat())
	t.Logf("    storage   %s", stoMs.stat())
	t.Logf("  database    %s", dbMs.stat())
	t.Logf("commit wall   %s", wall.stat())
	t.Logf("hash (IR)     %s", hashMs.stat())

	var names []string
	snaps := map[string]*metrics.ResettingTimerSnapshot{}
	metrics.DefaultRegistry.Each(func(name string, m any) {
		if rt, ok := m.(*metrics.ResettingTimer); ok {
			if !strings.HasPrefix(name, "state/commit") && !strings.HasPrefix(name, "pathdb/update") && !strings.HasPrefix(name, "pathdb/layer") {
				return
			}
			s := rt.Snapshot()
			if s.Count() == 0 {
				return
			}
			names = append(names, name)
			snaps[name] = s
		}
	})
	sort.Strings(names)
	for _, n := range names {
		s := snaps[n]
		ps := s.Percentiles([]float64{0.5, 0.95})
		t.Logf("%-32s count %5d mean %8.0fµs p50 %8.0fµs p95 %8.0fµs", n, s.Count(), s.Mean()/1e3, ps[0]/1e3, ps[1]/1e3)
	}
}

// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// A legacy source can feed the v2 ingest writer without upgrading the source
// or copying its existing data into the generated database.
func TestSnapshotGenerateTrie(t *testing.T) {
	datadir := t.TempDir()
	chaindata := filepath.Join(datadir, "geth", "chaindata")
	src, err := pebble.NewV1(chaindata, 16, 16, "test/source/", false)
	if err != nil {
		t.Fatal(err)
	}
	owner := common.Hash{0x51}
	account := types.NewEmptyStateAccount()
	encoded, err := rlp.EncodeToBytes(account)
	if err != nil {
		t.Fatal(err)
	}
	st := trie.NewStackTrie(nil)
	if err := st.Update(owner[:], encoded); err != nil {
		t.Fatal(err)
	}
	root := st.Hash()
	oldRoot := []byte("original root node")
	rawdb.WriteAccountSnapshot(src, owner, types.SlimAccountRLP(*account))
	rawdb.WriteSnapshotRoot(src, root)
	rawdb.WriteAccountTrieNode(src, nil, oldRoot)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	run := func(flags ...string) string {
		t.Helper()
		cmd := runGeth(t, append([]string{"--datadir", datadir, "snapshot", "generate-trie"}, flags...)...)
		out := string(cmd.Output())
		cmd.WaitExit()
		if cmd.Err != nil {
			t.Fatalf("generate-trie %v: %v\n%s\n%s", flags, cmd.Err, out, cmd.StderrText())
		}
		for _, phase := range []string{"flat state iteration", "trie build + bookkeeping", "trie node write", "root assembly + sync"} {
			_, rest, ok := strings.Cut(out, phase)
			line, _, _ := strings.Cut(rest, "\n")
			fields := strings.Fields(strings.TrimLeft(line, ": \t"))
			if !ok || len(fields) == 0 {
				t.Fatalf("missing %q timing in benchmark output:\n%s", phase, out)
			}
			if d, err := time.ParseDuration(fields[0]); err != nil || d < 0 {
				t.Fatalf("invalid duration for %q: %s", phase, line)
			}
			if strings.Contains(line, "%") != (phase != "root assembly + sync") {
				t.Fatalf("expected worker percentage only for partition phases: %s%s", phase, line)
			}
		}
		for _, metric := range []string{"SST rewrite ratio", "SST compaction read", "SST compaction write", "write stalls", "initial ingest placement"} {
			if !strings.Contains(out, metric) {
				t.Fatalf("missing %q in benchmark output:\n%s", metric, out)
			}
		}
		return out
	}
	output := filepath.Join(t.TempDir(), "nodes")
	if out := run("--output", output, "--keep"); !strings.Contains(out, "writer:    ingest") {
		t.Fatalf("expected v2 ingest writer in command output:\n%s", out)
	}
	dest, err := pebble.New(output, 16, 16, "test/output/", true)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	if got := crypto.Keccak256Hash(rawdb.ReadAccountTrieNode(dest, nil)); got != root {
		t.Fatalf("persisted root hash %x, want %x", got, root)
	}
	if rawdb.ReadAccountSnapshot(dest, owner) != nil || rawdb.ReadSnapshotRoot(dest) != (common.Hash{}) {
		t.Fatal("output contains flat snapshot data from the source")
	}

	disposable := filepath.Join(t.TempDir(), "nodes")
	if out := run("--checkpoint", disposable, "--batch"); !strings.Contains(out, "writer:    batch") {
		t.Fatalf("expected ordinary batch writer in command output:\n%s", out)
	}
	if _, err := os.Stat(disposable); !os.IsNotExist(err) {
		t.Fatalf("temporary output still exists or stat failed: %v", err)
	}
	if !pebble.NeedsV1(chaindata) {
		t.Fatal("source database format was upgraded")
	}
	src, err = pebble.NewV1(chaindata, 16, 16, "test/source/", true)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if have := rawdb.ReadAccountTrieNode(src, nil); !bytes.Equal(have, oldRoot) {
		t.Fatalf("overwrote source root node: %x", have)
	}
}

func TestTrieCompactionRewriteRatio(t *testing.T) {
	for _, tt := range []struct {
		compacted, initial uint64
		want               string
	}{
		{0, 0, "n/a"},
		{1, 0, "n/a"},
		{0, 1000, "0.0000%"},
		{1, 3, "33.3333%"},
		{7, 4, "175.0000%"},
	} {
		if got := trieCompactionRewriteRatio(tt.compacted, tt.initial); got != tt.want {
			t.Errorf("trieCompactionRewriteRatio(%d, %d) = %q, want %q", tt.compacted, tt.initial, got, tt.want)
		}
	}
}

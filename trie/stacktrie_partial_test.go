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

package trie

import (
	"bytes"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// mkKey builds a 32-byte key from a leading hex string, right-padded with zeros
// (e.g. "3a" -> 0x3a000...0). The first nibble is prefixHex[0].
func mkKey(prefixHex string) []byte {
	return common.HexToHash(prefixHex + strings.Repeat("0", 64-len(prefixHex))).Bytes()
}

// sortedPairs turns key prefixes into 32-byte (key, value) slices sorted by key,
// as StackTrie requires. Values are distinct and 32 bytes long.
func sortedPairs(prefixes []string) (keys, vals [][]byte) {
	type kv struct{ k, v []byte }
	ps := make([]kv, len(prefixes))
	for i, p := range prefixes {
		ps[i] = kv{mkKey(p), bytes.Repeat([]byte{byte(i + 1)}, 32)}
	}
	sort.Slice(ps, func(i, j int) bool { return bytes.Compare(ps[i].k, ps[j].k) < 0 })
	for _, p := range ps {
		keys = append(keys, p.k)
		vals = append(vals, p.v)
	}
	return keys, vals
}

// partitionRoot builds partition n over the given keys and returns its subtree
// root blob (the node emitted at path [n]).
func partitionRoot(t *testing.T, n byte, keys, vals [][]byte) []byte {
	t.Helper()
	var root []byte
	pst := NewPartialStackTrie(n, func(path []byte, _ common.Hash, blob []byte) {
		if len(path) == 1 {
			root = common.CopyBytes(blob)
		}
	})
	for i := range keys {
		if err := pst.Update(keys[i], vals[i]); err != nil {
			t.Fatalf("partition update: %v", err)
		}
	}
	pst.Hash()
	return root
}

type nodeRec struct {
	hash common.Hash
	blob []byte
}

func TestReverseStackTrie(t *testing.T) {
	type builder interface {
		Update([]byte, []byte) error
		Hash() common.Hash
	}
	builders := []struct {
		name    string
		forward func(OnTrieNode) builder
		reverse func(OnTrieNode) builder
	}{
		{"full", func(cb OnTrieNode) builder { return NewStackTrie(cb) },
			func(cb OnTrieNode) builder { return NewReverseStackTrie(cb) }},
		{"partition", func(cb OnTrieNode) builder { return NewPartialStackTrie(3, cb) },
			func(cb OnTrieNode) builder { return NewReversePartialStackTrie(3, cb) }},
	}
	// Include short embedded leaves, branches with missing siblings and
	// extensions split at different depths.
	keys, vals := sortedPairs([]string{"30", "3004", "3110", "3115", "3115fa", "311b", "3a", "3fe", strings.Repeat("3", 63) + "0", strings.Repeat("3", 63) + "1"})
	for i := range vals {
		if i%2 == 0 {
			vals[i] = vals[i][:1]
		}
	}
	for _, tc := range builders {
		t.Run(tc.name, func(t *testing.T) {
			var expected common.Hash
			nodes := collect(func(onNode OnTrieNode) {
				b := tc.forward(onNode)
				for i := range keys {
					if err := b.Update(keys[i], vals[i]); err != nil {
						t.Fatal(err)
					}
				}
				expected = b.Hash()
			})
			var last []byte
			b := tc.reverse(func(path []byte, hash common.Hash, blob []byte) {
				if last != nil && bytes.Compare(last, path) <= 0 {
					t.Fatalf("paths not descending: %x followed by %x", last, path)
				}
				last = append(last[:0], path...)
				n, ok := nodes[string(path)]
				if !ok || n.hash != hash || !bytes.Equal(n.blob, blob) {
					t.Fatalf("unexpected node at %x: hash %x, blob %x", path, hash, blob)
				}
				delete(nodes, string(path))
			})
			for i := len(keys) - 1; i >= 0; i-- {
				if err := b.Update(keys[i], vals[i]); err != nil {
					t.Fatal(err)
				}
			}
			for _, key := range [][]byte{keys[0], keys[1]} {
				if err := b.Update(key, vals[0]); err == nil {
					t.Fatalf("accepted non-descending key %x", key)
				}
			}
			if root := b.Hash(); root != expected || len(nodes) != 0 {
				t.Fatalf("got root %x, want %x, missing %d nodes", root, expected, len(nodes))
			}
		})
	}
}

// collect builds a trie via the given updater and records every committed node
// keyed by its path.
func collect(update func(onNode OnTrieNode)) map[string]nodeRec {
	nodes := make(map[string]nodeRec)
	update(func(path []byte, hash common.Hash, blob []byte) {
		nodes[string(path)] = nodeRec{hash, common.CopyBytes(blob)}
	})
	return nodes
}

func TestPartitionStorageTrie(t *testing.T) {
	for _, tc := range []struct {
		name    string
		nibble  byte
		reverse bool
	}{{"ascending", 3, false}, {"descending", 9, true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The reference builds storage tries independently in ascending order.
			// The partition shares buffers while its account trie is still live.
			build := func(shared bool) (common.Hash, map[string]nodeRec) {
				var root common.Hash
				nodes := collect(func(onNode OnTrieNode) {
					order := []byte{0, 1, 6, 15}
					newAccountTrie := NewPartialStackTrie
					newStorageTrie := NewStackTrie
					if shared && tc.reverse {
						slices.Reverse(order)
						newAccountTrie = NewReversePartialStackTrie
					}
					p := newAccountTrie(tc.nibble, onNode)
					if shared {
						if p.inner.hashPool == newAccountTrie(tc.nibble, nil).inner.hashPool {
							t.Fatal("partitions share a hash buffer pool")
						}
						newStorageTrie = p.NewStorageTrie
					}
					for _, account := range order {
						st := newStorageTrie(func(path []byte, hash common.Hash, blob []byte) {
							// Keep each account's storage nodes distinct from the nibble-prefixed account nodes.
							onNode(append([]byte{16, account}, path...), hash, blob)
						})
						if shared && st.hashPool != p.inner.hashPool {
							t.Fatal("storage trie uses a different hash buffer pool from its partition")
						}
						for _, slot := range order {
							value := bytes.Repeat([]byte{account, slot}, 20)
							if slot%2 == 0 {
								value = value[:2] // exercise embedded as well as hashed children
							}
							if err := st.Update([]byte{slot}, value); err != nil {
								t.Fatal(err)
							}
						}
						h := st.Hash()
						if err := p.Update([]byte{tc.nibble<<4 | account}, h[:]); err != nil {
							t.Fatal(err)
						}
					}
					root = p.Hash()
				})
				return root, nodes
			}
			wantRoot, wantNodes := build(false)
			root, nodes := build(true)
			if root != wantRoot || !reflect.DeepEqual(nodes, wantNodes) {
				t.Fatalf("partition %x: got root %x and %d nodes, want %x and %d nodes", tc.nibble, root, len(nodes), wantRoot, len(wantNodes))
			}
		})
	}
}

// nodeKind decodes a node blob into "branch", "extension" or "leaf".
func nodeKind(t *testing.T, blob []byte) string {
	t.Helper()
	elems, err := decodeNodeElements(blob)
	if err != nil {
		t.Fatalf("decode node: %v", err)
	}
	switch len(elems) {
	case 17:
		return "branch"
	case 2:
		key, _, err := rlp.SplitString(elems[0])
		if err != nil {
			t.Fatalf("split key: %v", err)
		}
		if hasTerm(compactToHex(key)) {
			return "leaf"
		}
		return "extension"
	default:
		t.Fatalf("unexpected element count %d", len(elems))
		return ""
	}
}

// TestPartialStackTrieMatchesFullSubtree proves that, for every shape the
// partition subtree root can take, the nodes emitted by a PartialStackTrie for
// partition n are byte-for-byte identical (path, hash, blob) to the [n]-subtree
// of the full trie built from the same keys.
func TestPartialStackTrieMatchesFullSubtree(t *testing.T) {
	const n = byte(3)

	// A single key in another partition (first nibble 9 > 3, so it sorts last)
	// forces the full trie's root to be a branch, giving a clean [n]-subtree.
	otherKey := mkKey("9")
	otherVal := bytes.Repeat([]byte{0xff}, 32)

	cases := []struct {
		name     string
		keys     []string // partition-n key prefixes (first nibble must be 3)
		wantRoot string   // expected shape of the partition subtree root
	}{
		{"single-leaf", []string{"3abc"}, "leaf"},
		{"branch-root", []string{"30", "37", "3a"}, "branch"},
		{"extension-root", []string{"3110", "3115", "311a"}, "extension"},
		{"mixed", []string{"30", "3105", "310a", "3f00", "3f0f"}, "branch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys, vals := sortedPairs(tc.keys)

			// Reference: full trie over the partition-n keys plus the other-partition key.
			full := collect(func(onNode OnTrieNode) {
				st := NewStackTrie(onNode)
				for i := range keys {
					if err := st.Update(keys[i], vals[i]); err != nil {
						t.Fatalf("full update: %v", err)
					}
				}
				if err := st.Update(otherKey, otherVal); err != nil {
					t.Fatalf("full update (other): %v", err)
				}
				st.Hash()
			})

			// Subject: PartialStackTrie over just the partition-n keys.
			var partRoot common.Hash
			part := collect(func(onNode OnTrieNode) {
				pst := NewPartialStackTrie(n, onNode)
				for i := range keys {
					if err := pst.Update(keys[i], vals[i]); err != nil {
						t.Fatalf("partial update: %v", err)
					}
				}
				partRoot = pst.Hash()
			})

			// The subtree root must live at path [n] in the full trie (i.e. it is
			// hash-referenced, not inlined) and its hash must match Hash().
			rootRec, ok := full[string([]byte{n})]
			if !ok {
				t.Fatalf("full trie has no node at path [%d]", n)
			}
			if rootRec.hash != partRoot {
				t.Fatalf("partition root %x != full subtree root %x", partRoot, rootRec.hash)
			}
			if got := nodeKind(t, rootRec.blob); got != tc.wantRoot {
				t.Fatalf("subtree root kind = %s, want %s", got, tc.wantRoot)
			}

			// Every full-trie node under [n] must equal the partition's node, and
			// the partition must emit no node outside [n].
			want := make(map[string]nodeRec)
			for p, rec := range full {
				if len(p) >= 1 && p[0] == n {
					want[p] = rec
				}
			}
			if len(want) != len(part) {
				t.Fatalf("node count: full subtree=%d, partition=%d", len(want), len(part))
			}
			for p, rec := range want {
				got, ok := part[p]
				if !ok {
					t.Fatalf("partition missing node at path %x", []byte(p))
				}
				if got.hash != rec.hash || !bytes.Equal(got.blob, rec.blob) {
					t.Fatalf("node mismatch at path %x", []byte(p))
				}
			}
		})
	}
}

// TestPartialStackTrieWrongNibble checks the guard that rejects a key whose
// leading nibble does not belong to the partition.
func TestPartialStackTrieWrongNibble(t *testing.T) {
	pst := NewPartialStackTrie(3, nil)
	if err := pst.Update(mkKey("4abc"), []byte{0x01}); err == nil {
		t.Fatal("expected error for key outside the partition, got nil")
	}
}

// TestMountPartitionRoot checks that folding the leading nibble back into a
// single partition's subtree root reproduces the canonical trie root, for every
// root shape (leaf, extension, branch). The branch case is the one not reachable
// through the triedb single-partition tests.
func TestMountPartitionRoot(t *testing.T) {
	const n = byte(3)
	cases := []struct {
		name         string
		keys         []string
		wantOrphaned bool
	}{
		{"leaf", []string{"3abc"}, true},
		{"extension", []string{"3110", "3115", "311a"}, true},
		{"branch", []string{"30", "37", "3a"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys, vals := sortedPairs(tc.keys)

			// Canonical root: a plain trie over the same keys. They all share
			// nibble n, so there is no top-level branch to collapse.
			ref := NewStackTrie(nil)
			for i := range keys {
				if err := ref.Update(keys[i], vals[i]); err != nil {
					t.Fatalf("ref update: %v", err)
				}
			}
			want := ref.Hash()

			got, blob, isOrphaned, err := MountPartitionRoot(partitionRoot(t, n, keys, vals), n)
			if err != nil {
				t.Fatalf("MountPartitionRoot: %v", err)
			}
			if isOrphaned != tc.wantOrphaned {
				t.Fatalf("isOrphaned = %v, want %v", isOrphaned, tc.wantOrphaned)
			}
			if got != want {
				t.Fatalf("mounted root %x, want %x", got, want)
			}
			if crypto.Keccak256Hash(blob) != got {
				t.Fatalf("returned blob does not hash to the returned root")
			}
		})
	}
}

// TestAssembleBranch checks that packing partition subtree-root hashes into a
// top-level branch reproduces the canonical root of the union of those keys.
func TestAssembleBranch(t *testing.T) {
	keys3, vals3 := sortedPairs([]string{"30", "37", "3a"})
	keys7, vals7 := sortedPairs([]string{"71", "75"})

	// Canonical root over both partitions (all "3..." sort before all "7...").
	ref := NewStackTrie(nil)
	for i := range keys3 {
		if err := ref.Update(keys3[i], vals3[i]); err != nil {
			t.Fatalf("ref update: %v", err)
		}
	}
	for i := range keys7 {
		if err := ref.Update(keys7[i], vals7[i]); err != nil {
			t.Fatalf("ref update: %v", err)
		}
	}
	want := ref.Hash()

	var children [17][]byte
	children[3] = crypto.Keccak256(partitionRoot(t, 3, keys3, vals3))
	children[7] = crypto.Keccak256(partitionRoot(t, 7, keys7, vals7))
	blob, got, err := AssembleBranch(children)
	if err != nil {
		t.Fatalf("AssembleBranch: %v", err)
	}
	if got != want {
		t.Fatalf("assembled root %x, want %x", got, want)
	}
	if crypto.Keccak256Hash(blob) != got {
		t.Fatalf("returned blob does not hash to the returned root")
	}
}

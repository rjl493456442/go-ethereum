// Copyright 2024 The go-ethereum Authors
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
	"errors"
	"fmt"
	"runtime"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
	"golang.org/x/sync/errgroup"
)

// context wraps all fields for executing state diffs.
type context struct {
	prevRoot      common.Hash
	postRoot      common.Hash
	accounts      map[common.Address][]byte
	storages      map[common.Address]map[common.Hash][]byte
	nodes         *trienode.MergedNodeSet
	rawStorageKey bool

	// TODO (rjl493456442) abstract out the state hasher
	// for supporting verkle tree.
	accountTrie *trie.Trie
}

// storageJob captures the work required to revert a single account: replaying
// the reverse diff against its post-state storage trie and writing the account
// back into (or removing it from) the account trie.
//
// The storage-trie part of the job is self-contained: every account owns a
// distinct storage trie and reads only the immutable node database, so jobs can
// be executed concurrently. The account-trie part (recorded here as fields) is
// applied serially afterwards, since the account trie is a single shared object.
type storageJob struct {
	addr        common.Address
	addrHash    common.Hash
	isDelete    bool                // whether the account is being removed in prev-state
	storageRoot common.Hash         // storage root in post-state, where the replay starts
	wantRoot    common.Hash         // storage root expected after the replay (prev.Root, or empty for deletes)
	prev        *types.StateAccount // prev-state account to write back; nil for deletes
}

// apply processes the given state diffs, updates the corresponding post-state
// and returns the trie nodes that have been modified.
//
// The merged flag indicates that the supplied diffs are the aggregation of
// multiple consecutive state histories. In that mode an account carrying an
// empty value may already be absent in the post-state (created and deleted
// again within the merged range), in which case reverting it is a no-op rather
// than an inconsistency.
func apply(db database.NodeDatabase, prevRoot common.Hash, postRoot common.Hash, rawStorageKey bool, accounts map[common.Address][]byte, storages map[common.Address]map[common.Hash][]byte, merged bool) (map[common.Hash]map[string]*trienode.Node, error) {
	tr, err := trie.New(trie.TrieID(postRoot), db)
	if err != nil {
		return nil, err
	}
	ctx := &context{
		prevRoot:      prevRoot,
		postRoot:      postRoot,
		accounts:      accounts,
		storages:      storages,
		accountTrie:   tr,
		rawStorageKey: rawStorageKey,
		nodes:         trienode.NewMergedNodeSet(),
	}
	// Phase 1: resolve every account against the post-state account trie and
	// build the per-account jobs. The account trie is a single shared object,
	// so these reads must happen serially before the parallel phase.
	jobs, err := buildStorageJobs(ctx, merged)
	if err != nil {
		return nil, fmt.Errorf("failed to revert state, err: %w", err)
	}
	// Phase 2: replay each account's storage trie independently and in parallel.
	// Storage tries have disjoint owners and touch only the read-only node
	// database, so the commits never interfere with one another.
	results := make([]*trienode.NodeSet, len(jobs))
	g := new(errgroup.Group)
	g.SetLimit(runtime.NumCPU())
	for i := range jobs {
		g.Go(func() error {
			set, err := jobs[i].commitStorage(db, ctx)
			if err != nil {
				return err
			}
			results[i] = set
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to revert state, err: %w", err)
	}
	// Phase 3: merge the storage node sets and apply the account-trie mutations.
	// Both operate on shared state (the merged node set and the account trie),
	// so they run serially.
	for _, set := range results {
		// The returned set can be nil if the storage trie is not changed at all.
		if set == nil {
			continue
		}
		if err := ctx.nodes.Merge(set); err != nil {
			return nil, err
		}
	}
	// Apply the account updates before the deletions, mirroring the original
	// sequential order. Deleting an account can collapse a branch node in the
	// account trie, and the set of nodes that collapse (hence the tracked node
	// set) depends on this ordering.
	for _, job := range jobs {
		if !job.isDelete {
			if err := job.commitAccount(ctx); err != nil {
				return nil, fmt.Errorf("failed to revert state, err: %w", err)
			}
		}
	}
	for _, job := range jobs {
		if job.isDelete {
			if err := job.commitAccount(ctx); err != nil {
				return nil, fmt.Errorf("failed to revert state, err: %w", err)
			}
		}
	}
	root, result := tr.Commit(false)
	if root != prevRoot {
		return nil, fmt.Errorf("failed to revert state, want %#x, got %#x", prevRoot, root)
	}
	if err := ctx.nodes.Merge(result); err != nil {
		return nil, err
	}
	return ctx.nodes.Nodes(), nil
}

// buildStorageJobs reads every account from the post-state account trie and
// derives the work needed to revert it. This is the only place the shared
// account trie is read, hence it runs serially.
func buildStorageJobs(ctx *context, merged bool) ([]*storageJob, error) {
	jobs := make([]*storageJob, 0, len(ctx.accounts))
	for addr, account := range ctx.accounts {
		addrHash := crypto.Keccak256Hash(addr.Bytes())

		// The account may or may not be existent in post-state, try to load it
		// and decode if it's found.
		blob, err := ctx.accountTrie.Get(addrHash.Bytes())
		if err != nil {
			return nil, err
		}
		if len(account) == 0 {
			// The account is not present in prev-state and is expected to be
			// existent in post-state; the reverse diff wipes it out.
			if len(blob) == 0 {
				// In a merged revert the account may have been created and deleted
				// again within the aggregated range, leaving it absent in the
				// post-state. Such an account nets to no change, so reverting it
				// (and its storage, which is likewise absent) is a no-op.
				if merged {
					continue
				}
				return nil, fmt.Errorf("account is non-existent %#x", addrHash)
			}
			var post types.StateAccount
			if err := rlp.DecodeBytes(blob, &post); err != nil {
				return nil, err
			}
			jobs = append(jobs, &storageJob{
				addr:        addr,
				addrHash:    addrHash,
				isDelete:    true,
				storageRoot: post.Root,
				wantRoot:    types.EmptyRootHash,
			})
			continue
		}
		// The account was present in prev-state, decode it from the 'slim-rlp'
		// format bytes.
		prev, err := types.FullAccount(account)
		if err != nil {
			return nil, err
		}
		post := types.NewEmptyStateAccount()
		if len(blob) != 0 {
			if err := rlp.DecodeBytes(blob, &post); err != nil {
				return nil, err
			}
		}
		jobs = append(jobs, &storageJob{
			addr:        addr,
			addrHash:    addrHash,
			isDelete:    false,
			storageRoot: post.Root,
			wantRoot:    prev.Root,
			prev:        prev,
		})
	}
	return jobs, nil
}

// commitStorage replays the reverse storage diff against the post-state storage
// trie and verifies the resulting root matches the prev-state. It only reads the
// immutable node database and operates on a private storage trie, so it is safe
// to run concurrently with other jobs. The returned node set may be nil if the
// storage trie is not changed at all.
func (job *storageJob) commitStorage(db database.NodeDatabase, ctx *context) (*trienode.NodeSet, error) {
	st, err := trie.New(trie.StorageTrieID(ctx.postRoot, job.addrHash, job.storageRoot), db)
	if err != nil {
		return nil, err
	}
	var deletes [][]byte
	for key, val := range ctx.storages[job.addr] {
		tkey := key
		if ctx.rawStorageKey {
			tkey = crypto.Keccak256Hash(key.Bytes())
		}
		if len(val) == 0 {
			deletes = append(deletes, tkey.Bytes())
			continue
		}
		if job.isDelete {
			return nil, errors.New("expect storage deletion")
		}
		if err := st.Update(tkey.Bytes(), val); err != nil {
			return nil, err
		}
	}
	for _, tkey := range deletes {
		if err := st.Delete(tkey); err != nil {
			return nil, err
		}
	}
	root, result := st.Commit(false)
	if root != job.wantRoot {
		if job.isDelete {
			return nil, errors.New("failed to clear storage trie")
		}
		return nil, errors.New("failed to reset storage trie")
	}
	return result, nil
}

// commitAccount writes the prev-state account back into the shared account trie
// (or removes it for deletions). It must be called serially.
func (job *storageJob) commitAccount(ctx *context) error {
	if job.isDelete {
		return ctx.accountTrie.Delete(job.addrHash.Bytes())
	}
	full, err := rlp.EncodeToBytes(job.prev)
	if err != nil {
		return err
	}
	return ctx.accountTrie.Update(job.addrHash.Bytes(), full)
}

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

package snap

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
)

// verifyAccessList checks that the given block access list matches the hash
// committed in the block header.
func verifyAccessList(b *bal.BlockAccessList, header *types.Header) error {
	if header.BlockAccessListHash == nil {
		return fmt.Errorf("header %d has no access list hash", header.Number)
	}
	have := b.Hash()
	if have != *header.BlockAccessListHash {
		return fmt.Errorf("access list hash mismatch for block %d: have %v, want %v", header.Number, have, *header.BlockAccessListHash)
	}
	return nil
}

// isFetched tells us if accountHash has been downloaded.
func (s *syncerV2) isFetched(accountHash common.Hash) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, task := range s.tasks {
		if bytes.Compare(accountHash[:], task.Last[:]) <= 0 {
			return bytes.Compare(accountHash[:], task.Next[:]) < 0
		}
	}
	return true
}

// isStorageFetched reports whether the specified storage slot has already
// been downloaded in a previous cycle.
func (s *syncerV2) isStorageFetched(accountHash, storageHash common.Hash) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, task := range s.tasks {
		if bytes.Compare(accountHash[:], task.Last[:]) > 0 {
			continue
		}
		// The account falls within a completed account range.
		if bytes.Compare(accountHash[:], task.Next[:]) < 0 {
			return true
		}
		// All storage for this account has been synchronized.
		if _, ok := task.stateCompleted[accountHash]; ok {
			return true
		}
		// No storage sync task exists for this account yet.
		subtasks, ok := task.SubTasks[accountHash]
		if !ok {
			return false
		}
		// Check whether the slot falls within a completed storage subrange.
		for _, sub := range subtasks {
			if bytes.Compare(storageHash[:], sub.Last[:]) <= 0 {
				return bytes.Compare(storageHash[:], sub.Next[:]) < 0
			}
		}
		return true // All storage subranges for this slot have been completed.
	}
	return true // The account belongs to a completed account range.
}

// updateStorageTrie applies a set of slot mutations to an account's on-disk
// storage trie in place and returns its new root. The trie is opened through
// the path-trusting reader: parents may hold stale references on exactly the
// paths being rewritten (mixed pivots, frontier ancestors), which the update
// walk resolves positionally and replaces. The committed node set — including
// the deletions displaced by structural collapses — goes into the caller's
// batch, atomically with the flat mutations of the same block.
func (s *syncerV2) updateStorageTrie(accountHash common.Hash, slots []common.Hash, values [][]byte, batch ethdb.Batch) (common.Hash, error) {
	var (
		tr  *trie.Trie
		err error
	)
	if rootNode := rawdb.ReadStorageTrieNode(s.db, accountHash, nil); len(rootNode) > 0 {
		diskRoot := crypto.Keccak256Hash(rootNode)
		tr, err = trie.New(trie.StorageTrieID(diskRoot, accountHash, diskRoot), &repairReader{db: s.db})
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		// No generated trie exists yet, e.g. for an account created from
		// scratch by the catch-up itself. Build it up from empty.
		tr, err = trie.New(trie.StorageTrieID(types.EmptyRootHash, accountHash, types.EmptyRootHash), &repairReader{db: s.db})
		if err != nil {
			return common.Hash{}, err
		}
	}
	for i, slot := range slots {
		if len(values[i]) == 0 {
			err = tr.Delete(slot[:])
		} else {
			err = tr.Update(slot[:], values[i])
		}
		if err != nil {
			return common.Hash{}, err
		}
	}
	root, set := tr.Commit(false)
	applyNodeSet(batch, accountHash, set)
	return root, nil
}

// applyAccessList applies a single block's access list diffs to the database.
// For each account, it applies the post-block values (highest TxIdx entry)
// for balance, nonce, code, and storage — to the flat state and synchronously
// to the generated tries as well: the storage tries are updated in place
// (keeping the flat storageRoot accurate, not stale), and the account trie
// absorbs all the block's leaf changes in one commit. Everything lands in the
// single per-block batch, so flat state, trie nodes and the journaled pivot
// watermark advance atomically.
//
// The account-trie commit reaches up to the root: ancestors whose coverage
// includes not-yet-downloaded key space are rewritten with their old (stale)
// references for that space — exactly the frontier nodes of the design,
// which the range proofs of subsequent downloads overwrite in place.
func (s *syncerV2) applyAccessList(b *bal.BlockAccessList, batch ethdb.Batch) error {
	// accountTrieOps collects the block's account leaf changes, applied to
	// the account trie in a single commit at the end. Nil data is a delete.
	type accountTrieOp struct {
		hash common.Hash
		data []byte
	}
	var accountTrieOps []accountTrieOp

	// Iterate over all accounts in the access list
	for _, access := range *b {
		addr := access.Address
		accountHash := crypto.Keccak256Hash(addr[:])

		// Apply the storage mutations, collecting the fetched ones for the
		// in-place storage trie update.
		var (
			slots  []common.Hash
			values [][]byte // nil value = deletion
		)
		for _, slotWrites := range access.StorageChanges {
			if n := len(slotWrites.SlotChanges); n > 0 {
				value := slotWrites.SlotChanges[n-1].PostValue
				slotKey := slotWrites.Slot.Bytes32()
				storageHash := crypto.Keccak256Hash(slotKey[:])

				if !s.isStorageFetched(accountHash, storageHash) {
					continue
				}
				if value.IsZero() {
					rawdb.DeleteStorageSnapshot(batch, accountHash, storageHash)
					slots, values = append(slots, storageHash), append(values, nil)
				} else {
					// Store the slot in the same encoding the snapshot and the
					// trie generation use: RLP of the minimal big-endian value
					// (leading zeros trimmed), matching core/state's snapshot
					// writes.
					blob, _ := rlp.EncodeToBytes(value.Bytes())
					rawdb.WriteStorageSnapshot(batch, accountHash, storageHash, blob)
					slots, values = append(slots, storageHash), append(values, blob)
				}
			}
		}
		// Roll the account's storage trie forward. This happens even before
		// the account leaf itself is downloaded (storage can complete ahead
		// of the account range): the maintained trie tracks the network's
		// storage root, so whenever the account leaf arrives, its embedded
		// root matches what is already on disk.
		var newRoot *common.Hash
		if len(slots) > 0 {
			root, err := s.updateStorageTrie(accountHash, slots, values, batch)
			if err != nil {
				return fmt.Errorf("failed to update storage trie of %v: %w", addr, err)
			}
			newRoot = &root
		}
		if !s.isFetched(accountHash) {
			continue
		}
		// Read the existing account from flat state (may not exist yet)
		var (
			account types.StateAccount
			isNew   bool
		)
		if data := rawdb.ReadAccountSnapshot(s.db, accountHash); len(data) > 0 {
			existing, err := types.FullAccount(data)
			if err != nil {
				return fmt.Errorf("failed to decode account %v: %w", addr, err)
			}
			account = *existing
		} else {
			// New account — initialize with defaults
			isNew = true
			account.Balance = new(uint256.Int)
			account.Root = types.EmptyRootHash
			account.CodeHash = types.EmptyCodeHash[:]
		}

		// Apply balance change (last entry = post-block state)
		if n := len(access.BalanceChanges); n > 0 {
			account.Balance = new(uint256.Int).Set(access.BalanceChanges[n-1].PostBalance)
		}

		// Apply nonce change (last entry = post-block state)
		if n := len(access.NonceChanges); n > 0 {
			account.Nonce = access.NonceChanges[n-1].PostNonce
		}

		// Apply code change (last entry = post-block state)
		if n := len(access.CodeChanges); n > 0 {
			code := access.CodeChanges[n-1].NewCode
			if len(code) > 0 {
				codeHash := crypto.Keccak256(code)
				rawdb.WriteCode(batch, common.BytesToHash(codeHash), code)
				account.CodeHash = codeHash
			} else {
				account.CodeHash = types.EmptyCodeHash[:]
			}
		}
		// Absorb the freshly recomputed storage root, keeping the flat entry
		// accurate instead of intentionally stale.
		if newRoot != nil {
			account.Root = *newRoot
		}

		// Don't create empty accounts in flat state (EIP-161).
		isEmpty := account.Balance.IsZero() && account.Nonce == 0 &&
			bytes.Equal(account.CodeHash, types.EmptyCodeHash[:])
		switch {
		case isEmpty && isNew:
			// This covers cases where an account is created and destroyed within the
			// same transaction, or where its net state change across the block is zero.
			// The empty -> empty transition should be excluded from account update.
		case isEmpty && !isNew:
			// Existing account got fully drained (e.g., pre-funded
			// address that gets deployed to with init code that
			// self-destructs). Delete the entry so the trie generation
			// doesn't pick it up as an empty leaf.
			rawdb.DeleteAccountSnapshot(batch, accountHash)
			accountTrieOps = append(accountTrieOps, accountTrieOp{hash: accountHash})

			// Cascade: the account's storage trie (and any flat storage
			// leftovers) lie on no touched path and must go explicitly.
			trieStart, trieLimit := storageTrieRange(accountHash)
			deleteKeyRange(batch, trieStart, trieLimit)
			flatStart := append(bytes.Clone(rawdb.SnapshotStoragePrefix), accountHash.Bytes()...)
			deleteKeyRange(batch, flatStart, increaseKey(bytes.Clone(flatStart)))
		default:
			// Write the updated account
			rawdb.WriteAccountSnapshot(batch, accountHash, types.SlimAccountRLP(account))
			full, err := rlp.EncodeToBytes(&account)
			if err != nil {
				return fmt.Errorf("failed to encode account %v: %w", addr, err)
			}
			accountTrieOps = append(accountTrieOps, accountTrieOp{hash: accountHash, data: full})
		}
	}
	// Fold the block's account leaf changes into the account trie with a
	// single commit.
	if len(accountTrieOps) > 0 {
		var (
			tr  *trie.Trie
			err error
		)
		if rootNode := rawdb.ReadAccountTrieNode(s.db, nil); len(rootNode) > 0 {
			diskRoot := crypto.Keccak256Hash(rootNode)
			tr, err = trie.New(trie.TrieID(diskRoot), &repairReader{db: s.db})
		} else {
			tr, err = trie.New(trie.TrieID(types.EmptyRootHash), &repairReader{db: s.db})
		}
		if err != nil {
			return err
		}
		for _, op := range accountTrieOps {
			if op.data == nil {
				err = tr.Delete(op.hash[:])
			} else {
				err = tr.Update(op.hash[:], op.data)
			}
			if err != nil {
				return fmt.Errorf("failed to update account trie for %x: %w", op.hash, err)
			}
		}
		_, set := tr.Commit(false)
		applyNodeSet(batch, common.Hash{}, set)
	}
	return nil
}

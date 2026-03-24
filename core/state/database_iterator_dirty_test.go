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

package state

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// TestDirtyAccountIteratorNewAccounts verifies that newly added (not yet
// committed) accounts are visible through the dirty iteratee.
func TestDirtyAccountIteratorNewAccounts(t *testing.T) {
	testDirtyAccountIteratorNewAccounts(t, rawdb.HashScheme)
	testDirtyAccountIteratorNewAccounts(t, rawdb.PathScheme)
}

func testDirtyAccountIteratorNewAccounts(t *testing.T, scheme string) {
	_, sdb, ndb, root, accounts := makeTestState(scheme)
	ndb.Commit(root, false)

	// Create a new state on top of the committed root.
	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Add some new accounts that are not committed.
	newAddrs := []common.Address{
		common.HexToAddress("0xdeadbeef01"),
		common.HexToAddress("0xdeadbeef02"),
		common.HexToAddress("0xdeadbeef03"),
	}
	for i, addr := range newAddrs {
		state.SetNonce(addr, uint64(100+i), tracing.NonceChangeUnspecified)
		state.AddBalance(addr, uint256.NewInt(uint64(1000+i)), tracing.BalanceChangeUnspecified)
	}
	// Finalise to move dirties to pending/mutations.
	state.Finalise(false)

	// Create a dirty iteratee and iterate accounts.
	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	acctIt, err := dirtyIt.NewAccountIterator(common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create account iterator: %v", scheme, err)
	}
	defer acctIt.Release()

	seen := make(map[common.Address]bool)
	var prevHash common.Hash
	for acctIt.Next() {
		hash := acctIt.Hash()
		// Verify ordering.
		if prevHash != (common.Hash{}) && bytes.Compare(prevHash.Bytes(), hash.Bytes()) >= 0 {
			t.Fatalf("(%s) hashes not ascending: %x >= %x", scheme, prevHash, hash)
		}
		prevHash = hash

		addr, err := acctIt.Address()
		if err != nil {
			// Skip accounts without preimage (committed accounts might not have it in some setups).
			continue
		}
		seen[addr] = true

		// Verify account data is decodable.
		blob := acctIt.Account()
		if blob == nil {
			t.Fatalf("(%s) nil account at %x", scheme, hash)
		}
		var decoded types.StateAccount
		if err := rlp.DecodeBytes(blob, &decoded); err != nil {
			t.Fatalf("(%s) bad RLP at %x: %v", scheme, hash, err)
		}
	}
	if err := acctIt.Error(); err != nil {
		t.Fatalf("(%s) iteration error: %v", scheme, err)
	}
	// Verify that all new accounts are visible.
	for _, addr := range newAddrs {
		if !seen[addr] {
			t.Fatalf("(%s) new account %x not seen in dirty iteration", scheme, addr)
		}
	}
	// Verify that all original committed accounts are still visible.
	for _, acc := range accounts {
		if !seen[acc.address] {
			t.Fatalf("(%s) committed account %x not seen in dirty iteration", scheme, acc.address)
		}
	}
	// Total should be original + new.
	expectedCount := len(accounts) + len(newAddrs)
	if len(seen) != expectedCount {
		t.Fatalf("(%s) seen %d accounts, want %d", scheme, len(seen), expectedCount)
	}
}

// TestDirtyAccountIteratorModifiedAccounts verifies that modified accounts
// reflect their dirty state rather than the committed state.
func TestDirtyAccountIteratorModifiedAccounts(t *testing.T) {
	testDirtyAccountIteratorModifiedAccounts(t, rawdb.HashScheme)
	testDirtyAccountIteratorModifiedAccounts(t, rawdb.PathScheme)
}

func testDirtyAccountIteratorModifiedAccounts(t *testing.T, scheme string) {
	_, sdb, ndb, root, accounts := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Modify the first account's nonce.
	target := accounts[0].address
	newNonce := uint64(99999)
	state.SetNonce(target, newNonce, tracing.NonceChangeUnspecified)
	state.Finalise(false)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	acctIt, err := dirtyIt.NewAccountIterator(common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create account iterator: %v", scheme, err)
	}
	defer acctIt.Release()

	targetHash := crypto.Keccak256Hash(target.Bytes())
	found := false
	for acctIt.Next() {
		if acctIt.Hash() != targetHash {
			continue
		}
		found = true
		var decoded types.StateAccount
		if err := rlp.DecodeBytes(acctIt.Account(), &decoded); err != nil {
			t.Fatalf("(%s) bad RLP: %v", scheme, err)
		}
		if decoded.Nonce != newNonce {
			t.Fatalf("(%s) nonce: got %d, want %d", scheme, decoded.Nonce, newNonce)
		}
		break
	}
	if !found {
		t.Fatalf("(%s) modified account %x not found", scheme, target)
	}
}

// TestDirtyAccountIteratorDeletedAccounts verifies that self-destructed
// accounts are excluded from dirty iteration.
func TestDirtyAccountIteratorDeletedAccounts(t *testing.T) {
	testDirtyAccountIteratorDeletedAccounts(t, rawdb.HashScheme)
	testDirtyAccountIteratorDeletedAccounts(t, rawdb.PathScheme)
}

func testDirtyAccountIteratorDeletedAccounts(t *testing.T, scheme string) {
	_, sdb, ndb, root, accounts := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Self-destruct the first account.
	target := accounts[0].address
	state.SelfDestruct(target)
	state.Finalise(true)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	acctIt, err := dirtyIt.NewAccountIterator(common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create account iterator: %v", scheme, err)
	}
	defer acctIt.Release()

	targetHash := crypto.Keccak256Hash(target.Bytes())
	count := 0
	for acctIt.Next() {
		if acctIt.Hash() == targetHash {
			t.Fatalf("(%s) deleted account %x still visible in dirty iteration", scheme, target)
		}
		count++
	}
	if err := acctIt.Error(); err != nil {
		t.Fatalf("(%s) iteration error: %v", scheme, err)
	}
	if count != len(accounts)-1 {
		t.Fatalf("(%s) expected %d accounts, got %d", scheme, len(accounts)-1, count)
	}
}

// TestDirtyStorageIteratorNewSlots verifies that uncommitted storage slots
// are visible through the dirty iteratee.
func TestDirtyStorageIteratorNewSlots(t *testing.T) {
	testDirtyStorageIteratorNewSlots(t, rawdb.HashScheme)
	testDirtyStorageIteratorNewSlots(t, rawdb.PathScheme)
}

func testDirtyStorageIteratorNewSlots(t *testing.T, scheme string) {
	_, sdb, ndb, root, accounts := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Pick an account that has storage (i%5==0 in makeTestState).
	target := accounts[0].address // address 0x00, which has i=0, 0%5==0 so has storage
	targetHash := crypto.Keccak256Hash(target.Bytes())

	// Count committed storage slots.
	committedIteratee, err := sdb.Iteratee(root)
	if err != nil {
		t.Fatalf("(%s) failed to create committed iteratee: %v", scheme, err)
	}
	committedIt, err := committedIteratee.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create committed storage iterator: %v", scheme, err)
	}
	committedCount := 0
	for committedIt.Next() {
		committedCount++
	}
	committedIt.Release()

	// Add new storage slots.
	newSlots := []common.Hash{
		crypto.Keccak256Hash([]byte("new-slot-1")),
		crypto.Keccak256Hash([]byte("new-slot-2")),
	}
	for _, slot := range newSlots {
		state.SetState(target, slot, common.HexToHash("0x1234"))
	}
	state.Finalise(false)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	storageIt, err := dirtyIt.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create dirty storage iterator: %v", scheme, err)
	}
	defer storageIt.Release()

	dirtyCount := 0
	var prevHash common.Hash
	for storageIt.Next() {
		hash := storageIt.Hash()
		if prevHash != (common.Hash{}) && bytes.Compare(prevHash.Bytes(), hash.Bytes()) >= 0 {
			t.Fatalf("(%s) storage hashes not ascending: %x >= %x", scheme, prevHash, hash)
		}
		prevHash = hash
		if storageIt.Slot() == nil {
			t.Fatalf("(%s) nil slot at %x", scheme, hash)
		}
		dirtyCount++
	}
	if err := storageIt.Error(); err != nil {
		t.Fatalf("(%s) storage iteration error: %v", scheme, err)
	}
	if dirtyCount != committedCount+len(newSlots) {
		t.Fatalf("(%s) expected %d slots, got %d", scheme, committedCount+len(newSlots), dirtyCount)
	}
}

// TestDirtyStorageIteratorModifiedSlots verifies that modified storage slots
// reflect their dirty values.
func TestDirtyStorageIteratorModifiedSlots(t *testing.T) {
	testDirtyStorageIteratorModifiedSlots(t, rawdb.HashScheme)
	testDirtyStorageIteratorModifiedSlots(t, rawdb.PathScheme)
}

func testDirtyStorageIteratorModifiedSlots(t *testing.T, scheme string) {
	_, sdb, ndb, root, _ := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Account 0x00 (i=0) has storage slots. Pick the first slot key from
	// makeTestState: crypto.Keccak256Hash([]byte{0, 0, 0, 0, 0, 0, 0})
	target := common.BytesToAddress([]byte{0})
	slotKey := crypto.Keccak256Hash([]byte{0, 0, 0, 0, 0, 0, 0})
	newValue := common.HexToHash("0xabcdef")

	state.SetState(target, slotKey, newValue)
	state.Finalise(false)

	targetHash := crypto.Keccak256Hash(target.Bytes())
	slotKeyHash := crypto.Keccak256Hash(slotKey.Bytes())

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	storageIt, err := dirtyIt.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create dirty storage iterator: %v", scheme, err)
	}
	defer storageIt.Release()

	found := false
	for storageIt.Next() {
		if storageIt.Hash() != slotKeyHash {
			continue
		}
		found = true
		slot := storageIt.Slot()
		// Decode RLP-encoded slot value.
		_, content, _, err := rlp.Split(slot)
		if err != nil {
			t.Fatalf("(%s) failed to decode slot: %v", scheme, err)
		}
		decoded := common.BytesToHash(content)
		if decoded != newValue {
			t.Fatalf("(%s) slot value: got %x, want %x", scheme, decoded, newValue)
		}
		break
	}
	if !found {
		t.Fatalf("(%s) modified slot %x not found", scheme, slotKey)
	}
}

// TestDirtyStorageIteratorDeletedSlots verifies that storage slots set to
// zero are excluded from dirty iteration.
func TestDirtyStorageIteratorDeletedSlots(t *testing.T) {
	testDirtyStorageIteratorDeletedSlots(t, rawdb.HashScheme)
	testDirtyStorageIteratorDeletedSlots(t, rawdb.PathScheme)
}

func testDirtyStorageIteratorDeletedSlots(t *testing.T, scheme string) {
	_, sdb, ndb, root, _ := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	target := common.BytesToAddress([]byte{0})
	targetHash := crypto.Keccak256Hash(target.Bytes())

	// Count committed storage.
	committedIteratee, err := sdb.Iteratee(root)
	if err != nil {
		t.Fatalf("(%s) failed to create committed iteratee: %v", scheme, err)
	}
	committedIt, err := committedIteratee.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create committed storage iterator: %v", scheme, err)
	}
	committedCount := 0
	for committedIt.Next() {
		committedCount++
	}
	committedIt.Release()

	// Delete one slot by setting it to zero.
	slotKey := crypto.Keccak256Hash([]byte{0, 0, 0, 0, 0, 0, 0})
	state.SetState(target, slotKey, common.Hash{})
	state.Finalise(false)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	storageIt, err := dirtyIt.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create dirty storage iterator: %v", scheme, err)
	}
	defer storageIt.Release()

	slotKeyHash := crypto.Keccak256Hash(slotKey.Bytes())
	dirtyCount := 0
	for storageIt.Next() {
		if storageIt.Hash() == slotKeyHash {
			t.Fatalf("(%s) deleted slot %x still visible", scheme, slotKey)
		}
		dirtyCount++
	}
	if err := storageIt.Error(); err != nil {
		t.Fatalf("(%s) storage iteration error: %v", scheme, err)
	}
	if dirtyCount != committedCount-1 {
		t.Fatalf("(%s) expected %d slots, got %d", scheme, committedCount-1, dirtyCount)
	}
}

// TestDirtyStorageIteratorDestructedAccount verifies that storage iteration
// for a destructed account hides committed storage and shows only new
// dirty storage (if the account was resurrected).
func TestDirtyStorageIteratorDestructedAccount(t *testing.T) {
	testDirtyStorageIteratorDestructedAccount(t, rawdb.HashScheme)
	testDirtyStorageIteratorDestructedAccount(t, rawdb.PathScheme)
}

func testDirtyStorageIteratorDestructedAccount(t *testing.T, scheme string) {
	_, sdb, ndb, root, _ := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	target := common.BytesToAddress([]byte{0})
	targetHash := crypto.Keccak256Hash(target.Bytes())

	// Self-destruct the account.
	state.SelfDestruct(target)
	state.Finalise(true)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	storageIt, err := dirtyIt.NewStorageIterator(targetHash, common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create dirty storage iterator: %v", scheme, err)
	}
	defer storageIt.Release()

	count := 0
	for storageIt.Next() {
		count++
	}
	if err := storageIt.Error(); err != nil {
		t.Fatalf("(%s) storage iteration error: %v", scheme, err)
	}
	if count != 0 {
		t.Fatalf("(%s) expected 0 slots for destructed account, got %d", scheme, count)
	}
}

// TestDirtyAccountIteratorOrdering verifies that the merged iteration
// maintains strict ascending hash order across all entries.
func TestDirtyAccountIteratorOrdering(t *testing.T) {
	testDirtyAccountIteratorOrdering(t, rawdb.HashScheme)
	testDirtyAccountIteratorOrdering(t, rawdb.PathScheme)
}

func testDirtyAccountIteratorOrdering(t *testing.T, scheme string) {
	_, sdb, ndb, root, accounts := makeTestState(scheme)
	ndb.Commit(root, false)

	state, err := New(root, sdb)
	if err != nil {
		t.Fatalf("(%s) failed to create state: %v", scheme, err)
	}
	// Add several new accounts to interleave with committed ones.
	for i := 0; i < 20; i++ {
		addr := common.BytesToAddress(crypto.Keccak256([]byte{byte(i + 200)}))
		state.SetNonce(addr, uint64(i), tracing.NonceChangeUnspecified)
		state.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	}
	// Also modify a few existing accounts.
	for i := 0; i < 5 && i < len(accounts); i++ {
		state.SetNonce(accounts[i].address, 77777, tracing.NonceChangeUnspecified)
	}
	state.Finalise(false)

	dirtyIt, err := state.DirtyIteratee()
	if err != nil {
		t.Fatalf("(%s) failed to create dirty iteratee: %v", scheme, err)
	}
	acctIt, err := dirtyIt.NewAccountIterator(common.Hash{})
	if err != nil {
		t.Fatalf("(%s) failed to create account iterator: %v", scheme, err)
	}
	defer acctIt.Release()

	var prevHash common.Hash
	count := 0
	for acctIt.Next() {
		hash := acctIt.Hash()
		if prevHash != (common.Hash{}) && bytes.Compare(prevHash.Bytes(), hash.Bytes()) >= 0 {
			t.Fatalf("(%s) hashes not ascending at position %d: %x >= %x", scheme, count, prevHash, hash)
		}
		prevHash = hash
		count++
	}
	if err := acctIt.Error(); err != nil {
		t.Fatalf("(%s) iteration error: %v", scheme, err)
	}
	// Should have original accounts + 20 new ones.
	expected := len(accounts) + 20
	if count != expected {
		t.Fatalf("(%s) expected %d accounts, got %d", scheme, expected, count)
	}
}

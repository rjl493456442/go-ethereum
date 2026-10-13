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

package state

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// testStateUpdate constructs a state update with mainnet-like mutations: 1500
// accounts and 5000 slots spread over 100 contracts, along with the origins.
func testStateUpdate(r *rand.Rand) *StateUpdate {
	sc := &StateUpdate{
		Accounts:       make(map[common.Hash]*types.StateAccount),
		AccountsOrigin: make(map[common.Address]*types.StateAccount),
		Storages:       make(map[common.Hash]map[common.Hash]common.Hash),
		StoragesOrigin: make(map[common.Address]map[common.Hash]common.Hash),
	}
	account := func() *types.StateAccount {
		if r.Intn(10) == 0 {
			return nil
		}
		acct := types.NewEmptyStateAccount()
		acct.Nonce = r.Uint64()
		acct.Balance = uint256.NewInt(r.Uint64())
		return acct
	}
	var owners []common.Address
	for i := 0; i < 1500; i++ {
		var (
			addr common.Address
			hash common.Hash
		)
		r.Read(addr[:])
		r.Read(hash[:])
		sc.Accounts[hash] = account()
		sc.AccountsOrigin[addr] = account()
		if i < 100 {
			owners = append(owners, addr)
			sc.Storages[hash] = make(map[common.Hash]common.Hash)
			sc.StoragesOrigin[addr] = make(map[common.Hash]common.Hash)
		}
	}
	var hashes []common.Hash
	for hash := range sc.Storages {
		hashes = append(hashes, hash)
	}
	for i := 0; i < 5000; i++ {
		var slot, val common.Hash
		r.Read(slot[:])
		r.Read(val[16:])
		sc.Storages[hashes[i%len(hashes)]][slot] = val
		sc.StoragesOrigin[owners[i%len(owners)]][slot] = val
	}
	return sc
}

// TestEncodeMPTState ensures the concurrent encoding yields the same result as
// encoding the mutations one by one.
func TestEncodeMPTState(t *testing.T) {
	sc := testStateUpdate(rand.New(rand.NewSource(1)))
	accounts, accountsOrigin, storages, storagesOrigin := sc.EncodeMPTState()
	if !reflect.DeepEqual(accounts, encodeAccounts(sc.Accounts)) {
		t.Fatal("Unexpected accounts")
	}
	if !reflect.DeepEqual(accountsOrigin, encodeAccounts(sc.AccountsOrigin)) {
		t.Fatal("Unexpected account origins")
	}
	if !reflect.DeepEqual(storages, encodeStorages(sc.Storages)) {
		t.Fatal("Unexpected storages")
	}
	if !reflect.DeepEqual(storagesOrigin, encodeStorages(sc.StoragesOrigin)) {
		t.Fatal("Unexpected storage origins")
	}
}

func BenchmarkEncodeMPTState(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	pool := make([]*StateUpdate, 32)
	for i := range pool {
		pool[i] = testStateUpdate(r)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool[i%len(pool)].EncodeMPTState()
	}
}

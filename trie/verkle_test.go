// Copyright 2023 The go-ethereum Authors
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
	"math/rand"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/holiman/uint256"
)

var (
	accounts = map[common.Address]*types.StateAccount{
		{1}: {
			Nonce:    100,
			Balance:  uint256.NewInt(100),
			CodeHash: common.Hash{0x1}.Bytes(),
		},
		{2}: {
			Nonce:    200,
			Balance:  uint256.NewInt(200),
			CodeHash: common.Hash{0x2}.Bytes(),
		},
	}
	storages = map[common.Address]map[common.Hash][]byte{
		{1}: {
			common.Hash{10}: []byte{10},
			common.Hash{11}: []byte{11},
			common.MaxHash:  []byte{0xff},
		},
		{2}: {
			common.Hash{20}: []byte{20},
			common.Hash{21}: []byte{21},
			common.MaxHash:  []byte{0xff},
		},
	}
)

func TestVerkleTreeReadWrite(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	tr, _ := NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))

	for addr, acct := range accounts {
		if err := tr.UpdateAccount(addr, acct, 0); err != nil {
			t.Fatalf("Failed to update account, %v", err)
		}
		for key, val := range storages[addr] {
			if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("Failed to update account, %v", err)
			}
		}
	}

	for addr, acct := range accounts {
		stored, err := tr.GetAccount(addr)
		if err != nil {
			t.Fatalf("Failed to get account, %v", err)
		}
		if !reflect.DeepEqual(stored, acct) {
			t.Fatal("account is not matched")
		}
		for key, val := range storages[addr] {
			stored, err := tr.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("Failed to get storage, %v", err)
			}
			if !bytes.Equal(stored, val) {
				t.Fatal("storage is not matched")
			}
		}
	}
}

func TestVerkleRollBack(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	tr, _ := NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))

	for addr, acct := range accounts {
		// create more than 128 chunks of code
		code := make([]byte, 129*32)
		for i := 0; i < len(code); i += 2 {
			code[i] = 0x60
			code[i+1] = byte(i % 256)
		}
		for key, val := range storages[addr] {
			if err := tr.UpdateStorage(addr, key.Bytes(), val); err != nil {
				t.Fatalf("Failed to update account, %v", err)
			}
		}
		if err := tr.UpdateContractCode(addr, code); err != nil {
			t.Fatalf("Failed to update contract, %v", err)
		}
		if err := tr.UpdateAccount(addr, acct, len(code)); err != nil {
			t.Fatalf("Failed to update account, %v", err)
		}
	}

	// Check that things were created
	for addr, acct := range accounts {
		stored, err := tr.GetAccount(addr)
		if err != nil {
			t.Fatalf("Failed to get account, %v", err)
		}
		if !reflect.DeepEqual(stored, acct) {
			t.Fatal("account is not matched")
		}
		for key, val := range storages[addr] {
			stored, err := tr.GetStorage(addr, key.Bytes())
			if err != nil {
				t.Fatalf("Failed to get storage, %v", err)
			}
			if !bytes.Equal(stored, val) {
				t.Fatal("storage is not matched")
			}
		}
	}

	// ensure there is some code in the 2nd group of the 1st account
	keyOf2ndGroup := utils.CodeChunkKeyWithEvaluatedAddress(tr.cache.GetPoint(common.Address{1}), 128)
	chunk, err := tr.root.Get(keyOf2ndGroup, nil)
	if err != nil {
		t.Fatalf("Failed to get account, %v", err)
	}
	if len(chunk) == 0 {
		t.Fatal("contract code chunk is not found")
	}

	// Rollback first account and check that it is gone
	addr1 := common.Address{1}
	err = tr.RollBackAccount(addr1)
	if err != nil {
		t.Fatalf("error rolling back address 1: %v", err)
	}

	// ensure the account is gone
	stored, err := tr.GetAccount(addr1)
	if err != nil {
		t.Fatalf("Failed to get account, %v", err)
	}
	if stored != nil {
		t.Fatal("account was not deleted")
	}

	// ensure that the last code chunk is also gone from the tree
	chunk, err = tr.root.Get(keyOf2ndGroup, nil)
	if err != nil {
		t.Fatalf("Failed to get account, %v", err)
	}
	if len(chunk) != 0 {
		t.Fatal("contract code was not deleted")
	}
}

func TestVerkleUpdateContractCode(t *testing.T) {
	db := newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.PathScheme)
	tr, _ := NewVerkleTrie(types.EmptyVerkleHash, db, utils.NewPointCache(100))

	var codes [][]byte
	// single byte code
	codes = append(codes, []byte{0x1})

	// code with 129 chunks, two stems should be used
	codeA := make([]byte, 129*32)
	for i := 0; i < len(codeA); i += 2 {
		codeA[i] = 0x60
		codeA[i+1] = byte(i % 256)
	}
	codes = append(codes, codeA)

	// code with 385 chunks, three stems should be used
	chunkB := 128 + 1 + 256
	codeB := make([]byte, chunkB*32)
	for i := 0; i < len(codeB); i += 2 {
		codeB[i] = 0x60
		codeB[i+1] = byte((i + 1) % 256)
	}
	codes = append(codes, codeB)

	// code with 127 chunks, one stem should be used
	codeC := make([]byte, 127*32)
	for i := 0; i < len(codeC); i += 2 {
		codeC[i] = 0x60
		codeC[i+1] = byte((i + 2) % 256)
	}
	codes = append(codes, codeC)

	// reset back to single byte code
	codes = append(codes, []byte{0x2})

	// clear code
	codes = append(codes, nil)

	// random codes
	for i := 0; i < 10; i++ {
		codes = append(codes, randBytes(rand.Intn(32768)+1))
	}
	// large codes
	for i := 0; i < 10; i++ {
		codes = append(codes, randBytes(rand.Intn(32768)+16384))
	}
	for _, code := range codes {
		tr.UpdateContractCode(common.Address{0x1}, code)
		tr.UpdateAccount(common.Address{0x1}, &types.StateAccount{
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			Root:     common.Hash{},
			CodeHash: crypto.Keccak256(code),
		}, len(code))

		size, err := tr.getCodeSize(common.Address{0x1})
		if err != nil {
			t.Fatalf("Failed to get contract code, %v", err)
		}
		if int(size) != len(code) {
			t.Fatalf("Unexpected contract code size, expected %d, got %d", len(code), size)
		}
		chunks := codeChunks(len(code))
		chunkified := ChunkifyCode(code)
		for i := 0; i < chunks; i++ {
			want := chunkified[32*i : 32*(i+1)]

			got, err := tr.getCodeChunk(common.Address{0x1}, i)
			if err != nil {
				t.Fatalf("Failed to get contract code chunk, %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Unexpected contract code chunk, expected %v, got %v", want, got)
			}
		}
		// ensure the codes in the higher position has been removed
		for i := chunks; i < 2*chunks; i++ {
			got, err := tr.getCodeChunk(common.Address{0x1}, i)
			if err == nil || len(got) != 0 {
				t.Fatalf("Unexpected contract code chunk, got %v", got)
			}
		}
	}
}

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

package types

import (
	"crypto/sha256"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/merkle"
	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/common"
)

func proofRoot(value merkle.Value, index uint64, branch merkle.Values) common.Hash {
	for _, sibling := range branch {
		var input [64]byte
		if index&1 == 0 {
			copy(input[:32], value[:])
			copy(input[32:], sibling[:])
		} else {
			copy(input[:32], sibling[:])
			copy(input[32:], value[:])
		}
		value = sha256.Sum256(input[:])
		index >>= 1
	}
	return common.Hash(value)
}

func TestGloasExecProof(t *testing.T) {
	if params.BodyIndexExecBlockHashGloas != 832 {
		t.Fatalf("Gloas execution block hash index = %d, want 832", params.BodyIndexExecBlockHashGloas)
	}
	branch := make(merkle.Values, 9) // floor(log2(832))
	for i := range branch {
		branch[i][0] = byte(i + 1)
	}
	hash := common.Hash{1}
	proof := &GloasExecutionProof{ExecutionBlockHash: hash, Branch: branch}
	header := HeaderWithExecProof{
		Header: Header{BodyRoot: proofRoot(merkle.Value(hash), 832, branch)},
		Proof:  proof,
	}
	if err := header.Validate(); err != nil {
		t.Fatalf("valid Gloas execution proof rejected: %v", err)
	}
	if header.BlockHash() != hash {
		t.Fatalf("execution block hash = %s, want %s", header.BlockHash(), hash)
	}
	proof.ExecutionBlockHash[0]++
	if err := header.Validate(); err == nil {
		t.Fatal("invalid Gloas execution block hash accepted")
	}
}

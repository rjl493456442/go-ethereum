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

package api

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/beacon/merkle"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
)

func TestDecodeGloasOptimisticUpdate(t *testing.T) {
	wantHash := common.Hash{1}
	var response struct {
		Version string `json:"version"`
		Data    struct {
			Attested struct {
				Beacon struct {
					Slot          string      `json:"slot"`
					ProposerIndex string      `json:"proposer_index"`
					ParentRoot    common.Hash `json:"parent_root"`
					StateRoot     common.Hash `json:"state_root"`
					BodyRoot      common.Hash `json:"body_root"`
				} `json:"beacon"`
				ExecutionBlockHash common.Hash   `json:"execution_block_hash"`
				ExecutionBranch    merkle.Values `json:"execution_branch"`
			} `json:"attested_header"`
			Aggregate     types.SyncAggregate `json:"sync_aggregate"`
			SignatureSlot string              `json:"signature_slot"`
		} `json:"data"`
	}
	response.Version = "gloas"
	response.Data.Attested.Beacon.Slot = "1"
	response.Data.Attested.Beacon.ProposerIndex = "0"
	response.Data.Attested.Beacon.StateRoot = common.Hash{2}
	response.Data.Attested.ExecutionBlockHash = wantHash
	response.Data.Attested.ExecutionBranch = nil
	response.Data.SignatureSlot = "2"
	enc, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	update, err := decodeOptimisticUpdate(enc)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := update.Attested.Proof.(*types.GloasExecutionProof); !ok {
		t.Fatalf("Gloas proof type = %T, want *types.GloasExecutionProof", update.Attested.Proof)
	}
	if update.Attested.BlockHash() != wantHash {
		t.Fatalf("execution block hash = %s, want %s", update.Attested.BlockHash(), wantHash)
	}
}

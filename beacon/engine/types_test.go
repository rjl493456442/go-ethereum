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

package engine

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/trie"
)

func TestBlobs(t *testing.T) {
	var (
		emptyBlob          = new(kzg4844.Blob)
		emptyBlobCommit, _ = kzg4844.BlobToCommitment(emptyBlob)
		emptyBlobProof, _  = kzg4844.ComputeBlobProof(emptyBlob, emptyBlobCommit)
		emptyCellProof, _  = kzg4844.ComputeCellProofs(emptyBlob)
	)
	header := types.Header{}
	block := types.NewBlock(&header, &types.Body{}, nil, nil)

	sidecarWithoutCellProofs := types.NewBlobTxSidecar(types.BlobSidecarVersion0, []kzg4844.Blob{*emptyBlob}, []kzg4844.Commitment{emptyBlobCommit}, []kzg4844.Proof{emptyBlobProof})
	env := BlockToExecutableData(block, common.Big0, []*types.BlobTxSidecar{sidecarWithoutCellProofs}, nil)
	if len(env.BlobsBundle.Proofs) != 1 {
		t.Fatalf("Expect 1 proof in blobs bundle, got %v", len(env.BlobsBundle.Proofs))
	}

	sidecarWithCellProofs := types.NewBlobTxSidecar(types.BlobSidecarVersion0, []kzg4844.Blob{*emptyBlob}, []kzg4844.Commitment{emptyBlobCommit}, emptyCellProof)
	env = BlockToExecutableData(block, common.Big0, []*types.BlobTxSidecar{sidecarWithCellProofs}, nil)
	if len(env.BlobsBundle.Proofs) != 128 {
		t.Fatalf("Expect 128 proofs in blobs bundle, got %v", len(env.BlobsBundle.Proofs))
	}
}

// TestExecutableDataToBlockAccessList checks how the block access list carried
// by a payload is validated: an undecodable access list must be reported as
// such, before and regardless of the block hash check, while a payload without
// the field is left alone.
func TestExecutableDataToBlockAccessList(t *testing.T) {
	accessList := &bal.BlockAccessList{}
	balHash := accessList.Hash()
	header := &types.Header{
		Number:              big.NewInt(1),
		GasLimit:            30_000_000,
		Time:                1,
		Difficulty:          common.Big0,
		BaseFee:             big.NewInt(7),
		BlockAccessListHash: &balHash,
	}
	body := &types.Body{Withdrawals: []*types.Withdrawal{}}
	block := types.NewBlock(header, body, nil, trie.NewStackTrie(nil)).WithAccessList(accessList)
	valid := *BlockToExecutableData(block, common.Big0, nil, nil).ExecutionPayload

	// hashWith returns the hash of the block whose header commits to the given
	// raw access list bytes.
	hashWith := func(rawAccessList []byte) common.Hash {
		h := types.CopyHeader(header)
		balHash := crypto.Keccak256Hash(rawAccessList)
		h.BlockAccessListHash = &balHash
		return types.NewBlock(h, body, nil, trie.NewStackTrie(nil)).Hash()
	}

	tests := []struct {
		name    string
		modify  func(data *ExecutableData)
		wantErr string
	}{
		{
			name:   "valid access list",
			modify: func(data *ExecutableData) {},
		},
		{
			// The header commits to the canonical encoding while the payload
			// carries garbage: the access list is at fault, not the hash.
			name:    "undecodable access list, header commits to canonical encoding",
			modify:  func(data *ExecutableData) { data.BlockAccessList = []byte{0x80} },
			wantErr: "failed to decode BAL",
		},
		{
			// The header commits to the payload bytes, so only the decoding
			// can catch the defect.
			name: "undecodable access list, header commits to payload bytes",
			modify: func(data *ExecutableData) {
				data.BlockAccessList = []byte{0xc1}
				data.BlockHash = hashWith(data.BlockAccessList)
			},
			wantErr: "failed to decode BAL",
		},
		{
			// An empty byte string is not the encoding of an empty list.
			name:    "empty access list field",
			modify:  func(data *ExecutableData) { data.BlockAccessList = []byte{} },
			wantErr: "failed to decode BAL",
		},
		{
			// A decodable access list that is not the one the header commits
			// to is a plain block hash mismatch.
			name: "decodable access list, wrong hash",
			modify: func(data *ExecutableData) {
				data.BlockAccessList = []byte{0xc0}
				data.BlockHash = hashWith([]byte{0xc1})
			},
			wantErr: "blockhash mismatch",
		},
		{
			// Without the field the header lacks the hash and the block hash
			// check catches the difference.
			name:    "missing access list field",
			modify:  func(data *ExecutableData) { data.BlockAccessList = nil },
			wantErr: "blockhash mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := valid
			tt.modify(&data)
			result, err := ExecutableDataToBlock(data, nil, nil, nil)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result.AccessList() == nil {
					t.Fatal("access list not attached to block")
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("wrong error: got %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

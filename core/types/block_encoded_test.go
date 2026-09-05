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
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// Tests that blob transactions carrying a sidecar are detected in the encoded
// body without decoding, and that plain transactions of any kind are not.
func TestEncodedBlockHasBlobSidecars(t *testing.T) {
	blobTx := func(sidecar *BlobTxSidecar) *Transaction {
		return NewTx(&BlobTx{
			ChainID:    uint256.NewInt(1),
			Nonce:      1,
			GasTipCap:  uint256.NewInt(1),
			GasFeeCap:  uint256.NewInt(1),
			Gas:        21000,
			To:         common.Address{1},
			BlobFeeCap: uint256.NewInt(1),
			BlobHashes: []common.Hash{{1}},
			Sidecar:    sidecar,
		})
	}
	sidecar := &BlobTxSidecar{
		Blobs:       []kzg4844.Blob{{}},
		Commitments: []kzg4844.Commitment{{}},
		Proofs:      []kzg4844.Proof{{}},
	}
	legacy := NewTx(&LegacyTx{Nonce: 1, Gas: 21000, GasPrice: big.NewInt(1), To: &common.Address{1}})
	dynamic := NewTx(&DynamicFeeTx{ChainID: big.NewInt(1), Nonce: 1, Gas: 21000, GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(1), To: &common.Address{1}})

	tests := []struct {
		txs  Transactions
		want bool
	}{
		{txs: nil, want: false},
		{txs: Transactions{legacy, dynamic}, want: false},
		{txs: Transactions{legacy, blobTx(nil), dynamic}, want: false},
		{txs: Transactions{legacy, blobTx(sidecar), dynamic}, want: true},
		{txs: Transactions{blobTx(nil), blobTx(sidecar)}, want: true},
	}
	for i, tt := range tests {
		body, err := rlp.EncodeToBytes(&Body{Transactions: tt.txs, Withdrawals: Withdrawals{}})
		if err != nil {
			t.Fatalf("test %d: failed to encode body: %v", i, err)
		}
		block := &EncodedBlock{Body: body}
		have, err := block.HasBlobSidecars()
		if err != nil {
			t.Fatalf("test %d: unexpected error: %v", i, err)
		}
		if have != tt.want {
			t.Errorf("test %d: sidecar detection mismatch: have %v, want %v", i, have, tt.want)
		}
	}
	// Malformed bodies must be reported instead of silently passing
	if _, err := (&EncodedBlock{Body: []byte{0xc1}}).HasBlobSidecars(); err == nil {
		t.Errorf("malformed body not rejected")
	}
}

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
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// EncodedBlock is a block with its components kept in their RLP encoded form.
// It is used to persist chain segments retrieved from the network during snap
// sync as they are, without decoding and re-encoding them.
type EncodedBlock struct {
	Header     *Header
	Body       rlp.RawValue // Encoded body ([transactions, uncles, withdrawals?])
	Receipts   rlp.RawValue // Encoded receipts, in the storage format
	AccessList rlp.RawValue // Encoded block access list, nil if unavailable
}

// EncodeBlock converts a block and its encoded receipts into the encoded form.
func EncodeBlock(block *Block, receipts rlp.RawValue) *EncodedBlock {
	body, err := rlp.EncodeToBytes(block.Body())
	if err != nil {
		log.Crit("Failed to encode block body", "err", err)
	}
	var accessList rlp.RawValue
	if list := block.AccessList(); list != nil {
		if accessList, err = rlp.EncodeToBytes(list); err != nil {
			log.Crit("Failed to encode block access list", "err", err)
		}
	}
	return &EncodedBlock{
		Header:     block.Header(),
		Body:       body,
		Receipts:   receipts,
		AccessList: accessList,
	}
}

// EncodeBlocks converts the blocks and their encoded receipts into the encoded
// form.
func EncodeBlocks(blocks Blocks, receipts []rlp.RawValue) []*EncodedBlock {
	encoded := make([]*EncodedBlock, len(blocks))
	for i, block := range blocks {
		encoded[i] = EncodeBlock(block, receipts[i])
	}
	return encoded
}

// HasBlobSidecars reports whether any blob transaction in the encoded body
// carries a sidecar, i.e. is in its network representation. Sidecars are not
// part of the block and must not be persisted alongside it. The check inspects
// the encoding without decoding the transactions.
func (b *EncodedBlock) HasBlobSidecars() (bool, error) {
	fields, _, err := rlp.SplitList(b.Body)
	if err != nil {
		return false, err
	}
	txs, _, err := rlp.SplitList(fields)
	if err != nil {
		return false, err
	}
	for len(txs) > 0 {
		kind, content, rest, err := rlp.Split(txs)
		if err != nil {
			return false, err
		}
		txs = rest

		// Typed transactions are wrapped into an RLP string starting with the
		// type byte, legacy ones are plain lists
		if kind != rlp.String || len(content) == 0 || content[0] != BlobTxType {
			continue
		}
		// The network representation wraps the payload list into an outer list
		// along with the sidecar, whereas the first field of the plain payload
		// is the chain id, a string
		payload, _, err := rlp.SplitList(content[1:])
		if err != nil {
			return false, err
		}
		if kind, _, _, err = rlp.Split(payload); err != nil {
			return false, err
		}
		if kind == rlp.List {
			return true, nil
		}
	}
	return false, nil
}

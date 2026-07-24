// Copyright 2015 The go-ethereum Authors
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

package core

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
)

// BlockValidator is responsible for validating block bodies and processed
// state. It is deliberately kept free of any chain state so that it can be
// reused for both the regular block insertion and the stateless execution
// path. Chain-dependent checks (known block, uncle verification against
// ancestors, ancestor availability) are performed by the insert iterator.
//
// BlockValidator implements Validator.
type BlockValidator struct {
	config *params.ChainConfig // Chain configuration options
}

// NewBlockValidator returns a new block validator which is safe for re-use
func NewBlockValidator(config *params.ChainConfig) *BlockValidator {
	return &BlockValidator{config: config}
}

// ValidateBody verifies that the transactions, uncles and withdrawals given in
// the block body match the commitments in the header. It only performs checks
// that are self-contained within the block itself; chain-dependent validation
// (known block, uncle verification against ancestors and ancestor availability)
// is carried out separately by the insert iterator. The header is assumed to be
// already validated at this point.
func (v *BlockValidator) ValidateBody(block *types.Block) error {
	// check EIP 7934 RLP-encoded block size cap
	if v.config.IsOsaka(block.Number(), block.Time()) && block.Size() > params.MaxBlockSize {
		return ErrBlockOversized
	}
	// Header validity is known at this point. Here we verify that uncles, transactions
	// and withdrawals given in the block body match the header.
	header := block.Header()
	if hash := types.CalcUncleHash(block.Uncles()); hash != header.UncleHash {
		return fmt.Errorf("uncle root hash mismatch (header value %x, calculated %x)", header.UncleHash, hash)
	}
	if hash := types.DeriveSha(block.Transactions(), trie.NewStackTrie(nil)); hash != header.TxHash {
		return fmt.Errorf("transaction root hash mismatch (header value %x, calculated %x)", header.TxHash, hash)
	}

	// Withdrawals are present after the Shanghai fork.
	if header.WithdrawalsHash != nil {
		// Withdrawals list must be present in body after Shanghai.
		if block.Withdrawals() == nil {
			return errors.New("missing withdrawals in block body")
		}
		if hash := types.DeriveSha(block.Withdrawals(), trie.NewStackTrie(nil)); hash != *header.WithdrawalsHash {
			return fmt.Errorf("withdrawals root hash mismatch (header value %x, calculated %x)", *header.WithdrawalsHash, hash)
		}
	} else if block.Withdrawals() != nil {
		// Withdrawals are not allowed prior to Shanghai fork
		return errors.New("withdrawals present in block body")
	}

	// Blob transactions may be present after the Cancun fork.
	var blobs int
	for i, tx := range block.Transactions() {
		// Count the number of blobs to validate against the header's blobGasUsed
		blobs += len(tx.BlobHashes())

		// If the tx is a blob tx, it must NOT have a sidecar attached to be valid in a block.
		if tx.BlobTxSidecar() != nil {
			return fmt.Errorf("unexpected blob sidecar in transaction at index %d", i)
		}

		// The individual checks for blob validity (version-check + not empty)
		// happens in state transition.
	}

	// Check blob gas usage.
	if header.BlobGasUsed != nil {
		if want := *header.BlobGasUsed / params.BlobTxBlobGasPerBlob; uint64(blobs) != want { // div because the header is surely good vs the body might be bloated
			return fmt.Errorf("blob gas used mismatch (header %v, calculated %v)", *header.BlobGasUsed, blobs*params.BlobTxBlobGasPerBlob)
		}
	} else {
		if blobs > 0 {
			return errors.New("data blobs present in block body")
		}
	}

	// Block access list hash must be present in header after the
	// Amsterdam hard fork.
	if v.config.IsAmsterdam(block.Number(), block.Time()) {
		if block.Header().BlockAccessListHash == nil {
			return errors.New("block access list hash not set in header")
		}
		// If the block does not include an access list, compute it locally during
		// execution and validate it against the access list hash in the header.
		//
		// If the block includes an attached access list, validate it directly here.
		if block.AccessList() != nil {
			computed := block.AccessList().Hash()
			if *block.Header().BlockAccessListHash != computed {
				return fmt.Errorf("access list hash mismatch, computed: %x, remote: %x", computed, *block.Header().BlockAccessListHash)
			} else if err := block.AccessList().Validate(block.GasLimit(), len(block.Transactions())); err != nil {
				return fmt.Errorf("invalid block access list: %v", err)
			}
		}
	} else if block.Header().BlockAccessListHash != nil || block.AccessList() != nil {
		return errors.New("block had access list before Amsterdam")
	}
	return nil
}

// ValidateState validates the various changes that happen after a state
// transition, such as the amount of used gas, the logs bloom, the requests
// hash, the block access list hash, the receipt root and the state root.
//
// When stateless is set, the receipt root, state root and block access list
// root are not validated here. Those three commitments are recomputed and
// returned to the caller for cross-validation so that the stateless runner is
// forced to derive them independently rather than being handed the expected
// values. All other, self-contained checks are still performed so that they
// are shared with the regular insertion path.
func (v *BlockValidator) ValidateState(block *types.Block, statedb *state.StateDB, res *ProcessResult, stateless bool) error {
	if res == nil {
		return errors.New("nil ProcessResult value")
	}
	header := block.Header()
	if block.GasUsed() != res.GasUsed {
		return fmt.Errorf("invalid gas used (remote: %d local: %d)", block.GasUsed(), res.GasUsed)
	}
	// Validate the received block's bloom with the one derived from the generated receipts.
	// For valid blocks this should always validate to true.
	//
	// Receipts must go through MakeReceipt to calculate the receipt's bloom
	// already. Merge the receipt's bloom together instead of recalculating
	// everything.
	rbloom := types.MergeBloom(res.Receipts)
	if rbloom != header.Bloom {
		return fmt.Errorf("invalid bloom (remote: %x  local: %x)", header.Bloom, rbloom)
	}
	// Validate the parsed requests match the expected header value.
	if header.RequestsHash != nil {
		reqhash := types.CalcRequestsHash(res.Requests)
		if reqhash != *header.RequestsHash {
			return fmt.Errorf("invalid requests hash (remote: %x local: %x)", *header.RequestsHash, reqhash)
		}
	} else if res.Requests != nil {
		return errors.New("block has requests before prague fork")
	}
	// In stateless mode the receipt root, state root and block access list root
	// are deliberately not validated here: they are recomputed and returned to
	// the caller for cross-validation instead of being checked against the
	// (possibly stripped) header values. Skip them and let the caller cross-check.
	if stateless {
		return nil
	}
	// Verify the block-level access list once Amsterdam is enabled.
	if v.config.IsAmsterdam(block.Number(), block.Time()) {
		if err := v.validateBlockAccessList(block, res); err != nil {
			return err
		}
	}
	// Validate the receipt root and the state root. The receipt trie derivation
	// is executed on a background thread to overlap it with the (more expensive)
	// state root computation.
	resultCh := make(chan error, 1)
	go func() {
		// The receipt Trie's root (R = (Tr [[H1, R1], ... [Hn, Rn]]))
		receiptSha := types.DeriveSha(res.Receipts, trie.NewStackTrie(nil))
		if receiptSha != header.ReceiptHash {
			resultCh <- fmt.Errorf("invalid receipt root hash (remote: %x local: %x)", header.ReceiptHash, receiptSha)
			return
		}
		resultCh <- nil
	}()
	// Validate the state root against the received state root and throw
	// an error if they don't match.
	var rootErr error
	if root := statedb.IntermediateRoot(v.config.IsEIP158(header.Number)); header.Root != root {
		rootErr = fmt.Errorf("invalid merkle root (remote: %x local: %x) dberr: %w", header.Root, root, statedb.Error())
	}
	if err := <-resultCh; err != nil {
		return err
	}
	return rootErr
}

// validateBlockAccessList verifies the EIP-7928 block-level access list produced
// during execution against the commitment in the header. It is only meaningful
// once Amsterdam is enabled.
func (v *BlockValidator) validateBlockAccessList(block *types.Block, res *ProcessResult) error {
	if res.Bal == nil {
		return errors.New("block access list is not available in amsterdam")
	}
	if block.Header().BlockAccessListHash == nil {
		return errors.New("block access list hash not set in header")
	}
	enc := res.Bal.ToEncodingObj()
	local, remote := enc.Hash(), *block.Header().BlockAccessListHash
	if local != remote {
		return fmt.Errorf("access list hash mismatch, local: %x, remote: %x", local, remote)
	}
	if err := enc.Validate(block.GasLimit(), len(block.Transactions())); err != nil {
		return fmt.Errorf("invalid block access list: %v", err)
	}
	return nil
}

// CalcGasLimit computes the gas limit of the next block after parent. It aims
// to keep the baseline gas close to the provided target, and increase it towards
// the target if the baseline gas is lower.
func CalcGasLimit(parentGasLimit, desiredLimit uint64) uint64 {
	delta := parentGasLimit/params.GasLimitBoundDivisor - 1
	limit := parentGasLimit
	if desiredLimit < params.MinGasLimit {
		desiredLimit = params.MinGasLimit
	}
	// If we're outside our allowed gas range, we try to hone towards them
	if limit < desiredLimit {
		limit = parentGasLimit + delta
		if limit > desiredLimit {
			limit = desiredLimit
		}
		return limit
	}
	if limit > desiredLimit {
		limit = parentGasLimit - delta
		if limit < desiredLimit {
			limit = desiredLimit
		}
	}
	return limit
}

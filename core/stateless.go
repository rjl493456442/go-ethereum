// Copyright 2024 The go-ethereum Authors
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
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// StatelessResult wraps the block-derived commitments computed during a
// stateless execution. These roots are intentionally not validated inside
// ExecuteStateless; they are returned so that the caller can recompute the
// expected values independently and cross-check them. The stateless runner is
// thereby forced to derive them from scratch rather than being handed the
// expected values through the header.
type StatelessResult struct {
	StateRoot           common.Hash
	ReceiptRoot         common.Hash
	BlockAccessListRoot common.Hash // Zero value before the Amsterdam fork
}

// ExecuteStateless runs a stateless execution based on a witness, verifies
// everything it can locally and returns the state root, receipt root and block
// access list root, that need the other side to explicitly check.
//
// This method is a bit of a sore thumb here, but:
//   - It cannot be placed in core/stateless, because state.New produces a circular dep
//   - It cannot be placed outside of core, because it needs to construct a dud headerchain
//
// TODO(karalabe): Would be nice to resolve both issues above somehow and move it.
func ExecuteStateless(ctx context.Context, config *params.ChainConfig, vmconfig vm.Config, block *types.Block, witness *stateless.Witness) (*StatelessResult, error) {
	// Sanity check if the supplied block accidentally contains a set root or
	// receipt hash. If so, be very loud, but still continue.
	if block.Root() != (common.Hash{}) {
		log.Error("stateless runner received state root it's expected to calculate (faulty consensus client)", "block", block.Number())
	}
	if block.ReceiptHash() != (common.Hash{}) {
		log.Error("stateless runner received receipt root it's expected to calculate (faulty consensus client)", "block", block.Number())
	}
	// Create and populate the state database to serve as the stateless backend
	memdb := witness.MakeHashDB()
	db, err := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), state.NewCodeDB(memdb)))
	if err != nil {
		return nil, err
	}
	// Create a blockchain that is idle, but can be used to access headers through
	chain := &HeaderChain{
		config:      config,
		chainDb:     memdb,
		headerCache: lru.NewCache[common.Hash, *types.Header](256),
		engine:      beacon.New(ethash.NewFaker()),
	}
	processor := NewStateProcessor(chain)
	validator := NewBlockValidator(config)

	// Validate the block body against the header before execution. These checks
	// are self-contained within the block and are shared with the regular block
	// insertion path; the chain-dependent checks (known block, uncle
	// verification against ancestors, ancestor availability) are intentionally
	// omitted here as there is no canonical chain to validate against.
	if err = validator.ValidateBody(block); err != nil {
		return nil, err
	}
	// Run the stateless block processing and self-validate everything that can
	// be validated locally.
	res, err := processor.Process(ctx, block, db, nil, nil, vmconfig, nil)
	if err != nil {
		return nil, err
	}
	// Validate the state transition. The receipt root, state root and block
	// access list root are deliberately excluded here (stateless=true): the
	// stateless runner must recompute them from scratch and return them so the
	// caller can cross-validate against the expected values.
	if err = validator.ValidateState(block, db, res, true); err != nil {
		return nil, err
	}
	// Everything that can be self-validated has been checked. Recompute the
	// commitments that need cross-validation and return them.
	result := &StatelessResult{
		ReceiptRoot: types.DeriveSha(res.Receipts, trie.NewStackTrie(nil)),
		StateRoot:   db.IntermediateRoot(config.IsEIP158(block.Number())),
	}
	if config.IsAmsterdam(block.Number(), block.Time()) {
		result.BlockAccessListRoot = res.Bal.ToEncodingObj().Hash()
	}
	return result, nil
}

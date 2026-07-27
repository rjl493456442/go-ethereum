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

package miner

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
)

// ReplayResult holds the outputs of replaying a block through the block-building
// path. It is intended for debugging build-vs-import divergence, covering both
// the EIP-7928 block-level access list and per-transaction gas accounting.
type ReplayResult struct {
	Receipts types.Receipts       // Per-transaction receipts (GasUsed, status, logs, ...)
	GasUsed  uint64               // Total block gas used by the build path
	BAL      *bal.BlockAccessList // Block-level access list produced by the build path
	BALHash  common.Hash          // Hash of BAL
}

// ReplayBlock replays the block-building path over the given block's own header
// and transactions, returning the per-transaction receipts, total gas used, and
// the EIP-7928 block-level access list (with its hash) that block building
// produces.
//
// It is a debugging aid for diagnosing build-vs-import mismatches: run it on a
// bad block and compare the results against the block header and against the
// import path (core.StateProcessor.Process). If block building is faithfully
// reproduced, ReplayResult.GasUsed equals the header GasUsed and ReplayResult.BALHash
// equals the header BlockAccessListHash.
//
// The replay deliberately mirrors generateWork: pre-execution system calls, then
// each transaction in order, then post-execution system calls and Finalize, all
// merged into a single construction access list with the same block-access index
// (tcount+1) used for the system calls. The block's parent state must be
// available in the chain.
func (miner *Miner) ReplayBlock(ctx context.Context, block *types.Block) (*ReplayResult, error) {
	parent := miner.chain.GetHeader(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent header %#x not found", block.ParentHash())
	}
	// Reconstruct the sealing header from the block's own header, resetting the
	// fields that block building accumulates during execution so the replay
	// starts from the same point as a real sealing task.
	header := types.CopyHeader(block.Header())
	header.GasUsed = 0
	if header.BlobGasUsed != nil {
		header.BlobGasUsed = new(uint64)
	}
	header.RequestsHash = nil
	header.BlockAccessListHash = nil

	env, err := miner.makeEnv(parent, header, block.Coinbase(), false)
	if err != nil {
		return nil, err
	}
	defer env.discard()

	// Pre-execution system calls (mirrors prepareWork's tail).
	env.bal.Merge(core.PreExecution(ctx, header.ParentBeaconRoot, parent, miner.chainConfig, env.evm, header.Number, header.Time))

	// Replay the block's transactions in order. We call applyTransaction directly
	// rather than commitTransaction: bad blocks decoded from RLP carry no blob
	// sidecars, and commitTransaction routes blob txs to commitBlobTransaction,
	// which panics without a sidecar. Blob execution itself needs only the blob
	// hashes (carried in the tx), not the sidecar, so we run every tx and then
	// replicate commitBlobTransaction's non-sidecar bookkeeping (blob gas used and
	// blob count) so the replayed header matches what block building produced.
	for i, tx := range block.Transactions() {
		env.state.SetTxContext(tx.Hash(), env.tcount, uint32(env.tcount+1))
		receipt, txBal, err := miner.applyTransaction(env, tx)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		env.txs = append(env.txs, tx)
		env.receipts = append(env.receipts, receipt)
		if tx.Type() == types.BlobTxType {
			if env.header.BlobGasUsed != nil {
				*env.header.BlobGasUsed += receipt.BlobGasUsed
			}
			env.blobs += len(tx.BlobHashes())
		}
		env.tcount++
		env.bal.Merge(txBal)
	}

	// Post-execution system calls and finalize (mirrors generateWork's tail).
	body := types.Body{Transactions: env.txs, Withdrawals: block.Withdrawals()}
	allLogs := make([]*types.Log, 0)
	for _, r := range env.receipts {
		allLogs = append(allLogs, r.Logs...)
	}
	_, postBal, err := core.PostExecution(ctx, miner.chainConfig, header.Number, header.Time, allLogs, env.evm, uint32(env.tcount+1))
	if err != nil {
		return nil, err
	}
	env.bal.Merge(postBal)
	miner.engine.Finalize(miner.chain, env.header, env.state, &body, uint32(env.tcount+1), env.bal)

	enc := env.bal.ToEncodingObj()
	return &ReplayResult{
		Receipts: env.receipts,
		GasUsed:  env.gasPool.Used(),
		BAL:      enc,
		BALHash:  enc.Hash(),
	}, nil
}

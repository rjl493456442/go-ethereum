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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb/database"
	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
)

var (
	errInvalidRootType   = errors.New("invalid node type for root")
	errCorruptedMetadata = errors.New("corrupted metadata")
)

// VerkleTrie is a wrapper around VerkleNode that implements the trie.Trie
// interface so that Verkle trees can be reused verbatim.
type VerkleTrie struct {
	root   *verkle.InternalNode
	cache  *utils.PointCache
	reader *trieReader
}

// NewVerkleTrie constructs a verkle tree based on the specified root hash.
func NewVerkleTrie(root common.Hash, db database.NodeDatabase, cache *utils.PointCache) (*VerkleTrie, error) {
	reader, err := newTrieReader(root, common.Hash{}, db)
	if err != nil {
		return nil, err
	}
	// Parse the root verkle node if it's not empty.
	node := verkle.New()
	if root != types.EmptyVerkleHash && root != types.EmptyRootHash {
		blob, err := reader.node(nil, common.Hash{})
		if err != nil {
			return nil, err
		}
		node, err = verkle.ParseNode(blob, 0)
		if err != nil {
			return nil, err
		}
	}
	rn, ok := node.(*verkle.InternalNode)
	if !ok {
		return nil, errInvalidRootType
	}
	return &VerkleTrie{
		root:   rn,
		cache:  cache,
		reader: reader,
	}, nil
}

// GetKey returns the sha3 preimage of a hashed key that was previously used
// to store a value.
func (t *VerkleTrie) GetKey(key []byte) []byte {
	return nil
}

// accountStem returns the verkle tree stem of the specified address.
func (t *VerkleTrie) accountStem(addr common.Address) verkle.Stem {
	hash := t.cache.GetPointHash(addr)
	return verkle.KeyToStem(hash[:])
}

// getAccountMetadata returns the metadata of the specified account address.
// The metadata includes the account header (version, nonce, balance,
// code size, and code hash), along with any storage slots and code chunks
// that fall within the account's first stem. In other words, this function
// retrieves all data stored in the first stem occupied by the account.
//
// If the account does not exist in the Verkle tree, nil is returned.
// If the tree is corrupted, an error is returned.
func (t *VerkleTrie) getAccountMetadata(addr common.Address) ([][]byte, error) {
	values, err := t.root.GetValuesAtStem(t.accountStem(addr), t.nodeResolver)
	if err != nil {
		return nil, err
	}
	if values != nil && len(values) != verkle.NodeWidth {
		return nil, fmt.Errorf("%w, value length: %d", errCorruptedMetadata, len(values))
	}
	return values, nil
}

// GetAccount implements state.Trie, retrieving the account with the specified
// account address. If the specified account is not in the verkle tree, nil will
// be returned. If the tree is corrupted, an error will be returned.
func (t *VerkleTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	metadata, err := t.getAccountMetadata(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get account, %w, address: %x", err, addr)
	}
	if len(metadata) == 0 {
		return nil, nil // non-existent account
	}
	basicData := metadata[utils.BasicDataLeafKey]
	acc := &types.StateAccount{
		Nonce:    binary.BigEndian.Uint64(basicData[utils.BasicDataNonceOffset:]),
		Balance:  new(uint256.Int).SetBytes(basicData[utils.BasicDataBalanceOffset : utils.BasicDataBalanceOffset+16]),
		CodeHash: metadata[utils.CodeHashLeafKey],
	}
	// TODO account.Root is leave as empty. How should we handle the legacy account?
	return acc, nil
}

// GetStorage implements state.Trie, retrieving the storage slot with the specified
// account address and storage key. If the specified slot is not in the verkle tree,
// nil will be returned. If the tree is corrupted, an error will be returned.
func (t *VerkleTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	k := utils.StorageSlotKeyWithEvaluatedAddress(t.cache.GetPoint(addr), key)
	val, err := t.root.Get(k, t.nodeResolver)
	if err != nil {
		return nil, err
	}
	return common.TrimLeftZeroes(val), nil
}

// UpdateAccount implements state.Trie by writing the provided account into the tree.
// The account data written includes the code length, nonce, balance, and code hash.
// To update contract code, use UpdateContractCode instead.
//
// If the tree is corrupted, an error will be returned.
func (t *VerkleTrie) UpdateAccount(addr common.Address, acc *types.StateAccount, codeLen int) error {
	var (
		basicData [32]byte
		values    = make([][]byte, verkle.NodeWidth)
	)
	// Code size is encoded in BasicData as a 3-byte big-endian integer.
	// Spare bytes are present before the code size to support bigger
	// integers in the future. PutUint32(...) requires 4 bytes, so we
	// need to shift the offset 1 byte to the left.
	binary.BigEndian.PutUint32(basicData[utils.BasicDataCodeSizeOffset-1:], uint32(codeLen))
	binary.BigEndian.PutUint64(basicData[utils.BasicDataNonceOffset:], acc.Nonce)
	if acc.Balance.ByteLen() > 16 {
		return fmt.Errorf("balance too large, size: %d, address: %x", acc.Balance.ByteLen(), addr)
	}
	acc.Balance.WriteToSlice(basicData[utils.BasicDataBalanceOffset : utils.BasicDataBalanceOffset+16])
	values[utils.BasicDataLeafKey] = basicData[:]
	values[utils.CodeHashLeafKey] = acc.CodeHash[:]

	return t.root.InsertValuesAtStem(t.accountStem(addr), values, t.nodeResolver)
}

// UpdateStorage implements state.Trie, writing the provided storage slot into
// the tree. If the tree is corrupted, an error will be returned.
func (t *VerkleTrie) UpdateStorage(address common.Address, key, value []byte) error {
	// Left padding the slot value to 32 bytes.
	var v [32]byte
	if len(value) >= 32 {
		copy(v[:], value[:32])
	} else {
		copy(v[32-len(value):], value[:])
	}
	k := utils.StorageSlotKeyWithEvaluatedAddress(t.cache.GetPoint(address), key)
	return t.root.Insert(k, v[:], t.nodeResolver)
}

// DeleteAccount leaves the account untouched, as no account deletion can happen
// in verkle.
// There is a special corner case, in which an account that is prefunded, CREATE2-d
// and then SELFDESTRUCT-d should see its funds drained. EIP161 says that account
// should be removed, but this is verboten by the verkle spec. This contains a
// workaround in which the method checks for this corner case, and if so, overwrites
// the balance with 0. This will be removed once the spec has been clarified.
func (t *VerkleTrie) DeleteAccount(addr common.Address) error {
	values, err := t.getAccountMetadata(addr)
	if err != nil {
		return err
	}
	var prefunded bool
	for i, v := range values {
		switch i {
		case 0:
			prefunded = len(v) == 32
		case 1:
			prefunded = len(v) == 32 && bytes.Equal(v, types.EmptyCodeHash[:])
		default:
			prefunded = v == nil
		}
		if !prefunded {
			break
		}
	}
	if prefunded {
		// This function assumes the account doesn't have any contract code
		// and storage slots.
		key := utils.BasicDataKeyWithEvaluatedAddress(t.cache.GetPoint(addr))
		val := make([]byte, 32)
		t.root.Insert(key, val, t.nodeResolver)
	}
	return nil
}

// RollBackAccount removes the account info + code from the tree, unlike DeleteAccount
// that will overwrite it with 0s. The first 64 storage slots are also removed.
func (t *VerkleTrie) RollBackAccount(addr common.Address) error {
	var (
		evaluatedAddr = t.cache.GetPoint(addr)
		basicDataKey  = utils.BasicDataKeyWithEvaluatedAddress(evaluatedAddr)
	)
	basicDataBytes, err := t.root.Get(basicDataKey, t.nodeResolver)
	if err != nil {
		return fmt.Errorf("rollback: error finding code size: %w", err)
	}
	if len(basicDataBytes) == 0 {
		return errors.New("rollback: basic data is not existent")
	}
	// The code size is encoded in BasicData as a 3-byte big-endian integer. Spare bytes are present
	// before the code size to support bigger integers in the future.
	// LittleEndian.Uint32(...) expects 4-bytes, so we need to shift the offset 1-byte to the left.
	codeSize := binary.BigEndian.Uint32(basicDataBytes[utils.BasicDataCodeSizeOffset-1:])

	// Delete the account header + first 64 slots + first 128 code chunks
	_, err = t.root.DeleteAtStem(basicDataKey[:31], t.nodeResolver)
	if err != nil {
		return fmt.Errorf("error rolling back account header: %w", err)
	}

	// Delete all further code
	for i, chunknr := uint64(31*128), 128; i < uint64(codeSize); i, chunknr = i+31*256, chunknr+256 {
		// evaluate group key at the start of a new group
		key := utils.CodeChunkKeyWithEvaluatedAddress(evaluatedAddr, chunknr)
		if _, err = t.root.DeleteAtStem(verkle.KeyToStem(key), t.nodeResolver); err != nil {
			return fmt.Errorf("error deleting code chunk stem (addr=%x, offset=%d) error: %w", addr[:], chunknr, err)
		}
	}
	return nil
}

// DeleteStorage implements state.Trie, deleting the specified storage slot from
// the trie. If the storage slot was not existent in the trie, no error will be
// returned. If the trie is corrupted, an error will be returned.
func (t *VerkleTrie) DeleteStorage(addr common.Address, key []byte) error {
	var zero [32]byte
	k := utils.StorageSlotKeyWithEvaluatedAddress(t.cache.GetPoint(addr), key)
	return t.root.Insert(k, zero[:], t.nodeResolver)
}

// RollbackStorage removes the storage slot from the trie, unlike DeleteStorage
// which will write zero instead.
func (t *VerkleTrie) RollbackStorage(addr common.Address, key []byte) error {
	k := utils.StorageSlotKeyWithEvaluatedAddress(t.cache.GetPoint(addr), key)
	_, err := t.root.Delete(k, t.nodeResolver)
	return err
}

// Hash returns the root hash of the tree. It does not write to the database and
// can be used even if the tree doesn't have one.
func (t *VerkleTrie) Hash() common.Hash {
	return t.root.Commit().Bytes()
}

// Commit writes all nodes to the tree's memory database.
func (t *VerkleTrie) Commit(_ bool) (common.Hash, *trienode.NodeSet) {
	nodes, err := t.root.BatchSerialize()
	if err != nil {
		// Error return from this function indicates error in the code logic
		// of BatchSerialize, and we fail catastrophically if this is the case.
		panic(fmt.Errorf("BatchSerialize failed: %v", err))
	}
	nodeset := trienode.NewNodeSet(common.Hash{})
	for _, node := range nodes {
		// Hash parameter is not used in pathdb
		nodeset.AddNode(node.Path, trienode.New(common.Hash{}, node.SerializedBytes))
	}
	// Serialize root commitment form
	return t.Hash(), nodeset
}

// NodeIterator implements state.Trie, returning an iterator that returns
// nodes of the trie. Iteration starts at the key after the given start key.
//
// TODO(gballet, rjl493456442) implement it.
func (t *VerkleTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	panic("not implemented")
}

// Prove implements state.Trie, constructing a Merkle proof for key. The result
// contains all encoded nodes on the path to the value at key. The value itself
// is also included in the last node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root), ending
// with the node that proves the absence of the key.
//
// TODO(gballet, rjl493456442) implement it.
func (t *VerkleTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	panic("not implemented")
}

// Copy returns a deep-copied verkle tree.
func (t *VerkleTrie) Copy() *VerkleTrie {
	return &VerkleTrie{
		root:   t.root.Copy().(*verkle.InternalNode),
		cache:  t.cache,
		reader: t.reader,
	}
}

// IsVerkle indicates if the trie is a Verkle trie.
func (t *VerkleTrie) IsVerkle() bool {
	return true
}

// Proof builds and returns the verkle multiproof for keys, built against
// the pre tree. The post tree is passed in order to add the post values
// to that proof.
func (t *VerkleTrie) Proof(posttrie *VerkleTrie, keys [][]byte) (*verkle.VerkleProof, verkle.StateDiff, error) {
	var postroot verkle.VerkleNode
	if posttrie != nil {
		postroot = posttrie.root
	}
	proof, _, _, _, err := verkle.MakeVerkleMultiProof(t.root, postroot, keys, t.nodeResolver)
	if err != nil {
		return nil, nil, err
	}
	p, kvps, err := verkle.SerializeProof(proof)
	if err != nil {
		return nil, nil, err
	}
	return p, kvps, nil
}

// ChunkedCode represents a sequence of 32-bytes chunks of code (31 bytes of which
// are actual code, and 1 byte is the pushdata offset).
type ChunkedCode []byte

// Copy the values here so as to avoid an import cycle
const (
	PUSH1  = byte(0x60)
	PUSH32 = byte(0x7f)
)

// codeChunks returns the number of code chunks if the code is chunkified.
func codeChunks(codeLength int) int {
	count := codeLength / 31
	if codeLength%31 != 0 {
		count++
	}
	return count
}

// ChunkifyCode generates the chunked version of an array representing EVM bytecode
func ChunkifyCode(code []byte) ChunkedCode {
	var (
		chunkOffset = 0 // offset in the chunk
		codeOffset  = 0 // offset in the code
		chunkCount  = codeChunks(len(code))
	)
	chunks := make([]byte, chunkCount*32)
	for i := 0; i < chunkCount; i++ {
		// number of bytes to copy, 31 unless the end of the code has been reached.
		end := 31 * (i + 1)
		if len(code) < end {
			end = len(code)
		}
		copy(chunks[i*32+1:], code[31*i:end]) // copy the code itself

		// chunk offset = taken from the last chunk.
		if chunkOffset > 31 {
			// skip offset calculation if push data covers the whole chunk
			chunks[i*32] = 31
			chunkOffset = 1
			continue
		}
		chunks[32*i] = byte(chunkOffset)
		chunkOffset = 0

		// Check each instruction and update the offset it should be 0 unless
		// a PUSH-N overflows.
		for ; codeOffset < end; codeOffset++ {
			if code[codeOffset] >= PUSH1 && code[codeOffset] <= PUSH32 {
				codeOffset += int(code[codeOffset] - PUSH1 + 1)
				if codeOffset+1 >= 31*(i+1) {
					codeOffset++
					chunkOffset = codeOffset - 31*(i+1)
					break
				}
			}
		}
	}
	return chunks
}

func (t *VerkleTrie) ToDot() string {
	return verkle.ToDot(t.root)
}

func (t *VerkleTrie) nodeResolver(path []byte) ([]byte, error) {
	return t.reader.node(path, common.Hash{})
}

// Witness returns a set containing all trie nodes that have been accessed.
func (t *VerkleTrie) Witness() map[string]struct{} {
	panic("not implemented")
}

// getCodeSize returns the contract code size of the specified account.
func (t *VerkleTrie) getCodeSize(addr common.Address) (uint32, error) {
	metadata, err := t.getAccountMetadata(addr)
	if err != nil {
		return 0, err
	}
	if metadata == nil {
		return 0, nil // non-existent account
	}
	basicData := metadata[utils.BasicDataLeafKey]
	if len(basicData) != 32 {
		return 0, fmt.Errorf("%w: address: %x", errCorruptedMetadata, addr)
	}
	return binary.BigEndian.Uint32(basicData[utils.BasicDataCodeSizeOffset-1:]), nil
}

// getCodeChunk retrieves the contract code chunk specified by the id.
func (t *VerkleTrie) getCodeChunk(addr common.Address, chunkID int) ([]byte, error) {
	//size, err := t.getCodeSize(addr)
	//if err != nil {
	//	return nil, err
	//}
	//chunks := codeChunks(int(size))
	//if chunkID >= chunks {
	//	return nil, fmt.Errorf("contract code chunk out of range, want: %d, have: %d", chunkID, chunks)
	//}
	key := utils.CodeChunkKey(addr.Bytes(), chunkID)
	return t.root.Get(key, t.nodeResolver)
}

// UpdateContractCode implements state.Trie, writing the provided contract code
// into the trie. If the provided contract code is larger than the previous stored
// one, the old contract code will be automatically overwritten. In contrast, the
// extra code chunks should be removed.
func (t *VerkleTrie) UpdateContractCode(addr common.Address, code []byte) error {
	codeSize, err := t.getCodeSize(addr)
	if err != nil {
		return err
	}
	var oldChunks int
	if codeSize != 0 {
		oldChunks = codeChunks(int(codeSize))
	}
	// Write the code chunks
	var (
		chunks     = codeChunks(len(code)) // The number of code chunks
		chunkified = ChunkifyCode(code)    // The code size is a multiple of 32
		values     [][]byte
	)
	for i := 0; i < chunks; i++ {
		treeIndex, offset := utils.CodeChunkIndex(i)
		if offset == 0 /* start of new group */ || i == 0 /* first chunk in header group */ {
			values = make([][]byte, verkle.NodeWidth)
		}
		values[offset] = chunkified[32*i : 32*(i+1)]

		// Insert the group of values at the boundary of stem, or the last
		// code chunk.
		if offset == 255 || i == chunks-1 {
			stem := utils.GetStemWithEvaluatedAddress(t.cache.GetPoint(addr), treeIndex)
			if err := t.root.InsertValuesAtStem(stem, values, t.nodeResolver); err != nil {
				return err
			}
		}
	}
	// Delete code chunks in the following stems
	if chunks < oldChunks {
		for i := chunks; i < oldChunks; {
			index, offset := utils.CodeChunkIndex(i)
			if offset != 0 {
				// TODO(rjl493456442) remove the remaining chunks in the first stem
				// in batch.
				for j := 0; j < verkle.NodeWidth-int(offset) && i+j < oldChunks; j++ {
					key := utils.CodeChunkKeyWithEvaluatedAddress(t.cache.GetPoint(addr), i+j)
					if _, err := t.root.Delete(key, t.nodeResolver); err != nil {
						return err
					}
				}
				i += verkle.NodeWidth - int(offset)
			} else {
				// evaluate group key at the start of a new group
				stem := utils.GetStemWithEvaluatedAddress(t.cache.GetPoint(addr), index)
				if _, err = t.root.DeleteAtStem(stem, t.nodeResolver); err != nil {
					return fmt.Errorf("error deleting code chunk stem (addr=%x, index=%d) error: %w", addr[:], index, err)
				}
				i += verkle.NodeWidth
			}
		}
	}
	return nil
}

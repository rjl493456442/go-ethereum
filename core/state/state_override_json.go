// Code generated by github.com/fjl/gencodec. DO NOT EDIT.

package state

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/holiman/uint256"
)

var _ = (*overrideMarshaling)(nil)

// MarshalJSON marshals as JSON.
func (o OverrideAccount) MarshalJSON() ([]byte, error) {
	type OverrideAccount struct {
		Nonce     *hexutil.Uint64             `json:"nonce"`
		Code      *hexutil.Bytes              `json:"code"`
		Balance   *hexutil.U256               `json:"balance"`
		State     map[common.Hash]common.Hash `json:"state"`
		StateDiff map[common.Hash]common.Hash `json:"stateDiff"`
	}
	var enc OverrideAccount
	enc.Nonce = (*hexutil.Uint64)(o.Nonce)
	enc.Code = (*hexutil.Bytes)(o.Code)
	enc.Balance = (*hexutil.U256)(o.Balance)
	enc.State = o.State
	enc.StateDiff = o.StateDiff
	return json.Marshal(&enc)
}

// UnmarshalJSON unmarshals from JSON.
func (o *OverrideAccount) UnmarshalJSON(input []byte) error {
	type OverrideAccount struct {
		Nonce     *hexutil.Uint64             `json:"nonce"`
		Code      *hexutil.Bytes              `json:"code"`
		Balance   *hexutil.U256               `json:"balance"`
		State     map[common.Hash]common.Hash `json:"state"`
		StateDiff map[common.Hash]common.Hash `json:"stateDiff"`
	}
	var dec OverrideAccount
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.Nonce != nil {
		o.Nonce = (*uint64)(dec.Nonce)
	}
	if dec.Code != nil {
		o.Code = (*[]byte)(dec.Code)
	}
	if dec.Balance != nil {
		o.Balance = (*uint256.Int)(dec.Balance)
	}
	if dec.State != nil {
		o.State = dec.State
	}
	if dec.StateDiff != nil {
		o.StateDiff = dec.StateDiff
	}
	return nil
}

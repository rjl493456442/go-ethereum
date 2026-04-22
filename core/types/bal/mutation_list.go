package bal

import (
	"maps"

	"github.com/ethereum/go-ethereum/common"
)

type StateMutations struct {
	list map[common.Address]AccountMutations
}

func NewStateMutations() *StateMutations {
	return &StateMutations{make(map[common.Address]AccountMutations)}
}

// Merge merges the state changes present in next into the caller.  After,
// the state of the caller is the aggregate diff through next.
func (s *StateMutations) Merge(next *StateMutations) {
	if next == nil {
		return
	}
	for account, diff := range next.list {
		if mut, ok := s.list[account]; ok {
			if diff.Balance != nil {
				mut.Balance = diff.Balance
			}
			if diff.Code != nil {
				mut.Code = diff.Code
			}
			if diff.Nonce != nil {
				mut.Nonce = diff.Nonce
			}
			if len(diff.StorageWrites) > 0 {
				if mut.StorageWrites == nil {
					mut.StorageWrites = maps.Clone(diff.StorageWrites)
				} else {
					for key, val := range diff.StorageWrites {
						mut.StorageWrites[key] = val
					}
				}
			}
			s.list[account] = mut
		} else {
			s.list[account] = *diff.Copy()
		}
	}
}

func (s *StateMutations) Set(addr common.Address, mut *AccountMutations) {
	s.list[addr] = *mut
}

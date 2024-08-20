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

package pathdb

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type lookup struct {
	destructs    map[common.Hash][]common.Hash
	accounts     map[common.Hash][]common.Hash
	storages     map[common.Hash]map[common.Hash][]common.Hash
	isDescendant func(state common.Hash, ancestor common.Hash) bool
}

func newLookup(head layer, isDescendant func(state common.Hash, ancestor common.Hash) bool) *lookup {
	l := new(lookup)
	l.reset(head)
	l.isDescendant = isDescendant
	return l
}

func (l *lookup) reset(head layer) {
	var (
		current = head
		layers  []layer
	)
	for current != nil {
		layers = append(layers, current)
		current = current.parentLayer()
	}
	l.destructs = make(map[common.Hash][]common.Hash)
	l.accounts = make(map[common.Hash][]common.Hash)
	l.storages = make(map[common.Hash]map[common.Hash][]common.Hash)

	for i := len(layers) - 1; i >= 0; i-- {
		switch diff := layers[i].(type) {
		case *diskLayer:
			continue
		case *diffLayer:
			l.addLayer(diff)
		}
	}
}

func (l *lookup) lookup(destructs []common.Hash, states []common.Hash, head common.Hash) common.Hash {
	findTip := func(list []common.Hash, root common.Hash) common.Hash {
		for i := len(list) - 1; i >= 0; i-- {
			if list[i] == root || l.isDescendant(root, list[i]) {
				lookupStepMeter.Mark(int64(len(list) - i))
				return list[i]
			}
		}
		return common.Hash{}
	}
	tipA := findTip(destructs, head)
	tipB := findTip(states, head)

	switch {
	case tipA == common.Hash{} && tipB == common.Hash{}:
		return common.Hash{}
	case tipA == common.Hash{} && tipB != common.Hash{}:
		return tipB
	case tipA != common.Hash{} && tipB == common.Hash{}:
		return tipA
	default:
		if tipA == tipB {
			return tipA
		}
		if l.isDescendant(tipA, tipB) {
			return tipA
		}
		return tipB
	}
}

func (l *lookup) lookupAccount(accountHash common.Hash, state common.Hash) common.Hash {
	defer func(now time.Time) {
		lookupAccountTimeMeter.UpdateSince(now)
	}(time.Now())

	return l.lookup(l.destructs[accountHash], l.accounts[accountHash], state)
}

func (l *lookup) lookupStorage(accountHash common.Hash, storageHash common.Hash, state common.Hash) common.Hash {
	defer func(now time.Time) {
		lookupStorageTimeMeter.UpdateSince(now)
	}(time.Now())

	var storages []common.Hash
	if set, exist := l.storages[accountHash]; exist {
		storages = set[storageHash]
	}
	return l.lookup(l.destructs[accountHash], storages, state)
}

func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimeMeter.UpdateSince(now)
	}(time.Now())

	state := diff.rootHash()
	for h := range diff.states.destructSet {
		l.destructs[h] = append(l.destructs[h], state)
	}
	for h := range diff.states.accountData {
		l.accounts[h] = append(l.accounts[h], state)
	}
	for accountHash, slots := range diff.states.storageData {
		subset := l.storages[accountHash]
		if subset == nil {
			subset = make(map[common.Hash][]common.Hash)
			l.storages[accountHash] = subset
		}
		for h := range slots {
			subset[h] = append(subset[h], state)
		}
	}
}

func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimeMeter.UpdateSince(now)
	}(time.Now())

	state := diff.rootHash()
	for h := range diff.states.destructSet {
		var found bool
		for j := 0; j < len(l.destructs[h]); j++ {
			if l.destructs[h][j] == state {
				l.destructs[h] = append(l.destructs[h][:j], l.destructs[h][j+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("failed to delete lookup in destruct set %x", h)
		}
	}
	for h := range diff.states.accountData {
		var found bool
		for j := 0; j < len(l.accounts[h]); j++ {
			if l.accounts[h][j] == state {
				l.accounts[h] = append(l.accounts[h][:j], l.accounts[h][j+1:]...)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("failed to delete lookup in account set %x", h)
		}
	}
	for accountHash, slots := range diff.states.storageData {
		subset := l.storages[accountHash]
		if subset == nil {
			return fmt.Errorf("storage of %x is not tracked", accountHash)
		}
		for h := range slots {
			var found bool
			for j := 0; j < len(subset[h]); j++ {
				if subset[h][j] == state {
					subset[h] = append(subset[h][:j], subset[h][j+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("failed to delete lookup in storage set %x %x", accountHash, h)
			}
		}
		if len(subset) == 0 {
			delete(l.storages, accountHash)
		}
	}
	return nil
}

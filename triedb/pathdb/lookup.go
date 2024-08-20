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
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sync/errgroup"
)

type lookup struct {
	nodes     map[common.Hash]map[string][]common.Hash
	destructs map[common.Hash][]common.Hash
	accounts  map[common.Hash][]common.Hash
	storages  map[common.Hash]map[common.Hash][]common.Hash

	descendant func(state common.Hash, ancestor common.Hash) bool
}

func newLookup(head layer, descendant func(state common.Hash, ancestor common.Hash) bool) *lookup {
	l := new(lookup)
	l.reset(head)
	l.descendant = descendant
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
	l.nodes = make(map[common.Hash]map[string][]common.Hash)
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

func (l *lookup) nodeTip(owner common.Hash, path []byte, head common.Hash) common.Hash {
	subset, exists := l.nodes[owner]
	if !exists {
		return common.Hash{}
	}
	list := subset[string(path)]

	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == head || l.descendant(head, list[i]) {
			return list[i]
		}
	}
	return common.Hash{}
}

func (l *lookup) stateTip(destructs []common.Hash, states []common.Hash, head common.Hash) common.Hash {
	findTip := func(list []common.Hash, root common.Hash) common.Hash {
		for i := len(list) - 1; i >= 0; i-- {
			if list[i] == root || l.descendant(root, list[i]) {
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
		if l.descendant(tipA, tipB) {
			return tipA
		}
		return tipB
	}
}

func (l *lookup) accountTip(accountHash common.Hash, state common.Hash) common.Hash {
	return l.stateTip(l.destructs[accountHash], l.accounts[accountHash], state)
}

func (l *lookup) storageTip(accountHash common.Hash, storageHash common.Hash, state common.Hash) common.Hash {
	var storages []common.Hash
	if set, exist := l.storages[accountHash]; exist {
		storages = set[storageHash]
	}
	return l.stateTip(l.destructs[accountHash], storages, state)
}

func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		state    = diff.rootHash()
		nodeLock sync.Mutex
		slotLock sync.Mutex
		workers  errgroup.Group
	)
	workers.SetLimit(runtime.NumCPU() / 2)

	for accountHash, nodes := range diff.nodes.nodes {
		accountHash, nodes := accountHash, nodes // closure

		workers.Go(func() error {
			nodeLock.Lock()
			subset := l.nodes[accountHash]
			if subset == nil {
				subset = make(map[string][]common.Hash)
				l.nodes[accountHash] = subset
			}
			nodeLock.Unlock()

			for path := range nodes {
				subset[path] = append(subset[path], state)
			}
			return nil
		})
	}
	workers.Go(func() error {
		for h := range diff.states.destructSet {
			l.destructs[h] = append(l.destructs[h], state)
		}
		return nil
	})
	workers.Go(func() error {
		for h := range diff.states.accountData {
			l.accounts[h] = append(l.accounts[h], state)
		}
		return nil
	})
	for accountHash, slots := range diff.states.storageData {
		accountHash, slots := accountHash, slots // closure

		workers.Go(func() error {
			slotLock.Lock()
			subset := l.storages[accountHash]
			if subset == nil {
				subset = make(map[common.Hash][]common.Hash)
				l.storages[accountHash] = subset
			}
			slotLock.Unlock()

			for h := range slots {
				subset[h] = append(subset[h], state)
			}
			return nil
		})
	}
	workers.Wait()
}

func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())

	var (
		state    = diff.rootHash()
		nodeLock sync.RWMutex
		slotLock sync.RWMutex
		workers  errgroup.Group
	)
	workers.SetLimit(runtime.NumCPU() / 2)

	for accountHash, nodes := range diff.nodes.nodes {
		accountHash, nodes := accountHash, nodes // closure

		workers.Go(func() error {
			nodeLock.RLock()
			subset := l.nodes[accountHash]
			if subset == nil {
				nodeLock.RUnlock()
				return fmt.Errorf("unknown node owner %x", accountHash)
			}
			nodeLock.RUnlock()

			for path := range nodes {
				var found bool
				for j := 0; j < len(subset[path]); j++ {
					if subset[path][j] == state {
						if j == 0 {
							subset[path] = subset[path][1:]
						} else {
							subset[path] = append(subset[path][:j], subset[path][j+1:]...)
						}
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("failed to delete lookup %x %v", accountHash, []byte(path))
				}
				if len(subset[path]) == 0 {
					delete(subset, path)
				}
			}
			if len(subset) == 0 {
				nodeLock.Lock()
				delete(l.nodes, accountHash)
				nodeLock.Unlock()
			}
			return nil
		})
	}
	workers.Go(func() error {
		for h := range diff.states.destructSet {
			var found bool
			for j := 0; j < len(l.destructs[h]); j++ {
				if l.destructs[h][j] == state {
					if j == 0 {
						l.destructs[h] = l.destructs[h][1:]
					} else {
						l.destructs[h] = append(l.destructs[h][:j], l.destructs[h][j+1:]...)
					}
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("failed to delete lookup in destruct set %x", h)
			}
			if len(l.destructs[h]) == 0 {
				delete(l.destructs, h)
			}
		}
		return nil
	})
	workers.Go(func() error {
		for h := range diff.states.accountData {
			var found bool
			for j := 0; j < len(l.accounts[h]); j++ {
				if l.accounts[h][j] == state {
					if j == 0 {
						l.accounts[h] = l.accounts[h][1:]
					} else {
						l.accounts[h] = append(l.accounts[h][:j], l.accounts[h][j+1:]...)
					}
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("failed to delete lookup in account set %x", h)
			}
			if len(l.accounts[h]) == 0 {
				delete(l.accounts, h)
			}
		}
		return nil
	})
	for accountHash, slots := range diff.states.storageData {
		accountHash, slots := accountHash, slots // closure

		workers.Go(func() error {
			slotLock.RLock()
			subset := l.storages[accountHash]
			if subset == nil {
				slotLock.RUnlock()
				return fmt.Errorf("storage of %x is not tracked", accountHash)
			}
			slotLock.RUnlock()

			for h := range slots {
				var found bool
				for j := 0; j < len(subset[h]); j++ {
					if subset[h][j] == state {
						if j == 0 {
							subset[h] = subset[h][1:]
						} else {
							subset[h] = append(subset[h][:j], subset[h][j+1:]...)
						}
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("failed to delete lookup in storage set %x %x", accountHash, h)
				}
				if len(subset[h]) == 0 {
					delete(subset, h)
				}
			}
			if len(subset) == 0 {
				slotLock.Lock()
				delete(l.storages, accountHash)
				slotLock.Unlock()
			}
			return nil
		})
	}
	return workers.Wait()
}

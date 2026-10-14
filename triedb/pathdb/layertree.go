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

package pathdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// layerTree is a group of state layers identified by the state root.
// This structure defines a few basic operations for manipulating
// state layers linked with each other in a tree structure. It's
// thread-safe to use. However, callers need to ensure the thread-safety
// of the referenced layer by themselves.
type layerTree struct {
	base   *diskLayer
	layers map[common.Hash]layer
	gate   *layerGate // filter over the keys of the live diff layers
	lock   sync.RWMutex
}

// newLayerTree constructs the layerTree with the given head layer.
func newLayerTree(head layer) *layerTree {
	tree := new(layerTree)
	tree.init(head)
	return tree
}

// init initializes the layerTree by the given head layer.
func (tree *layerTree) init(head layer) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	current := head
	tree.layers = make(map[common.Hash]layer)

	for {
		tree.layers[current.rootHash()] = current

		parent := current.parentLayer()
		if parent == nil {
			break
		}
		current = parent
	}
	tree.base = current.(*diskLayer) // panic if it's not a disk layer
	tree.rebuildGate(maxDiffLayers/layerGateWindow + 3)
}

// addGate inserts the keys of the given diff layer into the gate, which is
// regrown if the live layers span more windows than it covers.
//
// This function assumes the write lock has been held.
func (tree *layerTree) addGate(dl *diffLayer) {
	defer func(start time.Time) {
		layerGateAddTimer.UpdateSince(start)
	}(time.Now())

	if !tree.gate.add(dl.id, dl.keys, tree.base.id) {
		tree.rebuildGate(2 * len(tree.gate.gens))
	}
}

// rebuildGate reconstructs the gate with at least the given number of
// generations from the live diff layers.
//
// This function assumes the write lock has been held.
func (tree *layerTree) rebuildGate(gens int) {
	for {
		var (
			gate = newLayerGate(gens)
			ok   = true
		)
		for _, l := range tree.layers {
			if dl, isDiff := l.(*diffLayer); isDiff && !gate.add(dl.id, dl.keys, tree.base.id) {
				ok = false
				break
			}
		}
		if ok {
			tree.gate = gate
			return
		}
		gens *= 2
	}
}

// get retrieves a layer belonging to the given state root.
func (tree *layerTree) get(root common.Hash) layer {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	return tree.layers[root]
}

// forEach iterates the stored layers inside and applies the
// given callback on them.
func (tree *layerTree) forEach(onLayer func(layer)) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	for _, layer := range tree.layers {
		onLayer(layer)
	}
}

// len returns the number of layers cached.
func (tree *layerTree) len() int {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	return len(tree.layers)
}

// add inserts a new layer into the tree if it can be linked to an existing old parent.
func (tree *layerTree) add(root common.Hash, parentRoot common.Hash, block uint64, nodes *nodeSetWithOrigin, states *StateSetWithOrigin) error {
	// Reject noop updates to avoid self-loops. This is a special case that can
	// happen for clique networks and proof-of-stake networks where empty blocks
	// don't modify the state (0 block subsidy).
	//
	// Although we could silently ignore this internally, it should be the caller's
	// responsibility to avoid even attempting to insert such a layer.
	if root == parentRoot {
		return errors.New("layer cycle")
	}
	// If a layer with this root already exists, skip the insertion. Fork blocks
	// can produce the same state root as the canonical block (same parent, same
	// coinbase, zero txs); overwriting tree.layers[root] would corrupt the parent
	// chain for any child layers already built on top of the existing one.
	if tree.get(root) != nil {
		return nil
	}
	parent := tree.get(parentRoot)
	if parent == nil {
		return fmt.Errorf("triedb parent [%#x] layer missing", parentRoot)
	}
	l := parent.update(root, parent.stateID()+1, block, nodes, states)

	tree.lock.Lock()
	defer tree.lock.Unlock()

	// Link the given layer into the layer set and the gate
	tree.layers[l.rootHash()] = l
	tree.addGate(l)
	return nil
}

// cap traverses downwards the diff tree until the number of allowed diff layers
// are crossed. All diffs beyond the permitted number are flattened downwards.
func (tree *layerTree) cap(root common.Hash, layers int) error {
	// Retrieve the head layer to cap from
	l := tree.get(root)
	if l == nil {
		return fmt.Errorf("triedb layer [%#x] missing", root)
	}
	diff, ok := l.(*diffLayer)
	if !ok {
		return fmt.Errorf("triedb layer [%#x] is disk layer", root)
	}
	start := time.Now()
	tree.lock.Lock()
	defer tree.lock.Unlock()
	updateCapLockTimer.UpdateSince(start)

	// If full commit was requested, flatten the diffs and merge onto disk
	if layers == 0 {
		base, err := diff.persist(true)
		if err != nil {
			return err
		}
		tree.base = base

		// Reset the layer tree with the single new disk layer
		tree.layers = map[common.Hash]layer{
			base.rootHash(): base,
		}
		tree.rebuildGate(len(tree.gate.gens))
		return nil
	}
	// Dive until we run out of layers or reach the persistent database
	for i := 0; i < layers-1; i++ {
		// If we still have diff layers below, continue down
		if parent, ok := diff.parentLayer().(*diffLayer); ok {
			diff = parent
		} else {
			// Diff stack too shallow, return without modifications
			return nil
		}
	}
	// We're out of layers, flatten anything below, stopping if it's the disk or if
	// the memory limit is not yet exceeded.
	var (
		err     error
		newBase *diskLayer
	)
	switch parent := diff.parentLayer().(type) {
	case *diskLayer:
		return nil

	case *diffLayer:
		// Hold the lock to prevent any read operations until the new
		// parent is linked correctly.
		diff.lock.Lock()

		// Replace the original parent layer with new disk layer. The procedure
		// can be illustrated as below:
		//
		// Before change:
		//     Chain:
		//        C1->C2->C3->C4 (HEAD)
		//          ->C2'->C3'->C4'
		//
		// After change:
		//     Chain:
		//        (a) C3->C4 (HEAD)
		//		  (b) C1->C2
		//		        ->C2'->C3'->C4'
		// The original C3 is replaced by the new base (with root C3)
		// Dangling layers in (b) will be removed later
		persistStart := time.Now()
		newBase, err = parent.persist(false)
		if err != nil {
			diff.lock.Unlock()
			return err
		}
		updatePersistTimer.UpdateSince(persistStart)
		tree.layers[newBase.rootHash()] = newBase

		// Link the new parent and release the lock
		diff.parent = newBase
		diff.lock.Unlock()

	default:
		panic(fmt.Sprintf("unknown data layer in triedb: %T", parent))
	}
	// Remove any layer that is stale or links into a stale layer
	children := make(map[common.Hash][]common.Hash)
	for root, layer := range tree.layers {
		if dl, ok := layer.(*diffLayer); ok {
			parent := dl.parentLayer().rootHash()
			children[parent] = append(children[parent], root)
		}
	}
	// The keys of the removed diff layers are left in the gate, they expire
	// along with the window once the disk layer moves beyond it.
	var remove func(root common.Hash)
	remove = func(root common.Hash) {
		// Unlink the layer from the layer tree and cascade to its children
		delete(tree.layers, root)
		for _, child := range children[root] {
			remove(child)
		}
		delete(children, root)
	}
	remove(tree.base.rootHash()) // remove the old/stale disk layer
	tree.base = newBase          // update the base layer with newly constructed one
	return nil
}

// bottom returns the bottom-most disk layer in this tree.
func (tree *layerTree) bottom() *diskLayer {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	return tree.base
}

// layerGateDepth is the number of the topmost layers walked through before the
// gate is consulted.
const layerGateDepth = 4

// lookup returns the layer holding the entry with the given bloom key in the
// specified state, along with its distance from the state, or the disk layer if
// no diff layer holds it. has reports whether a diff layer holds the entry, it's
// consulted only for the layers whose filter fails to rule the key out.
func (tree *layerTree) lookup(state common.Hash, key uint64, has func(*diffLayer) bool) (layer, int, error) {
	// Hold the read lock to prevent the unexpected layer changes
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	l := tree.layers[state]
	if l == nil {
		return nil, 0, fmt.Errorf("[%#x] %w", state, errSnapshotStale)
	}
	// Walk down the layers, the parent links are stable with the lock held.
	// The gate is consulted once the topmost layers, holding the recently
	// modified entries, are passed; from then on, the layers in the windows
	// ruled out by the gate generations are skipped without probing.
	var (
		depth  int
		window = ^uint64(0) // the window of the layers being walked through
		maybe  = true       // whether the window possibly holds the key
	)
	for {
		dl, ok := l.(*diffLayer)
		if !ok {
			break
		}
		if depth >= layerGateDepth {
			if depth == layerGateDepth && !tree.gate.contains(key, tree.base.id) {
				layerGateShortMeter.Mark(1)
				return tree.base, int(l.stateID() - tree.base.id), nil
			}
			if w := (dl.id - 1) / layerGateWindow; w != window {
				window, maybe = w, tree.gate.windowContains(dl.id, key)
			}
		}
		if maybe && dl.bloom.contains(key) && has(dl) {
			break
		}
		l, depth = dl.parent, depth+1
	}
	layerGateWalkMeter.Mark(1)
	layerGateDepthMeter.Mark(int64(depth))
	return l, depth, nil
}

// lookupAccount returns the layer that is guaranteed to contain the account data
// corresponding to the specified state root being queried.
func (tree *layerTree) lookupAccount(accountHash common.Hash, state common.Hash) (layer, error) {
	l, _, err := tree.lookup(state, bloomAccountKey(accountHash), func(dl *diffLayer) bool {
		_, ok := dl.states.account(accountHash)
		return ok
	})
	return l, err
}

// lookupStorage returns the layer that is guaranteed to contain the storage slot
// data corresponding to the specified state root being queried.
func (tree *layerTree) lookupStorage(accountHash common.Hash, slotHash common.Hash, state common.Hash) (layer, error) {
	l, _, err := tree.lookup(state, bloomStorageKey(accountHash, slotHash), func(dl *diffLayer) bool {
		_, ok := dl.states.storage(accountHash, slotHash)
		return ok
	})
	return l, err
}

// lookupNode returns the layer that is guaranteed to contain the trie node
// corresponding to the specified state root being queried, along with its
// distance from the state.
func (tree *layerTree) lookupNode(owner common.Hash, path []byte, state common.Hash) (layer, int, error) {
	return tree.lookup(state, bloomNodeKey(owner, path), func(dl *diffLayer) bool {
		_, ok := dl.nodes.node(owner, path)
		return ok
	})
}

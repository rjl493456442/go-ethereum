// Copyright 2020 The go-ethereum Authors
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

package client

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/nodestate"
)

func TestWrsIterator(t *testing.T) {
	ns := nodestate.NewNodeStateMachine(nil, nil, &mclock.Simulated{}, testSetup)
	weights := make([]uint64, iterTestNodeCount+1)
	wfn := func(i interface{}) uint64 {
		id := i.(enode.ID)
		n := ns.GetNode(id)
		if n == nil {
			return 0
		}
		idx := testNodeIndex(id)
		if idx <= 0 || idx > len(weights) {
			t.Errorf("Invalid node id %v", id)
		}
		return weights[idx]
	}
	w := NewWrsIterator(ns, sfTest2, sfTest3, sfTest4, wfn)
	ns.Start()
	for i := 1; i <= iterTestNodeCount; i++ {
		weights[i] = 1
		ns.SetState(testNode(i), sfTest1, nodestate.Flags{}, 0)
	}
	ch := make(chan *enode.Node)
	go func() {
		for w.Next() {
			ch <- w.Node()
		}
		close(ch)
	}()
	next := func() int {
		select {
		case node := <-ch:
			return testNodeIndex(node.ID())
		case <-time.After(time.Millisecond * 200):
			return 0
		}
	}
	exp := func(i int) {
		n := next()
		if n != i {
			t.Errorf("Wrong item returned by iterator (expected %d, got %d)", i, n)
		}
	}
	set := make(map[int]bool)
	expset := func() {
		for len(set) > 0 {
			n := next()
			if !set[n] {
				t.Errorf("Item returned by iterator not in the expected set (got %d)", n)
			}
			delete(set, n)
		}
		exp(0)
	}

	exp(0)
	ns.SetState(testNode(1), sfTest2, nodestate.Flags{}, 0)
	ns.SetState(testNode(2), sfTest2, nodestate.Flags{}, 0)
	ns.SetState(testNode(3), sfTest2, nodestate.Flags{}, 0)
	set[1] = true
	set[2] = true
	set[3] = true
	expset()
	ns.SetState(testNode(4), sfTest2, nodestate.Flags{}, 0)
	ns.SetState(testNode(5), sfTest2.Or(sfTest3), nodestate.Flags{}, 0)
	ns.SetState(testNode(6), sfTest2, nodestate.Flags{}, 0)
	set[4] = true
	set[6] = true
	expset()
	weights[2] = 0
	ns.SetState(testNode(1), nodestate.Flags{}, sfTest4, 0)
	ns.SetState(testNode(2), nodestate.Flags{}, sfTest4, 0)
	ns.SetState(testNode(3), nodestate.Flags{}, sfTest4, 0)
	set[1] = true
	set[3] = true
	expset()
	weights[2] = 1
	ns.SetState(testNode(2), nodestate.Flags{}, sfTest2, 0)
	ns.SetState(testNode(1), nodestate.Flags{}, sfTest4, 0)
	ns.SetState(testNode(2), sfTest2, sfTest4, 0)
	ns.SetState(testNode(3), nodestate.Flags{}, sfTest4, 0)
	set[1] = true
	set[2] = true
	set[3] = true
	expset()
}

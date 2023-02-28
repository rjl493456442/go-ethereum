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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package types

import "github.com/ethereum/go-ethereum/common"

// Node is a wrapper which contains the encoded blob of the trie node and its
// unique hash identifier. It is general enough that can be used to represent
// trie nodes corresponding to different trie implementations.
type Node struct {
	Hash common.Hash // Node hash, empty for deleted node
	Blob []byte      // Encoded node blob, nil for the deleted node
}

// Size returns the total memory size used by this node.
func (n *Node) Size() int {
	return len(n.Blob) + common.HashLength
}

// IsDeleted returns the indicator if the node is marked as deleted.
func (n *Node) IsDeleted() bool {
	return n.Hash == (common.Hash{})
}

// NodeWithPrev wraps the Node with the previous node value attached.
type NodeWithPrev struct {
	*Node
	Prev []byte // Encoded original value, nil means it's non-existent
}

// Unwrap returns the internal Node object.
func (n *NodeWithPrev) Unwrap() *Node {
	return n.Node
}

// Size returns the total memory size used by this node. It overloads
// the function in Node by counting the size of previous value as well.
// nolint: unused
func (n *NodeWithPrev) Size() int {
	return n.Node.Size() + len(n.Prev)
}

// NewNode constructs a node with provided node information.
func NewNode(hash common.Hash, blob []byte) *Node {
	return &Node{Hash: hash, Blob: blob}
}

// NewDeletedNode constructs a deletion marker for deleted node.
func NewDeletedNode() *Node {
	return &Node{}
}

// NewNodeWithPrev constructs a node with provided node information.
func NewNodeWithPrev(hash common.Hash, blob []byte, prev []byte) *NodeWithPrev {
	return &NodeWithPrev{
		Node: NewNode(hash, blob),
		Prev: prev,
	}
}

// NewDeletedNodeWithPrev constructs a deletion marker for deleted node with
// provided original value.
func NewDeletedNodeWithPrev(prev []byte) *NodeWithPrev {
	return &NodeWithPrev{
		Node: NewDeletedNode(),
		Prev: prev,
	}
}

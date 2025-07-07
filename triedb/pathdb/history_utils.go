// Copyright 2025 The go-ethereum Authors
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

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// onHeadTruncation defines a callback function that is invoked when entries are
// truncated from the head of ancient store.
// 'head' refers to the number of items in the ancient store before truncation;
// 'tail' refers to the number of first stored item in the ancient store;

//nolint:unused
type onHeadTruncation = func(head uint64, tail uint64) error

// onTailTruncation defines a callback function that is invoked when entries are
// truncated from the tail of ancient store.
// 'head' refers to the number of items in the ancient store before truncation;
// 'tail' refers to the number of first stored item in the ancient store;

//nolint:unused
type onTailTruncation = func(head uint64, tail uint64) error

// truncateFromHead removes excess elements from the head of the freezer based
// on the given parameters. It returns the number of items that were removed.
func truncateFromHead(store ethdb.AncientStore, name string, nhead uint64) (int, error) {
	ohead, err := store.Ancients()
	if err != nil {
		return 0, err
	}
	otail, err := store.Tail()
	if err != nil {
		return 0, err
	}
	log.Info("Truncating from head", "ohead", ohead, "tail", otail, "nhead", nhead)

	// Ensure that the truncation target falls within the valid range.
	if ohead < nhead || nhead < otail {
		return 0, fmt.Errorf("truncate head: %s, out of range, tail: %d, head: %d, target: %d", name, otail, ohead, nhead)
	}
	// Short circuit if nothing to truncate.
	if ohead == nhead {
		return 0, nil
	}
	ohead, err = store.TruncateHead(nhead)
	if err != nil {
		return 0, err
	}
	return int(ohead - nhead), nil
}

// truncateFromTail removes excess elements from the end of the freezer based
// on the given parameters. It returns the number of items that were removed.
func truncateFromTail(store ethdb.AncientStore, name string, ntail uint64) (int, error) {
	ohead, err := store.Ancients()
	if err != nil {
		return 0, err
	}
	otail, err := store.Tail()
	if err != nil {
		return 0, err
	}
	// Ensure that the truncation target falls within the valid range.
	if otail > ntail || ntail > ohead {
		return 0, fmt.Errorf("truncate tail: %s, out of range, tail: %d, head: %d, target: %d", name, otail, ohead, ntail)
	}
	// Short circuit if nothing to truncate.
	if otail == ntail {
		return 0, nil
	}
	otail, err = store.TruncateTail(ntail)
	if err != nil {
		return 0, err
	}
	return int(ntail - otail), nil
}

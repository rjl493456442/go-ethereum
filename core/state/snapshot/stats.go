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

package snapshot

import (
	"sync"
	"time"
)

const (
	locDisk = iota
	locClean
	locDiff
)

type stats struct {
	diskHits  int
	cleanHits int
	diffHits  int

	diskTime time.Duration

	misses int
	finds  int
	lock   sync.Mutex
}

func (s *stats) copy() *stats {
	s.lock.Lock()
	defer s.lock.Unlock()

	cpy := &stats{
		diskHits:  s.diskHits,
		cleanHits: s.cleanHits,
		diffHits:  s.diffHits,
		diskTime:  s.diskTime,
		misses:    s.misses,
		finds:     s.finds,
	}
	return cpy
}

func (s *stats) update(loc int, duration time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch loc {
	case locDisk:
		s.diskHits++
		s.diskTime += duration
	case locClean:
		s.cleanHits++
	case locDiff:
		s.diffHits++
	}
}

func (s *stats) find() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.finds++
}

func (s *stats) miss() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.misses++
}

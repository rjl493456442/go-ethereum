// Copyright 2026 The go-ethereum Authors
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

package snap

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// Profile buckets, one per response type.
const (
	profAccount = iota
	profBytecode
	profStorage
	profTrienodeHeal
	profBytecodeHeal
	profKinds
)

var profKindNames = [profKinds]string{
	"account",
	"bytecode",
	"storage",
	"trienodeheal",
	"bytecodeheal",
}

// profStat aggregates wall-clock samples of one instrumentation point. The
// observers may run on concurrent peer threads.
type profStat struct {
	cnt atomic.Int64
	sum atomic.Int64 // nanoseconds
	max atomic.Int64 // nanoseconds
}

func (p *profStat) observe(d time.Duration) {
	p.cnt.Add(1)
	p.sum.Add(int64(d))

	for {
		old := p.max.Load()
		if int64(d) <= old || p.max.CompareAndSwap(old, int64(d)) {
			return
		}
	}
}

// String renders the aggregate as count/total/mean/max.
func (p *profStat) String() string {
	cnt, sum, max := p.cnt.Load(), p.sum.Load(), p.max.Load()
	if cnt == 0 {
		return "-"
	}
	return fmt.Sprintf("n=%d tot=%v avg=%v max=%v", cnt, common.PrettyDuration(sum), common.PrettyDuration(sum/cnt), common.PrettyDuration(max))
}

// syncProfile aggregates wall-clock statistics about the interplay between
// the concurrent peer threads and the single-threaded sync runloop, to
// quantify how much the loop's serialization costs overall sync throughput.
//
// The peer threads verify responses concurrently, but hand every verified
// response to the runloop over an unbuffered channel: deliver measures how
// long they block on that handover. The runloop's own time splits into idle
// (waiting in select), schedule (task cleanup/assignment at the loop top)
// and process (handling one response); commit is the batch-write share of
// process.
type syncProfile struct {
	idle     profStat // runloop blocked in select, waiting for events
	schedule profStat // runloop top-of-loop cleanup/assignment work

	deliver    [profKinds]profStat // peer threads blocked handing a verified response to the runloop
	process    [profKinds]profStat // runloop handling one response (including persistence)
	commit     [profKinds]profStat // batch writes within the response handlers
	healCommit profStat            // healer scheduler commits (shared by both heal kinds)
	exec       profStat            // storage job execution on the worker pool
}

// reportProfile dumps the accumulated statistics.
func (s *syncer) reportProfile() {
	log.Info("Sync loop profile", "idle", s.prof.idle.String(), "schedule", s.prof.schedule.String(), "storageexec", s.prof.exec.String(), "healcommit", s.prof.healCommit.String())
	for i, name := range profKindNames {
		log.Info("Sync loop profile: "+name, "deliverwait", s.prof.deliver[i].String(), "process", s.prof.process[i].String(), "commit", s.prof.commit[i].String())
	}
}

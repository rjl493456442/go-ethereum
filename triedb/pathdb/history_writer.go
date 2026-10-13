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

package pathdb

import (
	"errors"
	"sync"
)

// errHistoryWriterClosed is returned if a history write is scheduled after the
// writer is closed.
var errHistoryWriterClosed = errors.New("history writer closed")

// historyWriter persists the histories of the committed layers in background,
// strictly in the commit order, taking the history construction, the freezer
// write and the indexing off the block commit path.
//
// The written histories are not synced; the callers relying on them (the buffer
// flush, the journal, the rollback and the historical access) must wait for the
// scheduled writes first.
type historyWriter struct {
	tasks   chan func() error
	lock    sync.Mutex
	cond    *sync.Cond
	pending int   // the number of the scheduled tasks not yet done
	err     error // the error of the first failed task
	closed  bool  // whether the writer is closed
}

// newHistoryWriter constructs the writer and starts its background thread.
func newHistoryWriter() *historyWriter {
	w := &historyWriter{
		tasks: make(chan func() error, 128),
	}
	w.cond = sync.NewCond(&w.lock)
	go w.loop()
	return w
}

// loop runs the scheduled tasks in order until the writer is closed.
func (w *historyWriter) loop() {
	for task := range w.tasks {
		err := task()

		w.lock.Lock()
		if err != nil && w.err == nil {
			w.err = err
		}
		w.pending -= 1
		w.cond.Broadcast()
		w.lock.Unlock()
	}
}

// submit schedules the task, returning the error of any failed task before
// instead, which leaves the histories in an unrecoverable state.
func (w *historyWriter) submit(task func() error) error {
	w.lock.Lock()
	if w.err != nil {
		w.lock.Unlock()
		return w.err
	}
	if w.closed {
		w.lock.Unlock()
		return errHistoryWriterClosed
	}
	w.pending += 1
	w.lock.Unlock()

	w.tasks <- task
	return nil
}

// wait blocks until all the scheduled tasks are done, returning the error of
// the first failed task if any.
func (w *historyWriter) wait() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	for w.pending > 0 {
		w.cond.Wait()
	}
	return w.err
}

// close waits for the scheduled tasks and terminates the background thread.
// No task can be scheduled afterwards. It must not run concurrently with submit,
// both are serialized by the database lock.
func (w *historyWriter) close() error {
	err := w.wait()

	w.lock.Lock()
	defer w.lock.Unlock()
	if !w.closed {
		w.closed = true
		close(w.tasks)
	}
	return err
}

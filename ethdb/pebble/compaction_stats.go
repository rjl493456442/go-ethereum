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

package pebble

import (
	"fmt"
	"io"
	"time"
)

// WriteCompactionStats writes this database's compaction breakdown to w,
// covering everything it has done since it was opened. elapsed is the span the
// rates are computed against; pass the duration of the work being measured.
//
// It is reached through an interface assertion by callers that only hold an
// ethdb.Database, so that they need not import this package to ask for the
// breakdown, and get nothing if the store underneath is not pebble. The writer
// is the caller's choice because those callers differ in where their output is
// expected to land: a command prints to stdout, a running node to the stream
// its logs already go to.
func (d *Database) WriteCompactionStats(w io.Writer, elapsed time.Duration) {
	WriteCompactionStats(w, d.CompactionStats(), elapsed)
}

// WriteCompactionStats breaks a run's write amplification down by stage. The
// counters are cumulative over the database's lifetime, so they describe the
// run exactly when the database was opened for it.
//
// The report is kept to printable ASCII and, in particular, free of equals
// signs. A caller may hand the result to geth's logger as one multi-line
// message, and its terminal handler passes newlines through only for messages
// that need no quoting; a single '=' anywhere would collapse the whole table
// onto one escaped line.
func WriteCompactionStats(w io.Writer, cs CompactionStats, elapsed time.Duration) {
	if cs.WALBytes == 0 {
		fmt.Fprintf(w, "\nno writes recorded\n")
		return
	}
	var (
		wal  = float64(cs.WALBytes)
		gb   = func(v int64) float64 { return float64(v) / (1 << 30) }
		secs = max(elapsed.Seconds(), 1)
	)
	fmt.Fprintf(w, "\n--- write amplification ---\n")
	fmt.Fprintf(w, "accepted:  %.2f GB (%.0f MB/s)\n", gb(int64(cs.WALBytes)), float64(cs.WALBytes)/(1<<20)/secs)
	fmt.Fprintf(w, "written:   %.2f GB (%.0f MB/s)\n", gb(int64(cs.DiskBytes)), float64(cs.DiskBytes)/(1<<20)/secs)
	fmt.Fprintf(w, "read back: %.2f GB (%.0f MB/s)\n", gb(int64(cs.ReadBytes)), float64(cs.ReadBytes)/(1<<20)/secs)
	fmt.Fprintf(w, "write amp: %.3f bytes written per byte accepted\n\n", cs.WriteAmp())

	// Every stage is that level's own written-bytes counter, so the column sums
	// to the total rather than leaving a residual.
	fmt.Fprintf(w, "  %-14s%10s%10s\n", "stage", "GB", "per byte")
	for _, st := range []struct {
		name  string
		bytes uint64
	}{
		{"WAL", cs.WALBytes},
		{"flush -> L0", cs.FlushedBytes},
		{"intra-L0", cs.IntraL0WrittenBytes},
		{"L0 -> Lbase", cs.LbaseWrittenBytes},
		{"Lbase -> L6", cs.CascadeWrittenBytes},
	} {
		if st.bytes == 0 && st.name == "intra-L0" {
			continue
		}
		fmt.Fprintf(w, "  %-14s%10.2f%10.3f\n", st.name, gb(int64(st.bytes)), float64(st.bytes)/wal)
	}
	fmt.Fprintf(w, "  %-14s%10.2f%10.3f\n", "total", gb(int64(cs.DiskBytes)), cs.WriteAmp())

	// Per level, so the cascade above is not just a residual.
	fmt.Fprintf(w, "\n  %-6s%8s%10s%12s%12s%12s\n", "level", "files", "size GB", "in GB", "read GB", "moved GB")
	for _, l := range cs.Levels {
		name := fmt.Sprintf("L%d", l.Level)
		if l.Level == 0 {
			name = fmt.Sprintf("L0/%d", l.Sublevels)
		} else if l.Level == cs.BaseLevel {
			name += "*"
		}
		fmt.Fprintf(w, "  %-6s%8d%10.2f%12.2f%12.2f%12.2f\n", name, l.Files, gb(l.Size),
			gb(int64(l.BytesFlushed+l.BytesCompacted)), gb(int64(l.BytesRead)), gb(int64(l.BytesMoved)))
	}
	fmt.Fprintf(w, "  (* marks the base level, the one L0 drains into; L0/n shows the sublevel count)\n")

	// L0 by sublevel. Files in one sublevel never overlap, so a sublevel with
	// many files spanning most of the key range is a tiled layer, while one with
	// few files over a small span is a narrow column.
	if len(cs.Sublevels) > 0 {
		fmt.Fprintf(w, "\n  %-10s%8s%10s%10s%12s\n", "L0", "files", "size GB", "span", "avg file MB")
		for i := len(cs.Sublevels) - 1; i >= 0; i-- {
			sl := cs.Sublevels[i]
			avg := 0.0
			if sl.Files > 0 {
				avg = float64(sl.Size) / float64(sl.Files) / (1 << 20)
			}
			fmt.Fprintf(w, "  %-10s%8d%10.2f%9.0f%%%12.1f\n",
				fmt.Sprintf("sub %d", sl.Sublevel), sl.Files, gb(sl.Size), 100*sl.Span, avg)
		}
		fmt.Fprintf(w, "  (span is the share of L0's key range covered; ~100%% means the sublevel tiles it)\n")
	}

	fmt.Fprintf(w, "\n  flushes: %d", cs.FlushCount)
	if cs.FlushCount > 0 {
		fmt.Fprintf(w, " (%.0f MB each)", float64(cs.FlushedBytes)/float64(cs.FlushCount)/(1<<20))
	}
	fmt.Fprintf(w, "\n  compactions: %d total, %d move-only, %d multi-level\n",
		cs.Compactions, cs.MoveCompactions, cs.MultiLevelCompactions)
	if cs.L0Drains > 0 {
		mb := func(v uint64) float64 { return float64(v) / float64(cs.L0Drains) / (1 << 20) }
		fmt.Fprintf(w, "    L0->Lbase: %d, %.2f sublevels each\n",
			cs.L0Drains, float64(cs.L0DrainSublevels)/float64(cs.L0Drains))
		fmt.Fprintf(w, "      inputs:  %.1f MB of L0 plus %.1f MB of Lbase, %.1f MB in all\n",
			mb(cs.L0DrainBytes), mb(cs.LbaseDrainBytes), mb(cs.L0DrainBytes+cs.LbaseDrainBytes))
		fmt.Fprintf(w, "      write amp %.3f, ie %.1f bytes of L0 pushed down per byte of Lbase disturbed\n",
			cs.DrainWriteAmp(), cs.DrainAmortisation())
		fmt.Fprintf(w, "      totals:  %.2f GB of L0 + %.2f GB of Lbase\n",
			gb(int64(cs.L0DrainBytes)), gb(int64(cs.LbaseDrainBytes)))
	}
	if cs.L0MoveCount > 0 {
		fmt.Fprintf(w, "    L0 moves:  %d, %.2f GB relinked into Lbase without writing\n",
			cs.L0MoveCount, gb(int64(cs.L0MoveBytes)))
	}
	if cs.IntraL0Count > 0 {
		fmt.Fprintf(w, "    intra-L0:  %d, %.2f GB rewritten (%.3f per byte accepted, pure overhead)\n",
			cs.IntraL0Count, gb(int64(cs.IntraL0Bytes)), float64(cs.IntraL0Bytes)/wal)
	}
	if cs.FailedCompactions > 0 || cs.CancelledBytes > 0 {
		fmt.Fprintf(w, "    %d failed, %.2f GB cancelled\n", cs.FailedCompactions, gb(cs.CancelledBytes))
	}
	fmt.Fprintf(w, "  compaction time: %s (%.0f%% of wall clock, concurrency %.1fx)\n",
		cs.CompactionDuration.Round(time.Second),
		100*cs.CompactionDuration.Seconds()/secs, cs.CompactionDuration.Seconds()/secs)

	fmt.Fprintf(w, "\n  peak L0 sublevels: %d\n", cs.PeakSublevels)
	fmt.Fprintf(w, "  write stalls: %d (%s)\n", cs.StallCount, cs.StallDuration.Round(time.Millisecond))
	if h, m := cs.BlockCacheHits, cs.BlockCacheMisses; h+m > 0 {
		fmt.Fprintf(w, "  block cache: %.1f%% hit", 100*float64(h)/float64(h+m))
		if fh, fm := cs.FilterHits, cs.FilterMisses; fh+fm > 0 {
			fmt.Fprintf(w, ", bloom filter: %.1f%% useful", 100*float64(fm)/float64(fh+fm))
		}
		fmt.Fprintln(w)
	}
}

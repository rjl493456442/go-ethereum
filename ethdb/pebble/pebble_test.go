// Copyright 2019 The go-ethereum Authors
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
	"errors"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	pebblev1 "github.com/cockroachdb/pebble"
	pebblev2 "github.com/cockroachdb/pebble/v2"
	vfsv2 "github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/vfs"
	vfsv1 "github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestPebbleDB(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			db, err := pebblev2.Open("", &pebblev2.Options{
				FS: vfsv2.NewMem(),
			})
			if err != nil {
				t.Fatal(err)
			}
			return &Database{
				db: db,
			}
		})
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			db, err := pebblev1.Open("", &pebblev1.Options{
				FS: vfsv1.NewMem(),
			})
			if err != nil {
				t.Fatal(err)
			}
			return &V1Database{
				db: db,
			}
		})
	})
}

func TestReverseIterator(t *testing.T) {
	db, err := pebblev2.Open("", &pebblev2.Options{FS: vfsv2.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &Database{db: db}
	keys := []string{"o", "p", "p\x00", "pa", "pa\x00", "p\xff", "p\xff\xff", "q", "\xff\xff", "\xff\xff\xff"}
	for _, k := range keys {
		if err := store.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, prefix string
		start        []byte
		want         []string
	}{
		{"prefix", "p", nil, []string{"p\xff\xff", "p\xff", "pa\x00", "pa", "p\x00", "p"}},
		{"exact", "p", []byte("a"), []string{"pa", "p\x00", "p"}},
		{"missing", "p", []byte("b"), []string{"pa\x00", "pa", "p\x00", "p"}},
		{"empty suffix", "p", []byte{}, []string{"p"}},
		{"trailing ff", "p", []byte{0xff}, []string{"p\xff", "pa\x00", "pa", "p\x00", "p"}},
		{"unbounded prefix", "\xff\xff", nil, []string{"\xff\xff\xff", "\xff\xff"}},
		{"absent prefix", "x", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it := store.NewReverseIterator([]byte(tc.prefix), tc.start)
			var got []string
			for it.Next() {
				if string(it.Key()) != string(it.Value()) {
					t.Fatalf("key %x has value %x", it.Key(), it.Value())
				}
				got = append(got, string(it.Key()))
				if len(got) == 1 {
					// Resume inclusively after releasing the old read state.
					start := []byte(got[0][len(tc.prefix):])
					it.Release()
					it = store.NewReverseIterator([]byte(tc.prefix), start)
					if !it.Next() || string(it.Key()) != got[0] {
						t.Fatalf("resume at %x returned %x (error %v)", got[0], it.Key(), it.Error())
					}
				}
			}
			err := it.Error()
			it.Release()
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("got %x (error %v), want %x", got, err, tc.want)
			}
		})
	}
}

func BenchmarkPebbleDB(b *testing.B) {
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		db, err := pebblev2.Open("", &pebblev2.Options{
			FS: vfsv2.NewMem(),
		})
		if err != nil {
			b.Fatal(err)
		}
		return &Database{
			db: db,
		}
	})
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		db, err := pebblev1.Open("", &pebblev1.Options{
			FS: vfsv1.NewMem(),
		})
		if err != nil {
			b.Fatal(err)
		}
		return &V1Database{
			db: db,
		}
	})
}

func TestPebbleLogDataV1(t *testing.T) {
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = db.Get(nil)
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal("Unknown database entry")
	}

	b := db.NewBatch()
	b.LogData(nil, nil)
	db.Apply(b, pebble.Sync)

	_, _, err = db.Get(nil)
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal("Unknown database entry")
	}
}

func TestPebbleLogDataV2(t *testing.T) {
	db, err := pebblev2.Open("", &pebblev2.Options{
		FS: vfsv2.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = db.Get(nil)
	if !errors.Is(err, pebblev2.ErrNotFound) {
		t.Fatal("Unknown database entry")
	}

	b := db.NewBatch()
	b.LogData(nil, nil)
	db.Apply(b, pebblev2.Sync)

	_, _, err = db.Get(nil)
	if !errors.Is(err, pebblev2.ErrNotFound) {
		t.Fatal("Unknown database entry")
	}
}

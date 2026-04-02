// tests for verifying that the write-ahead log works to specificiations
// ensures that PUT and DELETE operations are on disk and are replayed when server crashes

package wal

import (
	"path/filepath"
	"testing"

	"database_store/internal/store"
)

func TestOpenReplayEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.db")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var n int
	err = w.Replay(func(e Entry) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("replay on empty WAL: got %d entries, want 0", n)
	}
}

func TestAppendReplayOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.db")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	entries := []Entry{
		{Op: OpPut, Key: "a", Value: "1"},
		{Op: OpPut, Key: "b", Value: "2"},
		{Op: OpDelete, Key: "a"},
		{Op: OpPut, Key: "c", Value: "three"},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	var got []Entry
	err = w.Replay(func(e Entry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("len(got)=%d want %d", len(got), len(entries))
	}
	for i := range entries {
		if got[i] != entries[i] {
			t.Fatalf("entry %d: got %+v want %+v", i, got[i], entries[i])
		}
	}
}

func TestReplayRebuildsStoreLikeMain(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.db")

	{
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range []Entry{
			{Op: OpPut, Key: "k1", Value: "v1"},
			{Op: OpPut, Key: "k2", Value: "v2"},
			{Op: OpDelete, Key: "k1"},
		} {
			if err := w.Append(e); err != nil {
				t.Fatal(err)
			}
		}
		w.Close()
	}

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	s := store.New()
	err = w.Replay(func(e Entry) error {
		switch e.Op {
		case OpPut:
			s.Put(e.Key, e.Value)
		case OpDelete:
			s.Delete(e.Key)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Get("k1"); ok {
		t.Fatal("k1 should be deleted")
	}
	v, ok := s.Get("k2")
	if !ok || v != "v2" {
		t.Fatalf("k2: got %q, %v; want v2, true", v, ok)
	}
}

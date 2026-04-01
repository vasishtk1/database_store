// Package wal implements the Write-Ahead Log (WAL).
//
// The WAL's job is durability: before we apply any mutation to the
// in-memory store, we write it to disk first. If the process crashes
// between the write and the apply, we can replay the WAL on restart
// and end up in exactly the state we had before the crash.
//
// We use BoltDB (bbolt) as the underlying storage engine because it
// gives us ACID transactions and simple key-value storage on top of
// a single file — no external database process needed.
//
// In Week 2, this WAL will be superseded by the Raft log, which acts
// as the distributed equivalent. For now it gives us single-node durability.
package wal

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Op is the type of a WAL operation. Only mutations are logged —
// reads don't change state so they don't need to be replayed.
type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DELETE"
)

// Entry is one record in the WAL. It captures everything needed to
// re-apply the operation on a fresh in-memory store.
type Entry struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // omitted for DELETE entries
}

// walBucket is the name of the BoltDB bucket that holds WAL entries.
// A BoltDB bucket is like a namespace — we only need one here.
var walBucket = []byte("wal")

// WAL wraps a BoltDB database and exposes append-only writes + full replay.
type WAL struct {
	db *bolt.DB
}

// Open opens the WAL file at path (creating it if it doesn't exist).
// Always call Close() when done.
func Open(path string) (*WAL, error) {
	// bolt.Open is safe to call on an existing file — it just opens it.
	// 0600 means the file is readable/writable only by the current user.
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open wal db: %w", err)
	}

	// BoltDB requires buckets to exist before writing into them.
	// db.Update runs a read-write transaction.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(walBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create wal bucket: %w", err)
	}

	return &WAL{db: db}, nil
}

// Close flushes all pending writes and closes the BoltDB file.
func (w *WAL) Close() error {
	return w.db.Close()
}

// Append persists one Entry to disk inside a BoltDB transaction.
//
// IMPORTANT: call Append BEFORE applying the operation to the in-memory
// store. This ordering guarantees that a crash can never produce a state
// where data is in memory but not on disk — only the reverse, which
// is safe because the WAL replay will re-apply the missing entry.
func (w *WAL) Append(e Entry) error {
	return w.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucket)

		// NextSequence returns a monotonically increasing uint64.
		// Using it as the key ensures entries are stored and iterated
		// in the exact order they were appended.
		seq, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("next sequence: %w", err)
		}

		// Encode the entry as JSON. Simple and human-readable for now;
		// we could switch to protobuf later for compactness.
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}

		// Store with an 8-byte big-endian key so BoltDB's byte-sorted
		// iteration naturally replays entries in insertion order.
		return b.Put(uint64ToBytes(seq), data)
	})
}

// Replay iterates over every WAL entry in insertion order and calls fn
// for each one. Use this at startup to rebuild the in-memory store.
//
// If fn returns an error, Replay stops immediately and returns that error.
func (w *WAL) Replay(fn func(Entry) error) error {
	// db.View is a read-only transaction — safe to run concurrently with reads.
	return w.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucket)

		// ForEach visits every key-value pair in byte-sorted order,
		// which matches our insertion order because of the uint64 keys.
		return b.ForEach(func(k, v []byte) error {
			var e Entry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("unmarshal wal entry: %w", err)
			}
			return fn(e)
		})
	})
}

// uint64ToBytes encodes n as an 8-byte big-endian slice.
// Big-endian is required so BoltDB's lexicographic sort matches numeric order.
func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(n >> 56)
	b[1] = byte(n >> 48)
	b[2] = byte(n >> 40)
	b[3] = byte(n >> 32)
	b[4] = byte(n >> 24)
	b[5] = byte(n >> 16)
	b[6] = byte(n >> 8)
	b[7] = byte(n)
	return b
}

// persister.go handles writing and reading the three pieces of Raft state
// that MUST survive a crash.
//
// The Raft paper calls these "stable storage". If any of these are lost on
// restart, the node could violate safety (e.g. vote for two different
// candidates in the same term, or forget committed log entries).
//
// We reuse BoltDB (bbolt) — the same library used for the WAL in Week 1 —
// because it gives us atomic disk writes with a single dependency.
//
// The three durable fields are:
//
//	currentTerm — the highest election term this node has ever seen
//	votedFor    — which candidate we voted for in currentTerm ("" = nobody)
//	log         — the full Raft log including every entry ever appended
//
// Everything else (commitIndex, lastApplied, role, nextIndex, matchIndex)
// is volatile: it is safe to lose on a crash and will be reconstructed
// from the durable state on restart.
package raft

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// persistBucket is the BoltDB bucket name where we store the Raft state.
// A bucket is like a namespace inside the BoltDB file.
var persistBucket = []byte("raft_state")

// stateKey is the single key inside the bucket that holds the full state blob.
// We store everything as one JSON document for simplicity.
var stateKey = []byte("state")

// durableState is the struct we serialize to disk. Exported fields are
// required for encoding/json to pick them up.
type durableState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// Persister wraps a BoltDB database and exposes save / load operations
// for Raft's durable state.
type Persister struct {
	db *bolt.DB
}

// openPersister opens (or creates) the BoltDB file at path and ensures
// the raft_state bucket exists. Returns a ready-to-use Persister.
func openPersister(path string) (*Persister, error) {
	// bolt.Open creates the file if it doesn't exist.
	// 0600 = owner can read/write, nobody else can.
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db at %s: %w", path, err)
	}

	// db.Update runs a read-write transaction. We use it to create the
	// bucket if this is the first time we've opened the file.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(persistBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create raft bucket: %w", err)
	}

	return &Persister{db: db}, nil
}

// save atomically writes currentTerm, votedFor, and the log to disk.
//
// IMPORTANT: call save() BEFORE responding to any RPC or sending any RPC
// that depends on these values. This mirrors the WAL-first discipline
// from Week 1: disk is always at least as up-to-date as memory.
//
// Must be called with the Node's mutex held (caller's responsibility).
func (p *Persister) save(term int, votedFor string, log []LogEntry) error {
	state := durableState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal raft state: %w", err)
	}

	// db.Update is an atomic read-write transaction: either the whole
	// write succeeds and is flushed to disk, or nothing changes.
	return p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(persistBucket)
		return b.Put(stateKey, data)
	})
}

// load reads the previously saved state from disk.
// On a brand-new node (no file yet) it returns zero values: term=0,
// votedFor="", log=nil. The caller (node.go) handles the nil log case.
func (p *Persister) load() (term int, votedFor string, log []LogEntry, err error) {
	var data []byte

	// db.View is a read-only transaction.
	err = p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(persistBucket)
		v := b.Get(stateKey)
		if v != nil {
			// BoltDB memory is only valid inside the transaction, so copy it.
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	if err != nil {
		return 0, "", nil, fmt.Errorf("bolt view: %w", err)
	}

	// No data yet — this is a fresh node.
	if data == nil {
		return 0, "", nil, nil
	}

	var state durableState
	if err = json.Unmarshal(data, &state); err != nil {
		return 0, "", nil, fmt.Errorf("unmarshal raft state: %w", err)
	}

	return state.CurrentTerm, state.VotedFor, state.Log, nil
}

// close flushes and closes the underlying BoltDB file.
func (p *Persister) close() error {
	return p.db.Close()
}

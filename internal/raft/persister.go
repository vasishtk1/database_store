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

// persistBucket stores the three durable Raft fields (term, votedFor, log).
var persistBucket = []byte("raft_state")

// snapshotBucket stores the latest snapshot (KV data + metadata).
// Using a separate bucket keeps snapshot data isolated from the Raft state
// and makes it easy to load/replace without touching the rest.
var snapshotBucket = []byte("raft_snapshot")

// stateKey is the single key inside persistBucket.
var stateKey = []byte("state")

// snapDataKey holds the raw KV map bytes.
var snapDataKey = []byte("snap_data")

// snapMetaKey holds the snapshot metadata (index + term) as JSON.
var snapMetaKey = []byte("snap_meta")

// durableState is the struct we serialize to disk. Exported fields are
// required for encoding/json to pick them up.
type durableState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
	// CommitIndex is the highest log index known committed (majority replicated).
	// Persisted so after a crash we can re-apply committed entries to the state machine
	// before any new RPCs arrive.
	CommitIndex int `json:"commit_index"`
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
		if _, err := tx.CreateBucketIfNotExists(persistBucket); err != nil {
			return err
		}
		// Also create the snapshot bucket so it's always available.
		_, err := tx.CreateBucketIfNotExists(snapshotBucket)
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
func (p *Persister) save(term int, votedFor string, log []LogEntry, commitIndex int) error {
	state := durableState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
		CommitIndex: commitIndex,
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
// votedFor="", log=nil, commitIndex=0. The caller (node.go) handles the nil log case.
func (p *Persister) load() (term int, votedFor string, log []LogEntry, commitIndex int, err error) {
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
		return 0, "", nil, 0, fmt.Errorf("bolt view: %w", err)
	}

	// No data yet — this is a fresh node.
	if data == nil {
		return 0, "", nil, 0, nil
	}

	var state durableState
	if err = json.Unmarshal(data, &state); err != nil {
		return 0, "", nil, 0, fmt.Errorf("unmarshal raft state: %w", err)
	}

	return state.CurrentTerm, state.VotedFor, state.Log, state.CommitIndex, nil
}

// ── Snapshot persistence ──────────────────────────────────────────────────────

// snapshotMeta is the small metadata record stored alongside the raw snapshot data.
type snapshotMeta struct {
	LastIncludedIndex int `json:"last_included_index"`
	LastIncludedTerm  int `json:"last_included_term"`
}

// saveSnapshot writes the full snapshot to BoltDB atomically.
// data    — serialised KV store state (JSON map[string]string)
// index   — the last log index covered by this snapshot (lastIncludedIndex)
// term    — the term of that log entry (lastIncludedTerm)
//
// Both data and metadata are written in a single transaction so there is
// never a state where one exists without the other.
func (p *Persister) saveSnapshot(data []byte, index, term int) error {
	meta := snapshotMeta{LastIncludedIndex: index, LastIncludedTerm: term}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal snapshot meta: %w", err)
	}

	return p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)
		if err := b.Put(snapMetaKey, metaBytes); err != nil {
			return fmt.Errorf("write snap meta: %w", err)
		}
		if err := b.Put(snapDataKey, data); err != nil {
			return fmt.Errorf("write snap data: %w", err)
		}
		return nil
	})
}

// loadSnapshot reads the most recently saved snapshot from BoltDB.
// Returns (nil, 0, 0, nil) if no snapshot has been saved yet — this is
// the normal case on a fresh node and is NOT an error.
func (p *Persister) loadSnapshot() (data []byte, index, term int, err error) {
	var metaBytes, snapData []byte

	err = p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)

		// BoltDB memory is only valid inside the transaction — copy both.
		v := b.Get(snapMetaKey)
		if v != nil {
			metaBytes = make([]byte, len(v))
			copy(metaBytes, v)
		}

		v = b.Get(snapDataKey)
		if v != nil {
			snapData = make([]byte, len(v))
			copy(snapData, v)
		}
		return nil
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("bolt view snapshot: %w", err)
	}

	// No snapshot saved yet — fresh node.
	if metaBytes == nil {
		return nil, 0, 0, nil
	}

	var meta snapshotMeta
	if err = json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, 0, 0, fmt.Errorf("unmarshal snapshot meta: %w", err)
	}

	return snapData, meta.LastIncludedIndex, meta.LastIncludedTerm, nil
}

// close flushes and closes the underlying BoltDB file.
func (p *Persister) close() error {
	return p.db.Close()
}

// Package raft implements the Raft consensus algorithm from scratch.
//
// Raft lets a cluster of nodes agree on a shared sequence of commands even
// when some nodes crash or messages are lost. The main guarantee: as long as
// a majority of nodes are alive, the cluster makes progress and never loses
// committed data.
//
// This file defines the two fundamental data types that flow through the
// whole algorithm: LogEntry (a command waiting to be agreed on) and
// ApplyMsg (a command that HAS been agreed on and is ready to run).
package raft

// LogEntry is one record in the Raft log.
//
// Every write command (PUT, DELETE) that a client sends gets wrapped in a
// LogEntry before Raft touches it. The entry lives in the log — an ordered,
// append-only list — until a majority of nodes have it. Only then does it
// get "committed" and applied to the in-memory key-value store.
//
// Index + Term together act like a fingerprint: no two entries across the
// whole cluster will ever share the same Index AND the same Term. This
// property is what lets Raft detect and fix log inconsistencies.
type LogEntry struct {
	// Index is the position of this entry in the log, starting at 1.
	// Index 0 is reserved for the sentinel (a dummy entry that simplifies
	// boundary conditions — see the comment in node.go).
	Index int `json:"index"`

	// Term is the leader-election epoch in which this entry was created.
	// If a new leader is elected, Term increments. Entries from an old
	// term can be "overwritten" by a newer leader if they were never committed.
	Term int `json:"term"`

	// Op is the operation type: "PUT" or "DELETE".
	Op string `json:"op"`

	// Key and Value are the KV store payload.
	// Value is empty for DELETE entries.
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ApplyMsg is sent on the applyCh channel whenever an entry is committed
// OR when a snapshot must be installed on startup / after falling too far behind.
//
// There are two kinds of ApplyMsg:
//
//  1. Normal entry (IsSnapshot=false):
//     The server calls store.Put / store.Delete for the given op/key/value.
//
//  2. Snapshot (IsSnapshot=true):
//     The server replaces the entire store contents with the snapshot data.
//     This happens on restart (load from disk) or when a follower is so far
//     behind the leader that the missing log entries have been compacted away.
type ApplyMsg struct {
	// ── Normal entry fields ───────────────────────────────────────────────
	IsSnapshot bool   // true → this is a snapshot install, not a log entry
	Index      int    // log position (used for deduplication and WaitForApply)
	Op         string // "PUT" or "DELETE"  (ignored when IsSnapshot=true)
	Key        string // (ignored when IsSnapshot=true)
	Value      string // empty for DELETE   (ignored when IsSnapshot=true)

	// ── Snapshot fields (only set when IsSnapshot=true) ───────────────────
	Snapshot      []byte // serialised KV map (JSON-encoded map[string]string)
	SnapshotIndex int    // lastIncludedIndex: the log index covered by this snapshot
	SnapshotTerm  int    // lastIncludedTerm:  the term of that log entry
}

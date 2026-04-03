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

// ApplyMsg is sent on the applyCh channel whenever an entry is committed.
//
// "Committed" means a majority of nodes have the entry in their logs and
// it will never be overwritten. At this point it is safe — and required —
// to apply it to the in-memory store.
//
// The server reads from applyCh in a loop and calls store.Put / store.Delete
// for each message it receives. This is the bridge between the Raft layer
// and the key-value store built in Week 1.
type ApplyMsg struct {
	Index int    // which log position this is (useful for deduplication)
	Op    string // "PUT" or "DELETE"
	Key   string
	Value string // empty for DELETE
}

// rpc.go defines the request and reply structs for the two RPCs that Raft
// uses to communicate between nodes.
//
// Raft only needs TWO network calls to implement the entire algorithm:
//
//   RequestVote  — used during leader elections
//   AppendEntries — used for heartbeats AND log replication
//
// Go's net/rpc package will serialize these structs automatically over TCP.
// Both the sender and receiver must use the exact same struct layout.
package raft

// ── RequestVote ───────────────────────────────────────────────────────────────
//
// Sent by a candidate to every other node when it wants to become leader.
// A node can win an election only if it collects votes from a majority.

// RequestVoteArgs is what the candidate sends to each peer.
type RequestVoteArgs struct {
	// Term is the candidate's current election epoch.
	// If a receiver's term is higher, it rejects the vote immediately —
	// the candidate is stale and shouldn't be leader.
	Term int

	// CandidateID is the id of the node asking for a vote (e.g. "node1").
	// Used so the receiver can record who it voted for (one vote per term).
	CandidateID string

	// LastLogIndex and LastLogTerm describe the end of the candidate's log.
	// A receiver only votes YES if the candidate's log is at least as
	// up-to-date as its own — this prevents a node that missed entries
	// from becoming leader and overwriting committed data.
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is what the peer sends back.
type RequestVoteReply struct {
	// Term is the responder's current term. If this is higher than the
	// candidate's term, the candidate must step down to follower.
	Term int

	// VoteGranted is true if the peer is giving its vote to this candidate.
	VoteGranted bool
}

// ── AppendEntries ─────────────────────────────────────────────────────────────
//
// Sent by the leader to all followers constantly.
// When Entries is empty it is a heartbeat — just a "I'm alive, don't elect
// anyone else" signal. When Entries is non-empty it carries new log entries
// that the follower must append to its own log.

// AppendEntriesArgs is what the leader sends to each follower.
type AppendEntriesArgs struct {
	// Term is the leader's current term. If the follower has a higher term
	// it rejects the call and the caller steps down.
	Term int

	// LeaderID lets followers redirect clients: if a client accidentally
	// sends a write to a follower, the follower can tell it "go to LeaderID".
	LeaderID string

	// PrevLogIndex and PrevLogTerm describe the log entry IMMEDIATELY
	// BEFORE the new entries being sent. The follower checks that its own
	// log has the same entry at PrevLogIndex — if not, the logs have
	// diverged and the follower rejects the call so the leader can back up.
	// This consistency check is what keeps all logs identical.
	PrevLogIndex int
	PrevLogTerm  int

	// Entries are the new log entries to append. Empty slice = heartbeat.
	Entries []LogEntry

	// LeaderCommit is the leader's commitIndex. Once a follower sees this,
	// it knows it can advance its own commitIndex up to min(LeaderCommit,
	// index of last new entry), and apply those entries to its store.
	LeaderCommit int
}

// ── InstallSnapshot ───────────────────────────────────────────────────────────
//
// Sent by the leader to a follower that has fallen so far behind that the
// log entries it needs have already been compacted into a snapshot.
// Instead of replaying hundreds of individual entries, the leader ships the
// full snapshot in one RPC so the follower can catch up instantly.

// InstallSnapshotArgs is what the leader sends.
type InstallSnapshotArgs struct {
	// Term is the leader's current term.
	Term     int
	LeaderID string

	// LastIncludedIndex and LastIncludedTerm describe the last log entry
	// covered by this snapshot. After installing, the follower's log
	// starts fresh from this point.
	LastIncludedIndex int
	LastIncludedTerm  int

	// Data is the full serialised state of the KV store at LastIncludedIndex.
	// The follower replaces its entire store with this data.
	Data []byte
}

// InstallSnapshotReply is what the follower sends back.
type InstallSnapshotReply struct {
	// Term is the follower's current term. If higher than the leader's,
	// the leader steps down.
	Term int
}

// AppendEntriesReply is what the follower sends back.
type AppendEntriesReply struct {
	// Term is the follower's current term (leader steps down if stale).
	Term int

	// Success is true if the follower accepted the entries.
	// False means the consistency check at PrevLogIndex failed.
	Success bool

	// ConflictTerm and ConflictIndex are hints the follower provides when
	// Success=false so the leader can jump back quickly instead of
	// decrementing nextIndex one step at a time.
	//
	// If the follower has no entry at PrevLogIndex at all:
	//   ConflictTerm  = -1
	//   ConflictIndex = len(follower's log)   ← leader should send from here
	//
	// If the follower has an entry at PrevLogIndex but with the wrong term:
	//   ConflictTerm  = the term of that conflicting entry
	//   ConflictIndex = the first index in the follower's log with that term
	//     ← leader skips past the whole conflicting term in one RPC
	ConflictTerm  int
	ConflictIndex int
}

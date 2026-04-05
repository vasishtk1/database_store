// snapshot.go implements Raft log compaction and snapshot transfer.
//
// ── Why snapshots? ────────────────────────────────────────────────────────────
//
// The Raft log grows forever. Every PUT and DELETE appended to it adds another
// entry. Without compaction, the log would eat all disk space and restart
// replay would take longer and longer.
//
// Snapshots solve this: every 100 committed entries, the server takes a
// point-in-time photo of the in-memory KV map and hands it to Raft via
// TakeSnapshot(). Raft saves the photo to disk and discards all log entries
// up to that point. Now the log starts from a much smaller base.
//
// ── What about followers that miss entries? ───────────────────────────────────
//
// If a follower crashes and restarts after the leader has already compacted
// away the entries it missed, the normal AppendEntries path can't help —
// those entries are gone. Instead, the leader sends the full snapshot via
// InstallSnapshot. The follower discards its own log and replaces its KV
// store state with the snapshot data, then catches up with any new entries
// via normal AppendEntries from that point on.
//
// ── Log index arithmetic after compaction ────────────────────────────────────
//
// Before snapshotting the log array looks like:
//
//	n.log = [ {0,0,sentinel}, {1,1,PUT...}, {2,1,PUT...}, ..., {50,2,PUT...} ]
//	n.snapshotIndex = 0   (no snapshot yet)
//
// After snapshotting at index 50:
//
//	n.log = [ {50,2,sentinel} ]   ← sentinel now represents index 50
//	n.snapshotIndex = 50
//
// The helper arrayIdx(logIdx) converts a log index to an array position:
//
//	arrayIdx(k) = k - n.snapshotIndex
//
// So n.log[0] is always the sentinel at snapshotIndex, and
// n.log[1] is the first real entry AFTER the snapshot.
package raft

import (
	"log"
)

// ── TakeSnapshot (called by the application) ──────────────────────────────────

// TakeSnapshot compacts the log up to lastApplied and saves the snapshot.
//
// Called by server.go every 100 applied entries. data is the full serialised
// KV store state at the moment the snapshot was taken.
//
// After this call:
//   - All log entries with index <= lastApplied are discarded.
//   - n.snapshotIndex is advanced to lastApplied.
//   - The snapshot is persisted to BoltDB for crash recovery.
func (n *Node) TakeSnapshot(data []byte, lastApplied int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Nothing to do if we've already snapshotted at or past this point.
	if lastApplied <= n.snapshotIndex {
		return
	}

	// The entry at lastApplied must exist in our log.
	arrIdx := lastApplied - n.snapshotIndex
	if arrIdx < 0 || arrIdx >= len(n.log) {
		log.Printf("[raft %s] TakeSnapshot: index %d out of range (snapshotIndex=%d logLen=%d)",
			n.id, lastApplied, n.snapshotIndex, len(n.log))
		return
	}

	newSnapshotTerm := n.log[arrIdx].Term

	// ── Compact the log ───────────────────────────────────────────────────
	// Keep only entries after lastApplied. The new log[0] is a sentinel
	// entry at the snapshot boundary, so the array-index arithmetic stays
	// consistent: log[i].Index == snapshotIndex + i for all i.
	compacted := make([]LogEntry, 1, len(n.log)-arrIdx)
	compacted[0] = LogEntry{Index: lastApplied, Term: newSnapshotTerm} // new sentinel
	compacted = append(compacted, n.log[arrIdx+1:]...)
	n.log = compacted

	n.snapshotIndex = lastApplied
	n.snapshotTerm = newSnapshotTerm

	// ── Persist snapshot + compacted log ──────────────────────────────────
	if err := n.persister.saveSnapshot(data, lastApplied, newSnapshotTerm); err != nil {
		log.Printf("[raft %s] saveSnapshot error: %v", n.id, err)
	}
	n.persist() // also re-save the Raft state (log is now shorter)

	log.Printf("[raft %s] snapshot taken | snapshotIndex=%d logLen=%d",
		n.id, n.snapshotIndex, len(n.log)-1)
}

// SnapshotIndex returns the index of the last log entry included in the
// most recent snapshot (0 if no snapshot has been taken yet).
// Safe to call from any goroutine.
func (n *Node) SnapshotIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotIndex
}

// ── InstallSnapshot RPC (sent by leader, received by follower) ────────────────

// sendInstallSnapshot sends a full snapshot to a follower that has fallen
// so far behind that the missing log entries have already been compacted away.
//
// Called from replication.go when nextIndex[peerID] <= snapshotIndex.
func (n *Node) sendInstallSnapshot(peerID string) {
	// Load from disk first (I/O), then verify under lock that this snapshot
	// still matches the leader's compaction point — avoids shipping bytes
	// that belong to an older snapshot after a fresh TakeSnapshot().
	data, snapIdx, snapTerm, err := n.persister.loadSnapshot()
	if err != nil || data == nil || snapIdx == 0 {
		return
	}

	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	if snapIdx != n.snapshotIndex || snapTerm != n.snapshotTerm {
		n.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              data,
	}
	term := n.currentTerm
	n.mu.Unlock()

	// ── Send the RPC ──────────────────────────────────────────────────────
	reply := &InstallSnapshotReply{}
	if err := n.callRPC(peerID, "Node.InstallSnapshot", args, reply); err != nil {
		return
	}

	// ── Process reply ─────────────────────────────────────────────────────
	n.mu.Lock()
	defer n.mu.Unlock()

	// Stale leader check.
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if n.role != Leader || n.currentTerm != term {
		return
	}

	// Advance the follower's tracking indices so we don't send the snapshot
	// again on the next heartbeat tick.
	if args.LastIncludedIndex > n.matchIndex[peerID] {
		n.matchIndex[peerID] = args.LastIncludedIndex
		n.nextIndex[peerID] = args.LastIncludedIndex + 1
	}

	log.Printf("[raft %s] installed snapshot on %s | index=%d",
		n.id, peerID, args.LastIncludedIndex)
}

// InstallSnapshot is the RPC handler — called on followers by the leader.
// net/rpc calls this as "Node.InstallSnapshot".
func (n *Node) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm

	// ── Rule 1: Reject stale leaders ──────────────────────────────────────
	if args.Term < n.currentTerm {
		return nil
	}

	// ── Rule 2: Step down if needed, reset election timer ─────────────────
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.persist()
	}
	n.role = Follower
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// ── Rule 3: Ignore stale snapshots ────────────────────────────────────
	// If we've already applied entries past this snapshot, there's nothing
	// to install — our state is already newer.
	if args.LastIncludedIndex <= n.snapshotIndex {
		return nil
	}

	// ── Rule 4: Compact / replace the log ────────────────────────────────
	// If our log already contains entries past the snapshot index, keep them.
	// Otherwise, start fresh with just the sentinel.
	newLog := []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	arrIdx := args.LastIncludedIndex - n.snapshotIndex
	if arrIdx >= 0 && arrIdx < len(n.log) &&
		n.log[arrIdx].Term == args.LastIncludedTerm {
		// We already have entries beyond the snapshot; keep them.
		newLog = append(newLog, n.log[arrIdx+1:]...)
	}
	// Otherwise drop everything — our log diverged.
	n.log = newLog
	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm

	// Advance commit and applied pointers so the apply loop doesn't try
	// to re-deliver entries the snapshot already covers.
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}

	// ── Rule 5: Save snapshot to disk and persist Raft state ──────────────
	if err := n.persister.saveSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm); err != nil {
		log.Printf("[raft %s] InstallSnapshot save error: %v", n.id, err)
	}
	n.persist()

	// ── Rule 6: Deliver snapshot to the KV store via applyCh ──────────────
	// We must send this OUTSIDE the lock to avoid deadlock with the server's
	// apply loop. We'll do it by signalling loadedSnapshot and setting a flag.
	n.loadedSnapshot = args.Data
	n.pendingSnapshotApply = true

	log.Printf("[raft %s] snapshot installed | index=%d", n.id, args.LastIncludedIndex)
	return nil
}

// arrayIdx converts a log index to its position in n.log.
// n.log[0] is always the sentinel at n.snapshotIndex.
// Caller must hold n.mu.
func (n *Node) arrayIdx(logIndex int) int {
	return logIndex - n.snapshotIndex
}

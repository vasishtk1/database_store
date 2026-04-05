// replication.go implements the log-replication half of Raft.
//
// How log replication works (plain English):
//
//  1. A client sends a write (PUT/DELETE) to the leader via Submit().
//  2. The leader appends the entry to its own log and immediately calls
//     broadcastAppendEntries() to send it to all followers.
//  3. Each follower checks: "does the entry just before the new one match
//     what I have?" If yes → append and reply success. If no → reply failure.
//  4. On failure the leader backs nextIndex up for that follower and retries
//     until the logs are consistent.
//  5. Once a MAJORITY of nodes (including the leader) have the entry in their
//     logs, the leader advances commitIndex and notifies followers via the
//     next AppendEntries (through LeaderCommit).
//  6. The apply loop (apply.go) then pushes committed entries to the KV store.
package raft

import (
	"log"
	"time"
)

// ── Heartbeat loop ────────────────────────────────────────────────────────────

// runHeartbeatLoop sends AppendEntries to all peers on every heartbeatInterval
// tick. It runs as a goroutine for the entire duration of the leadership and
// exits as soon as the node is no longer the leader.
//
// Even when there are no new entries to send, the empty AppendEntries acts as
// a "keep-alive" that prevents followers from starting spurious elections.
func (n *Node) runHeartbeatLoop() {
	for {
		// Exit if we're no longer the leader (e.g. we stepped down because
		// we saw a higher term in someone's reply).
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		// Send AppendEntries to all followers concurrently.
		n.broadcastAppendEntries()

		// Sleep until the next heartbeat tick.
		time.Sleep(heartbeatInterval)
	}
}

// broadcastAppendEntries sends one AppendEntries RPC to every peer in parallel.
// Called both by the heartbeat loop (periodic) and by Submit() (immediately
// after a new entry is appended, to minimise commit latency).
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	// Collect peer IDs while holding the lock.
	peerIDs := make([]string, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}
	n.mu.Unlock()

	for _, peerID := range peerIDs {
		peerID := peerID // capture for goroutine
		go n.sendAppendEntries(peerID)
	}
}

// sendAppendEntries builds and sends one AppendEntries RPC to a single peer,
// then processes the reply. This is the most complex function in Raft.
func (n *Node) sendAppendEntries(peerID string) {
	// ── Build the RPC args while holding the lock ─────────────────────────
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peerID]

	// ── Snapshot path: follower has fallen behind the compaction point ────
	// If the next entry we'd send has already been discarded into a snapshot,
	// we can't replay it from the log. Send the full snapshot instead.
	if nextIdx <= n.snapshotIndex {
		n.mu.Unlock()
		go n.sendInstallSnapshot(peerID)
		return
	}

	// prevLogIndex is the index of the entry immediately before the new ones.
	// Convert to array index using snapshotIndex as the base offset.
	prevLogIndex := nextIdx - 1
	prevArrIdx   := n.arrayIdx(prevLogIndex) // = prevLogIndex - snapshotIndex
	prevLogTerm  := n.log[prevArrIdx].Term

	// Entries to send: everything from nextIdx to end of log.
	// Convert nextIdx to array position first.
	nextArrIdx := n.arrayIdx(nextIdx)
	entries := make([]LogEntry, len(n.log[nextArrIdx:]))
	copy(entries, n.log[nextArrIdx:])

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	// ── Make the RPC call (no lock held) ──────────────────────────────────
	reply := &AppendEntriesReply{}
	if err := n.callRPC(peerID, "Node.AppendEntries", args, reply); err != nil {
		// Network error — the peer is down or unreachable. Try again on the
		// next heartbeat tick. No state changes needed.
		return
	}

	// ── Process the reply under the lock ─────────────────────────────────
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule: if the reply contains a higher term, we are a stale leader.
	// Step down immediately.
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	// Ignore replies that are no longer relevant (we changed term or role
	// while the RPC was in flight).
	if n.role != Leader || n.currentTerm != args.Term {
		return
	}

	if reply.Success {
		// ── Follower accepted the entries ─────────────────────────────────
		// Advance matchIndex and nextIndex for this peer.
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID]  = newMatch + 1
		}

		// Check whether we can now advance commitIndex.
		// (A new entry is committed once a majority have it in their logs.)
		n.maybeAdvanceCommitIndex()

	} else {
		// ── Follower rejected — log inconsistency ─────────────────────────
		// Use the ConflictTerm / ConflictIndex hint to jump back quickly
		// instead of decrementing one step at a time (optimisation from
		// the Raft paper, section 5.3).

		if reply.ConflictTerm < 0 {
			// The follower has no entry at PrevLogIndex at all.
			// Jump directly to the end of the follower's log.
			n.nextIndex[peerID] = reply.ConflictIndex

		} else {
			// The follower has an entry at PrevLogIndex but with the wrong term.
			// Search our own log for the last entry with ConflictTerm.
			// If we have entries with that term, start from just after them.
			// If we don't, start from the follower's ConflictIndex.
			newNext := reply.ConflictIndex
			for i := len(n.log) - 1; i >= 1; i-- {
				if n.log[i].Term == reply.ConflictTerm {
					newNext = n.log[i].Index + 1
					break
				}
			}
			n.nextIndex[peerID] = newNext
		}

		// nextIndex must not point before the first real entry after the snapshot.
		minNext := n.snapshotIndex + 1
		if minNext < 1 {
			minNext = 1
		}
		if n.nextIndex[peerID] < minNext {
			n.nextIndex[peerID] = minNext
		}
	}
}

// maybeAdvanceCommitIndex checks whether any log index beyond the current
// commitIndex is now replicated on a majority of nodes, and if so, advances
// commitIndex to the highest such index.
//
// Safety rule from the Raft paper: a leader can only commit entries from its
// CURRENT term. It may implicitly commit older entries at the same time, but
// it must not count them alone — doing so can lead to committed entries being
// overwritten. We enforce this with the `n.log[arrIdx].Term != n.currentTerm` check.
//
// Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex() {
	lastIdx    := n.lastLogIndex()
	totalNodes := len(n.peers) + 1 // peers + leader
	prevCommit := n.commitIndex

	for idx := n.commitIndex + 1; idx <= lastIdx; idx++ {
		// Convert log index to array index using snapshotIndex as base.
		arrIdx := n.arrayIdx(idx)
		if arrIdx < 0 || arrIdx >= len(n.log) {
			continue
		}

		// Only commit entries from the current term (safety rule).
		if n.log[arrIdx].Term != n.currentTerm {
			continue
		}

		// Count how many nodes have replicated this index.
		count := 1 // leader itself
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= idx {
				count++
			}
		}

		// count*2 > totalNodes  ←→  count > totalNodes/2  (majority)
		if count*2 > totalNodes {
			n.commitIndex = idx
			log.Printf("[raft %s] committed index=%d", n.id, idx)
		}
	}
	if n.commitIndex != prevCommit {
		n.persist()
	}
}

// ── AppendEntries RPC handler ─────────────────────────────────────────────────

// AppendEntries is called by the leader to replicate log entries and send
// heartbeats. It is the most frequently called RPC in the system.
// net/rpc calls this as "Node.AppendEntries".
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Default reply: failure.
	reply.Term    = n.currentTerm
	reply.Success = false
	reply.ConflictTerm  = -1
	reply.ConflictIndex = 0

	// ── Rule 1: Reject stale leaders ──────────────────────────────────────
	// If the sender's term is lower than ours, it is a ghost leader from a
	// previous term. Reject and let it know our current term so it steps down.
	if args.Term < n.currentTerm {
		return nil
	}

	// ── Rule 2: Recognise a current or newer leader ───────────────────────
	// Any valid AppendEntries (term >= ours) resets our election timer —
	// there IS a live leader, so we should not start an election.
	if args.Term > n.currentTerm {
		// We found out about a newer term; update and become a follower.
		n.currentTerm = args.Term
		n.votedFor    = ""
		n.persist()
	}
	// Even if terms are equal, step down if we are a candidate
	// (the sender won the election we were both in).
	n.role = Follower
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// ── Rule 3: Log consistency check at PrevLogIndex ────────────────────
	// We must verify that our log matches the leader's log at the point just
	// before the new entries. If it doesn't, we reject so the leader backs up.

	// If PrevLogIndex is behind our snapshot, we've already applied those
	// entries — accept vacuously (the leader is catching us up normally).
	if args.PrevLogIndex < n.snapshotIndex {
		// Trim any entries the snapshot already covers, then fall through
		// to append the rest.
		overlap := n.snapshotIndex - args.PrevLogIndex
		if overlap < len(args.Entries) {
			args.Entries = args.Entries[overlap:]
			args.PrevLogIndex = n.snapshotIndex
			args.PrevLogTerm = n.snapshotTerm
		} else {
			// All entries are before our snapshot — log unchanged, but we must
			// still honor LeaderCommit so commitIndex can advance.
			reply.Success = true
			n.commitUpToLeader(args.LeaderCommit, n.lastLogIndex())
			n.persist()
			return nil
		}
	}

	if args.PrevLogIndex > n.lastLogIndex() {
		// We don't have an entry at PrevLogIndex at all.
		reply.ConflictTerm  = -1
		reply.ConflictIndex = n.lastLogIndex() + 1
		return nil
	}

	prevArrIdx := n.arrayIdx(args.PrevLogIndex)
	if args.PrevLogIndex > n.snapshotIndex && n.log[prevArrIdx].Term != args.PrevLogTerm {
		// Term mismatch: tell the leader the first index of the conflicting term
		// so it can skip the entire section in one round-trip.
		conflictTerm := n.log[prevArrIdx].Term
		reply.ConflictTerm = conflictTerm

		// Walk forward from the start of our log (after sentinel) to find
		// the first entry with this conflicting term.
		for i := 1; i < len(n.log); i++ {
			if n.log[i].Term == conflictTerm {
				reply.ConflictIndex = n.snapshotIndex + i
				break
			}
		}
		return nil
	}

	// ── Rule 4: Append new entries, resolving any conflicts ───────────────
	for i, entry := range args.Entries {
		// Log index of this entry.
		logIdx := args.PrevLogIndex + 1 + i
		arrIdx := n.arrayIdx(logIdx)

		if arrIdx < len(n.log) {
			if n.log[arrIdx].Term == entry.Term {
				continue // already have this entry with matching term — skip
			}
			// Conflicting term: truncate from here and append the rest.
			n.log = n.log[:arrIdx]
			n.log = append(n.log, args.Entries[i:]...)
			break
		} else {
			// Log is shorter than incoming entries; append the rest.
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	// Truncation can shrink the log below our previous commitIndex.
	// Clamp — but never go below snapshotIndex.
	if n.commitIndex > n.lastLogIndex() {
		n.commitIndex = n.lastLogIndex()
	}
	if n.commitIndex < n.snapshotIndex {
		n.commitIndex = n.snapshotIndex
	}

	// ── Rule 5: Advance commitIndex ───────────────────────────────────────
	lastNewIdx := args.PrevLogIndex + len(args.Entries)
	n.commitUpToLeader(args.LeaderCommit, lastNewIdx)

	reply.Success = true
	n.persist()
	return nil
}

// commitUpToLeader sets commitIndex from LeaderCommit per Raft Figure 2.
// lastNewIdx is the index of the last entry in this AppendEntries batch (may equal PrevLogIndex if empty).
// Caller must hold n.mu.
func (n *Node) commitUpToLeader(leaderCommit, lastNewIdx int) {
	if leaderCommit <= n.commitIndex {
		if n.commitIndex > n.lastLogIndex() {
			n.commitIndex = n.lastLogIndex()
		}
		if n.commitIndex < n.snapshotIndex {
			n.commitIndex = n.snapshotIndex
		}
		return
	}
	n.commitIndex = leaderCommit
	if lastNewIdx < n.commitIndex {
		n.commitIndex = lastNewIdx
	}
	if n.commitIndex > n.lastLogIndex() {
		n.commitIndex = n.lastLogIndex()
	}
	if n.commitIndex < n.snapshotIndex {
		n.commitIndex = n.snapshotIndex
	}
}

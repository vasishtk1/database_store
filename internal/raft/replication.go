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

	// nextIndex[peerID] is the next log index we should send to this peer.
	// Everything from nextIndex onwards is "new" for the peer.
	nextIdx := n.nextIndex[peerID]

	// prevLogIndex is the index of the entry immediately before the new ones.
	// The peer will check that it has this entry with prevLogTerm — if not,
	// it will reject the call and we'll back up nextIndex.
	prevLogIndex := nextIdx - 1
	prevLogTerm  := n.log[prevLogIndex].Term // safe: prevLogIndex >= 0 always

	// Entries to send: everything in our log from nextIdx to the end.
	// If nextIdx == len(n.log) this is empty — pure heartbeat.
	entries := make([]LogEntry, len(n.log[nextIdx:]))
	copy(entries, n.log[nextIdx:])

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

		// nextIndex must never go below 1 (index 0 is the sentinel).
		if n.nextIndex[peerID] < 1 {
			n.nextIndex[peerID] = 1
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
// overwritten. We enforce this with the `n.log[idx].Term != n.currentTerm` check.
//
// Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex() {
	lastIdx    := n.lastLogIndex()
	totalNodes := len(n.peers) + 1 // peers + leader

	// Walk forward from commitIndex+1 and find the highest index where a
	// majority of nodes have the entry.
	for idx := n.commitIndex + 1; idx <= lastIdx; idx++ {
		// Only commit entries from the current term (see safety note above).
		if n.log[idx].Term != n.currentTerm {
			continue
		}

		// Count how many nodes have this entry (leader always has it = 1).
		count := 1
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= idx {
				count++
			}
		}

		// Majority check: count * 2 > totalNodes
		// e.g. 3-node cluster needs count=2: 2*2=4 > 3 ✓
		//      5-node cluster needs count=3: 3*2=6 > 5 ✓
		if count*2 > totalNodes {
			n.commitIndex = idx
			log.Printf("[raft %s] committed index=%d", n.id, idx)
		}
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

	if args.PrevLogIndex > n.lastLogIndex() {
		// We don't have an entry at PrevLogIndex at all — our log is shorter.
		// Tell the leader to start sending from the end of our log.
		reply.ConflictTerm  = -1
		reply.ConflictIndex = n.lastLogIndex() + 1
		return nil
	}

	if args.PrevLogIndex > 0 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// We have an entry at PrevLogIndex but its term doesn't match.
		// Tell the leader the first index where this conflict term starts
		// so it can skip the whole conflicting section in one round-trip.
		conflictTerm := n.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm

		// Walk backward to find the first index with this conflicting term.
		for i := 1; i <= args.PrevLogIndex; i++ {
			if n.log[i].Term == conflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return nil
	}

	// ── Rule 4: Append new entries, resolving any conflicts ───────────────
	// Walk through the incoming entries. For each one:
	//   - If our log already has an entry at that index with the SAME term,
	//     they match — no action needed, move to the next entry.
	//   - If our log has an entry at that index with a DIFFERENT term,
	//     our entry is stale — truncate from here and append the rest.
	//   - If our log doesn't reach that index yet, just append.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i

		if idx < len(n.log) {
			if n.log[idx].Term == entry.Term {
				continue // already have this entry — skip
			}
			// Conflicting entry: truncate our log from idx onwards and
			// append everything from args.Entries[i:].
			n.log = n.log[:idx]
			n.log = append(n.log, args.Entries[i:]...)
			break
		} else {
			// Our log is shorter than the incoming entries; append the rest.
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}
	n.persist()

	// ── Rule 5: Advance commitIndex ───────────────────────────────────────
	// The leader tells us the highest index it has committed. We can advance
	// our own commitIndex to min(LeaderCommit, index of last new entry).
	// The min() ensures we don't commit entries we haven't actually received.
	if args.LeaderCommit > n.commitIndex {
		lastNewIdx := args.PrevLogIndex + len(args.Entries)
		n.commitIndex = args.LeaderCommit
		if lastNewIdx < n.commitIndex {
			n.commitIndex = lastNewIdx
		}
	}

	reply.Success = true
	return nil
}

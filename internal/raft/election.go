// election.go implements the leader-election half of Raft.
//
// How elections work (plain English):
//
//  1. Every follower runs an election timer set to a random duration (150–300 ms).
//  2. The timer is reset every time the follower receives a valid heartbeat
//     from a leader. As long as a healthy leader exists, timers never fire.
//  3. If the timer fires (because the leader crashed or the network failed),
//     the follower increments its term, becomes a candidate, votes for itself,
//     and sends RequestVote RPCs to all other nodes in parallel.
//  4. If a candidate receives YES votes from a majority (including its own vote)
//     before its timer fires again, it becomes the new leader.
//  5. Any node that sees a message from a higher term immediately steps down
//     to follower — this prevents two leaders existing at the same time.
package raft

import (
	"log"
)

// handleElectionTimeout is called by the election timer goroutine when it fires.
// It runs in a separate goroutine (spawned by time.AfterFunc in node.go).
func (n *Node) handleElectionTimeout() {
	n.mu.Lock()
	// Leaders send heartbeats — they never need to start an election.
	// If we somehow ended up here as a leader, just ignore the timeout.
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	// Everyone else (Follower or Candidate stuck from a previous round) starts
	// or retries an election.
	n.startElection()
}

// startElection transitions this node to Candidate, increments the term,
// votes for itself, and fires off parallel RequestVote RPCs to all peers.
// If a majority votes yes, this node becomes the new leader.
func (n *Node) startElection() {
	n.mu.Lock()

	// ── Transition to Candidate ───────────────────────────────────────────
	n.role = Candidate
	n.currentTerm++          // new term for this election round
	n.votedFor = n.id        // vote for ourselves (counts as 1 vote)
	n.persist()              // MUST write term + votedFor to disk before sending RPCs
	n.resetElectionTimer()   // restart timer: if no majority before it fires, retry

	// Snapshot the values we need to send in RequestVote RPCs.
	// We snapshot under the lock, then release before making network calls.
	term     := n.currentTerm
	lastIdx  := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	// Collect peer IDs so we can iterate after releasing the lock.
	peerIDs := make([]string, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}

	n.mu.Unlock()

	log.Printf("[raft %s] starting election | term=%d lastLogIndex=%d lastLogTerm=%d",
		n.id, term, lastIdx, lastTerm)

	// ── Vote counting ─────────────────────────────────────────────────────
	// votes is only ever accessed while holding n.mu, so it's safe
	// to share across goroutines without a separate mutex.
	votes       := 1     // we already voted for ourselves
	wonElection := false // prevents multiple "become leader" transitions

	totalNodes  := len(peerIDs) + 1       // us + peers
	votesNeeded := totalNodes/2 + 1       // strict majority

	// ── Immediate majority (single-node cluster) ────────────────────────────
	// With zero peers, votesNeeded is 1 and only our self-vote counts. No
	// RequestVote RPCs are sent, so we must win the election here.
	n.mu.Lock()
	if n.role == Candidate && n.currentTerm == term && votes >= votesNeeded {
		wonElection = true
		n.becomeLeader()
		go n.runHeartbeatLoop()
	}
	n.mu.Unlock()

	// ── Send RequestVote to every peer in parallel ────────────────────────
	for _, peerID := range peerIDs {
		peerID := peerID // capture loop variable for the goroutine

		go func() {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}

			// callRPC does not hold n.mu — it's a pure network call.
			if err := n.callRPC(peerID, "Node.RequestVote", args, reply); err != nil {
				// Peer unreachable or timed out — just skip it.
				// We might still win with votes from other peers.
				return
			}

			// ── Process the reply under the lock ─────────────────────────
			n.mu.Lock()
			defer n.mu.Unlock()

			// Rule: if any reply contains a higher term, we are stale.
			// Step down immediately — we cannot win this election.
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			// Ignore stale replies: our state has already moved on.
			// (e.g. we already won, or a newer election started)
			if n.role != Candidate || n.currentTerm != term {
				return
			}

			if !reply.VoteGranted {
				return
			}

			// ── We got a vote! ────────────────────────────────────────────
			votes++
			if !wonElection && votes >= votesNeeded {
				// First time we cross the majority threshold → become leader.
				wonElection = true
				n.becomeLeader()

				// Start the heartbeat loop in a separate goroutine.
				// It will run until this node stops being leader.
				go n.runHeartbeatLoop()
			}
		}()
	}
}

// RequestVote is the RPC handler — called by candidates asking for our vote.
// net/rpc calls this as "Node.RequestVote".
//
// The method signature (pointer receiver, *Args, *Reply, error) is the
// format required by Go's net/rpc package.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Default: deny the vote unless all checks pass.
	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// ── Rule 1: Reject stale candidates ──────────────────────────────────
	// If the candidate's term is lower than ours, it is from a previous
	// election round. It cannot be a valid leader — reject.
	if args.Term < n.currentTerm {
		return nil
	}

	// ── Rule 2: Step down if we see a higher term ─────────────────────────
	// A higher term means our knowledge is stale. Become a follower, clear
	// our vote (new term = new vote), and continue to evaluate the request.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	// Update the reply term in case becomeFollower changed currentTerm.
	reply.Term = n.currentTerm

	// ── Rule 3: Each node votes at most once per term ─────────────────────
	// If we already voted for a different candidate this term, deny.
	// (Voting for the same candidate twice is allowed — idempotent retry.)
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return nil
	}

	// ── Rule 4: Only vote for a candidate whose log is at least as up-to-date ──
	// "At least as up-to-date" means:
	//   a) Candidate's last entry has a higher term than ours, OR
	//   b) Same last term but candidate's log is at least as long as ours.
	// This ensures a new leader always has all committed entries.
	candidateUpToDate :=
		args.LastLogTerm > n.lastLogTerm() ||
			(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())

	if !candidateUpToDate {
		return nil
	}

	// ── All checks passed — grant the vote ───────────────────────────────
	n.votedFor = args.CandidateID
	n.persist() // persist before replying (safety requirement from Raft paper)
	n.resetElectionTimer() // we just heard from a valid candidate; reset timer

	reply.VoteGranted = true
	log.Printf("[raft %s] voted for %s | term=%d", n.id, args.CandidateID, n.currentTerm)
	return nil
}

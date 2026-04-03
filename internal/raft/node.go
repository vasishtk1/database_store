// node.go defines the core Raft node: its state, constructor, and the small
// helper methods that every other file in this package relies on.
//
// A Raft node is always in exactly one of three roles:
//
//	Follower  — the default. Listens for heartbeats; votes in elections.
//	Candidate — trying to become leader. Sends RequestVote RPCs.
//	Leader    — drives all writes. Sends AppendEntries RPCs to followers.
//
// ── Concurrency model ────────────────────────────────────────────────────────
// All fields of Node are protected by a single mutex (n.mu). The rule is:
//   - Acquire n.mu before reading or writing any field.
//   - Release n.mu before making any outgoing RPC call (RPCs can block).
//   - Helper methods that say "must be called with n.mu held" rely on the
//     caller having already acquired the lock.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// ── Role ─────────────────────────────────────────────────────────────────────

// Role is the current state of a Raft node in the leader-election state machine.
type Role int

const (
	Follower  Role = iota // starting state; also falls back here when stale
	Candidate             // running for leader
	Leader                // won the election; drives replication
)

// String makes Role printable in log messages.
func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// ── Timing constants ─────────────────────────────────────────────────────────

const (
	// electionTimeoutMin / Max: the random window for the election timer.
	// If a follower doesn't hear from a leader within this window it starts
	// a new election. The range must be much wider than heartbeatInterval
	// so normal heartbeats always reset the timer before it fires.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond

	// heartbeatInterval: how often the leader sends AppendEntries to followers.
	// Must be smaller than electionTimeoutMin to prevent spurious elections.
	heartbeatInterval = 50 * time.Millisecond
)

// ── Node ─────────────────────────────────────────────────────────────────────

// Node is one participant in the Raft cluster.
// Create one per process with New(); shut it down with Stop().
type Node struct {
	mu sync.Mutex // guards every field below

	// ── Identity ─────────────────────────────────────────────────────────
	id    string            // unique name for this node, e.g. "node1"
	peers map[string]string // id → TCP address of every OTHER node in the cluster

	// ── Durable state (persisted to disk before every state change) ───────
	// Losing any of these on a crash would violate Raft's safety guarantees.
	currentTerm int        // highest term this node has ever seen
	votedFor    string     // candidateID we voted for this term ("" = none yet)
	log         []LogEntry // ordered list of all commands Raft has processed
	//   log[0] is a sentinel (Index=0,Term=0) — never applied, just simplifies
	//   boundary maths so every real entry is at log[entry.Index].

	// ── Volatile state (rebuilt after a crash from durable state) ─────────
	role        Role // current role in the state machine
	commitIndex int  // highest log index known to be committed (majority acked)
	lastApplied int  // highest log index actually applied to the KV store

	// ── Leader-only volatile state (reset after every election) ───────────
	// Only valid when role == Leader. Tracks per-follower replication progress.
	nextIndex  map[string]int // next log index to send to each follower
	matchIndex map[string]int // highest index known to be replicated on each follower

	// ── Infrastructure ────────────────────────────────────────────────────
	persister     *Persister         // saves durable state to BoltDB
	applyCh       chan ApplyMsg       // committed entries flow out here → KV store
	electionTimer *time.Timer        // fires if no heartbeat received in time
	rpcClients    map[string]*rpc.Client // cached TCP connections to peers
	listener      net.Listener           // our own RPC server's TCP listener
}

// New creates, initialises, and starts a Raft node.
//
// Parameters:
//
//	id         — unique name for this node, e.g. "node1"
//	peers      — map of id → TCP address for every OTHER node
//	             e.g. {"node2": "localhost:7002", "node3": "localhost:7003"}
//	dbPath     — path to the BoltDB file for durable state (created if absent)
//	listenAddr — TCP address this node's RPC server will bind to
//	             e.g. "localhost:7001"
//	applyCh    — the caller must read from this channel; each value is one
//	             committed entry ready to be applied to the KV store
//
// The node starts as a Follower with a running election timer.
// Call Stop() to shut down cleanly.
func New(id string, peers map[string]string, dbPath, listenAddr string, applyCh chan ApplyMsg) (*Node, error) {
	// Step 1: open the persister (BoltDB file).
	p, err := openPersister(dbPath)
	if err != nil {
		return nil, fmt.Errorf("raft New: open persister: %w", err)
	}

	// Step 2: build the node with zero/default values.
	n := &Node{
		id:         id,
		peers:      peers,
		persister:  p,
		applyCh:    applyCh,
		role:       Follower,
		rpcClients: make(map[string]*rpc.Client),
	}

	// Step 3: load previously saved state (or zero values on first run).
	term, votedFor, savedLog, err := p.load()
	if err != nil {
		p.close()
		return nil, fmt.Errorf("raft New: load state: %w", err)
	}
	n.currentTerm = term
	n.votedFor = votedFor

	// Always have at least the sentinel entry at index 0.
	// The sentinel has Term=0 and is never applied — it exists so that
	// "no previous entry" can be represented as PrevLogIndex=0, PrevLogTerm=0.
	if len(savedLog) == 0 {
		n.log = []LogEntry{{Index: 0, Term: 0}}
	} else {
		n.log = savedLog
	}

	// Step 4: start the RPC server BEFORE the election timer so that when
	// the timer fires and we send RequestVote, peers can already reply to us.
	if err := n.startRPCServer(listenAddr); err != nil {
		p.close()
		return nil, fmt.Errorf("raft New: start rpc server: %w", err)
	}

	// Step 5: start the apply loop — watches commitIndex and pushes entries
	// to applyCh for the KV store to execute.
	go n.runApplyLoop()

	// Step 6: arm the election timer. From this moment the node will start
	// an election if it doesn't hear from a leader within the timeout window.
	n.electionTimer = time.AfterFunc(n.randomElectionTimeout(), n.handleElectionTimeout)

	log.Printf("[raft %s] started | term=%d logLen=%d addr=%s",
		n.id, n.currentTerm, len(n.log)-1, listenAddr)
	return n, nil
}

// Stop shuts down the node: cancels the election timer, closes the RPC
// listener (so incoming RPCs stop), and closes the BoltDB file.
func (n *Node) Stop() {
	// Stop the election timer to prevent spurious elections after shutdown.
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// Closing the listener causes the accept loop in transport.go to return.
	if n.listener != nil {
		n.listener.Close()
	}
	n.persister.close()
	log.Printf("[raft %s] stopped", n.id)
}

// Submit proposes a new command to the cluster.
//
// Returns (logIndex, term, isLeader).
// If this node is not the leader, isLeader=false and the caller should
// redirect the client to another node and retry.
//
// If isLeader=true the entry has been appended to the leader's log and
// replication has started. The entry will appear on applyCh once committed.
func (n *Node) Submit(op, key, value string) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only the leader can accept new entries.
	if n.role != Leader {
		return -1, n.currentTerm, false
	}

	// Build the new log entry.
	entry := LogEntry{
		Index: n.lastLogIndex() + 1,
		Term:  n.currentTerm,
		Op:    op,
		Key:   key,
		Value: value,
	}
	n.log = append(n.log, entry)
	n.persist() // write to disk before telling anyone about it

	log.Printf("[raft %s] submitted index=%d term=%d op=%s key=%s",
		n.id, entry.Index, entry.Term, op, key)

	// Kick off replication immediately (don't wait for the next heartbeat tick).
	go n.broadcastAppendEntries()

	return entry.Index, entry.Term, true
}

// State returns the current term and whether this node believes it is leader.
// Safe to call from any goroutine.
func (n *Node) State() (term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// LeaderID returns the id of this node if it is the current leader, or ""
// if it is not. Followers do not track who the leader is in this
// implementation — callers should retry on any node until they find the leader.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return ""
}

// ── Internal helpers ─────────────────────────────────────────────────────────
// These are small utility methods called by election.go and replication.go.
// They all require n.mu to be held by the caller.

// lastLogIndex returns the Index field of the last entry in our log.
// Because of the sentinel at position 0 this is always >= 0.
func (n *Node) lastLogIndex() int {
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the Term field of the last entry in our log.
func (n *Node) lastLogTerm() int {
	return n.log[len(n.log)-1].Term
}

// randomElectionTimeout returns a random duration in [min, max).
// Randomness prevents two followers from starting simultaneous elections
// (which would split the vote and nobody would win).
func (n *Node) randomElectionTimeout() time.Duration {
	spread := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(spread)))
}

// resetElectionTimer resets the election timer to a fresh random duration.
// Called whenever we receive a valid message from a current leader or grant
// a vote — either event means we should NOT start an election right now.
// Must be called with n.mu held.
func (n *Node) resetElectionTimer() {
	// Guard against the timer being nil during node startup.
	if n.electionTimer != nil {
		n.electionTimer.Reset(n.randomElectionTimeout())
	}
}

// becomeFollower transitions this node to Follower for the given term.
// Used when we discover a message from a node with a higher term — that
// means our term is stale and we must step down immediately.
// Must be called with n.mu held.
func (n *Node) becomeFollower(term int) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""  // new term → no vote cast yet
	n.persist()      // record the new term on disk before doing anything else
	n.resetElectionTimer()
	log.Printf("[raft %s] became Follower | term=%d", n.id, term)
}

// becomeLeader transitions this node to Leader and initialises the
// per-follower tracking maps (nextIndex and matchIndex).
// Must be called with n.mu held.
func (n *Node) becomeLeader() {
	n.role = Leader
	lastIdx := n.lastLogIndex()

	// Initialise nextIndex optimistically: assume each follower is fully
	// caught up, so we'll start by sending entries from lastIdx+1.
	// If a follower is behind, AppendEntries will fail and we'll back up.
	n.nextIndex = make(map[string]int, len(n.peers))
	n.matchIndex = make(map[string]int, len(n.peers))
	for id := range n.peers {
		n.nextIndex[id] = lastIdx + 1
		n.matchIndex[id] = 0
	}

	log.Printf("[raft %s] became Leader | term=%d lastLogIndex=%d",
		n.id, n.currentTerm, lastIdx)
}

// persist writes currentTerm, votedFor, and log to disk.
// Call this every time any of those three fields changes.
// Must be called with n.mu held.
func (n *Node) persist() {
	if err := n.persister.save(n.currentTerm, n.votedFor, n.log); err != nil {
		// Persistence failure is serious; log it loudly.
		log.Printf("[raft %s] PERSIST ERROR: %v", n.id, err)
	}
}

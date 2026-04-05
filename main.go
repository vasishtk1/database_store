// main.go is the entry point for the KV store server binary.
//
// ── Week 3: multi-node cluster startup ───────────────────────────────────────
//
// Each node is started as a separate process with its own --id flag:
//
//	./database_store --id node1 --config config/cluster.json
//	./database_store --id node2 --config config/cluster.json
//	./database_store --id node3 --config config/cluster.json
//
// The config file tells every node the Raft RPC address and KV client address
// of each peer. Nodes discover each other automatically — no manual wiring.
//
// ── Startup sequence ──────────────────────────────────────────────────────────
//  1. Parse flags and load cluster config
//  2. Open WAL (used only in single-node / Week 1 fallback mode)
//  3. Build in-memory store
//  4. Create applyCh — the channel Raft uses to deliver committed entries
//  5. Start Raft node (loads durable state, starts RPC server + election timer)
//  6. Wire server: give it the store, WAL, and Raft node
//  7. Start the apply loop (reads from applyCh, applies to store, triggers snapshots)
//  8. Start the TCP server and block

// entry point of the program, starts the TCP server, and waits for the clients
package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"database_store/internal/config"
	"database_store/internal/raft"
	"database_store/internal/server"
	"database_store/internal/store"
	"database_store/internal/wal"
)

func main() {
	// ── Flags ─────────────────────────────────────────────────────────────
	// --id      : this node's ID, must match an entry in the config file
	// --config  : path to cluster.json
	// --data    : directory for per-node data files (WAL + Raft BoltDB)
	// --single  : run in single-node (Week 1) mode with no Raft
	nodeID     := flag.String("id",     "",                        "This node's ID (e.g. node1)")
	configPath := flag.String("config", "config/cluster.json",    "Path to cluster config JSON")
	dataDir    := flag.String("data",   "data",                    "Directory for WAL and Raft state files")
	singleNode := flag.Bool("single",   false,                     "Run in single-node (Week 1) mode, no Raft")
	flag.Parse()

	// ── Single-node (Week 1) fallback ─────────────────────────────────────
	// Useful for running / testing without a full 3-node cluster.
	if *singleNode {
		runSingleNode(*dataDir)
		return
	}

	if *nodeID == "" {
		log.Fatal("--id is required in cluster mode (e.g. --id node1)")
	}

	// ── Step 1: Load cluster config ───────────────────────────────────────
	// The config file lists every node's ID, Raft RPC address, and KV address.
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Find this node's own config row.
	self, err := cfg.Self(*nodeID)
	if err != nil {
		log.Fatalf("find self in config: %v", err)
	}

	// Build the peers map: id → raft_addr for every OTHER node.
	// This is passed to raft.New() so it knows who to send RPCs to.
	peers := cfg.Peers(*nodeID)

	fmt.Printf("Starting node %s | raft=%s kv=%s peers=%v\n",
		self.ID, self.RaftAddr, self.KVAddr, peers)

	// ── Step 2: Open WAL (kept for single-node fallback; not used in Raft mode) ──
	walPath := filepath.Join(*dataDir, self.ID+".wal")
	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	// ── Step 3: Build in-memory store ────────────────────────────────────
	s := store.New()

	// ── Step 4: Create applyCh ────────────────────────────────────────────
	// Raft sends committed log entries on this channel; the server reads them
	// and applies them to the store. Buffer of 256 prevents the apply loop
	// from blocking Raft's commit path when the server is briefly busy.
	applyCh := make(chan raft.ApplyMsg, 256)

	// ── Step 5: Start Raft node ───────────────────────────────────────────
	// raft.New() opens BoltDB, loads saved state, starts the RPC server,
	// starts the apply loop goroutine, and arms the election timer.
	raftDBPath := filepath.Join(*dataDir, self.ID+".raft")
	raftNode, err := raft.New(self.ID, peers, raftDBPath, self.RaftAddr, applyCh)
	if err != nil {
		log.Fatalf("start raft node: %v", err)
	}
	defer raftNode.Stop()

	// ── Step 6: Wire the KV server ───────────────────────────────────────
	// Pass the Raft node so PUT/DELETE go through consensus instead of
	// directly touching the store. GET still reads locally (fast path).
	srv := server.New(self.KVAddr, s, w, raftNode)

	// ── Step 7: Start the apply loop ─────────────────────────────────────
	// This goroutine reads from applyCh and applies committed entries to s.
	// It also triggers a snapshot every 100 entries via raftNode.TakeSnapshot().
	srv.RunApplyLoop(applyCh)

	// ── Step 8: Start TCP server (blocks until error) ─────────────────────
	log.Printf("node %s ready — accepting KV clients on %s", self.ID, self.KVAddr)
	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// runSingleNode starts the server in Week-1-compatible single-node mode.
// No Raft, no cluster config — just WAL + store + TCP server.
func runSingleNode(dataDir string) {
	walPath := filepath.Join(dataDir, "single.wal")
	addr    := ":8080"

	fmt.Println("Running in single-node mode on", addr)

	w, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	s := store.New()

	// Replay WAL into store to recover from a previous crash.
	replayCount := 0
	if err := w.Replay(func(e wal.Entry) error {
		switch e.Op {
		case wal.OpPut:
			s.Put(e.Key, e.Value)
		case wal.OpDelete:
			s.Delete(e.Key)
		}
		replayCount++
		return nil
	}); err != nil {
		log.Fatalf("WAL replay: %v", err)
	}
	fmt.Printf("WAL replayed %d entries\n", replayCount)

	// nil raftNode → server uses Week 1 WAL-first path.
	srv := server.New(addr, s, w, nil)
	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

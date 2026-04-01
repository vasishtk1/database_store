// main.go is the entrypoint for the KV store server binary.
//
// It wires together the three core components built in Week 1:
//   - WAL  (write-ahead log)  — durability
//   - Store (in-memory map)   — state machine / fast reads
//   - Server (TCP listener)   — client-facing interface
//
// Startup sequence:
//  1. Open the WAL (creates the file if it's the first run)
//  2. Replay the WAL into the in-memory store (no-op on first run)
//  3. Start accepting TCP connections
package main

import (
	"flag"
	"fmt"
	"log"

	"database_store/internal/server"
	"database_store/internal/store"
	"database_store/internal/wal"
)

func main() {
	// Command-line flags so the server is easy to configure at startup.
	// Example: ./database_store --addr :9000 --wal /var/data/kv.wal
	addr    := flag.String("addr", ":8080", "TCP address the server listens on")
	walPath := flag.String("wal", "data.wal", "Path to the WAL file (created if absent)")
	flag.Parse()

	// ── Step 1: Open the WAL ─────────────────────────────────────────────────
	// If this is a fresh start, BoltDB creates the file.
	// If we're restarting after a crash, BoltDB opens the existing file.
	w, err := wal.Open(*walPath)
	if err != nil {
		log.Fatalf("failed to open WAL at %s: %v", *walPath, err)
	}
	defer w.Close()

	// ── Step 2: Replay WAL → rebuild in-memory state ─────────────────────────
	// On a clean first run this loop executes zero times.
	// After a crash it re-applies every logged PUT/DELETE in order,
	// restoring the store to exactly the state it had before the crash.
	s := store.New()
	replayCount := 0
	err = w.Replay(func(e wal.Entry) error {
		switch e.Op {
		case wal.OpPut:
			s.Put(e.Key, e.Value)
		case wal.OpDelete:
			s.Delete(e.Key)
		}
		replayCount++
		return nil
	})
	if err != nil {
		log.Fatalf("WAL replay failed: %v", err)
	}
	fmt.Printf("WAL replayed %d entries\n", replayCount)

	// ── Step 3: Start the TCP server ─────────────────────────────────────────
	// server.Start() blocks forever, so nothing below this line runs
	// during normal operation. A fatal error (e.g. port already in use)
	// will surface here.
	srv := server.New(*addr, s, w)
	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

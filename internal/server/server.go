// Package server implements the TCP server that clients connect to.
//
// Protocol: plain text, one command per line, newline-terminated.
//
//	PUT key value   → stores value under key, responds "OK"
//	GET key         → responds with the value, or "NULL" if not found
//	DELETE key      → removes the key, responds "OK"
//
// ── Week 1 vs Week 2+ behaviour ──────────────────────────────────────────────
//
// When a Raft node is wired in (raftNode != nil):
//   - PUT / DELETE are forwarded to Raft via Submit(). The server blocks
//     until the entry is committed and applied, then returns "OK".
//   - If this node is not the Raft leader, it returns "ERR not leader"
//     so the client can retry against another node.
//   - GET reads directly from the local in-memory store (eventually
//     consistent — followers may lag by a heartbeat or two).
//
// When raftNode is nil (single-node / Week 1 mode):
//   - PUT / DELETE go directly to the WAL + store, as in Week 1.
//
// ── Apply loop ───────────────────────────────────────────────────────────────
//
// RunApplyLoop() must be called once after New(). It reads committed entries
// from the Raft applyCh channel and applies them to the store. It also
// triggers a snapshot every snapshotEvery entries to keep the log compact.

// A Go dictionary that stores words mapped to other words using a mutex lock
// mutex lock: mechanism that ensures only one thread accesses a shared resource at a time
// prevents data corruption

// listens for incoming connections on a port
// each client gets its own goroutine

package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"database_store/internal/raft"
	"database_store/internal/store"
	"database_store/internal/wal"
)

// snapshotEvery controls how often we compact the Raft log. After this many
// applied entries, the server serialises the store and calls TakeSnapshot().
// 100 is small enough to test easily; in production you'd use 10_000+.
const snapshotEvery = 100

// Server holds everything needed to accept and handle client connections.
type Server struct {
	addr     string       // TCP address to listen on, e.g. ":8080"
	store    *store.Store // the in-memory state machine
	wal      *wal.WAL     // the write-ahead log (Week 1 / single-node only)
	raftNode *raft.Node   // nil in single-node (Week 1) mode
}

// New creates a Server. Call Start() to begin accepting connections.
// raftNode may be nil for single-node (Week 1) operation.
func New(addr string, s *store.Store, w *wal.WAL, r *raft.Node) *Server {
	return &Server{addr: addr, store: s, wal: w, raftNode: r}
}

// RunApplyLoop reads committed entries from applyCh and applies them to the
// store. Must be called once in its own goroutine BEFORE Start().
//
// It also handles snapshot ApplyMsgs: when IsSnapshot=true it restores the
// entire store from the snapshot bytes instead of applying a single entry.
// Every snapshotEvery applied entries it tells Raft to compact the log.
func (srv *Server) RunApplyLoop(applyCh <-chan raft.ApplyMsg) {
	go func() {
		entriesSinceSnapshot := 0

		for msg := range applyCh {
			// ── Snapshot install ──────────────────────────────────────────
			// Sent on startup (loaded from disk) or when the leader ships a
			// full snapshot because this node fell too far behind.
			if msg.IsSnapshot {
				if err := srv.store.LoadSnapshot(msg.Snapshot); err != nil {
					log.Printf("[server] LoadSnapshot error: %v", err)
				} else {
					log.Printf("[server] snapshot installed | index=%d", msg.SnapshotIndex)
				}
				entriesSinceSnapshot = 0
				continue
			}

			// ── Normal log entry ──────────────────────────────────────────
			switch msg.Op {
			case "PUT":
				srv.store.Put(msg.Key, msg.Value)
			case "DELETE":
				srv.store.Delete(msg.Key)
			default:
				log.Printf("[server] unknown op in ApplyMsg: %q", msg.Op)
				continue
			}

			entriesSinceSnapshot++

			// ── Snapshot trigger ──────────────────────────────────────────
			// Every snapshotEvery applied entries, serialise the store and
			// hand the bytes to Raft. Raft will compact its log and save the
			// snapshot to disk so restarts and lagging followers can use it.
			if srv.raftNode != nil && entriesSinceSnapshot >= snapshotEvery {
				data, err := srv.store.Snapshot()
				if err != nil {
					log.Printf("[server] snapshot error: %v", err)
				} else {
					srv.raftNode.TakeSnapshot(data, msg.Index)
					entriesSinceSnapshot = 0
					log.Printf("[server] triggered snapshot at index=%d", msg.Index)
				}
			}
		}
	}()
}

// Start binds to the TCP address and blocks, accepting client connections.
// Each accepted connection is handed off to handleConn in a new goroutine.
func (srv *Server) Start() error {
	ln, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.addr, err)
	}
	defer ln.Close()
	return srv.Serve(ln)
}

// Serve accepts connections on ln until Accept fails (for example when ln is closed).
// Used by Start after Listen; tests pass a listener bound to 127.0.0.1:0 for a free port.
func (srv *Server) Serve(ln net.Listener) error {
	fmt.Printf("KV server listening on %s\n", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}

		// Spin up a goroutine per connection — cheap in Go and keeps
		// the accept loop free to handle the next incoming client.
		go srv.handleConn(conn)
	}
}

// handleConn reads commands from one client until it disconnects.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		response := srv.dispatch(line)
		fmt.Fprintf(conn, "%s\n", response)
	}
}

// dispatch parses a command string and routes it to the right handler.
func (srv *Server) dispatch(line string) string {
	// SplitN(3) lets values contain spaces: "PUT key hello world" → ["PUT","key","hello world"]
	parts := strings.SplitN(line, " ", 3)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "PUT":
		if len(parts) < 3 {
			return "ERR usage: PUT <key> <value>"
		}
		return srv.handlePut(parts[1], parts[2])
	case "GET":
		if len(parts) < 2 {
			return "ERR usage: GET <key>"
		}
		return srv.handleGet(parts[1])
	case "DELETE":
		if len(parts) < 2 {
			return "ERR usage: DELETE <key>"
		}
		return srv.handleDelete(parts[1])
	default:
		return fmt.Sprintf("ERR unknown command %q", cmd)
	}
}

// handlePut persists and applies a PUT operation.
func (srv *Server) handlePut(key, value string) string {
	// ── Raft mode (Week 2+) ───────────────────────────────────────────────
	if srv.raftNode != nil {
		idx, _, isLeader := srv.raftNode.Submit("PUT", key, value)
		if !isLeader {
			return "ERR not leader"
		}
		// Block until the entry is committed and applied to the store.
		// Timeout after 5 seconds — generous for a local cluster.
		if !srv.raftNode.WaitForApply(idx, 5000) {
			return "ERR timeout waiting for commit"
		}
		return "OK"
	}

	// ── Single-node mode (Week 1) ─────────────────────────────────────────
	// Write to WAL FIRST, then apply to store.
	if err := srv.wal.Append(wal.Entry{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Sprintf("ERR wal append: %v", err)
	}
	srv.store.Put(key, value)
	return "OK"
}

// handleGet looks up a key. Reads never go through Raft — served locally.
func (srv *Server) handleGet(key string) string {
	val, ok := srv.store.Get(key)
	if !ok {
		return "NULL"
	}
	return val
}

// handleDelete persists and applies a DELETE operation.
func (srv *Server) handleDelete(key string) string {
	// ── Raft mode ─────────────────────────────────────────────────────────
	if srv.raftNode != nil {
		idx, _, isLeader := srv.raftNode.Submit("DELETE", key, "")
		if !isLeader {
			return "ERR not leader"
		}
		if !srv.raftNode.WaitForApply(idx, 5000) {
			return "ERR timeout waiting for commit"
		}
		return "OK"
	}

	// ── Single-node mode ──────────────────────────────────────────────────
	if err := srv.wal.Append(wal.Entry{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Sprintf("ERR wal append: %v", err)
	}
	srv.store.Delete(key)
	return "OK"
}

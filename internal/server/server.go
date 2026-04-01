// Package server implements the TCP server that clients connect to.
//
// Protocol: plain text, one command per line, newline-terminated.
//
//	PUT key value   → stores value under key, responds "OK"
//	GET key         → responds with the value, or "NULL" if not found
//	DELETE key      → removes the key, responds "OK"
//
// Each client connection is handled in its own goroutine, so many
// clients can be served concurrently without blocking each other.
//
// In Week 2, when we add Raft, writes (PUT, DELETE) will no longer
// apply directly to the store here. Instead they'll be forwarded to
// the Raft leader, which will replicate them to a majority of nodes
// before the state machine applies them. The protocol stays the same
// from the client's perspective.

// A Go dictionary that stores words mapped to other words using a mutex lock
// mutex lock: mechanism that ensures only one thread accesses a shared resoruce at a time
// prevents data corruption

// listens for incoming connectoins on a port
// each client gets its own go routine

package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"database_store/internal/store"
	"database_store/internal/wal"
)

// Server holds everything needed to accept and handle client connections.
type Server struct {
	addr  string       // TCP address to listen on, e.g. ":8080"
	store *store.Store // the in-memory state machine
	wal   *wal.WAL     // the write-ahead log for durability
}

// New creates a Server. It does not start listening yet — call Start() for that.
func New(addr string, s *store.Store, w *wal.WAL) *Server {
	return &Server{addr: addr, store: s, wal: w}
}

// Start binds to the TCP address and blocks, accepting client connections.
// Each accepted connection is handed off to handleConn in a new goroutine.
func (srv *Server) Start() error {
	ln, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.addr, err)
	}
	defer ln.Close()

	fmt.Printf("KV server listening on %s\n", srv.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Accept failing is usually fatal (e.g. listener closed).
			return fmt.Errorf("accept: %w", err)
		}

		// Spin up a goroutine per connection — cheap in Go and keeps
		// the accept loop free to handle the next incoming client.
		go srv.handleConn(conn)
	}
}

// handleConn reads commands from one client until it disconnects.
// bufio.Scanner handles the line-splitting for us.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close() // always close the connection when we're done

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		response := srv.dispatch(line)

		// Write the response back. fmt.Fprintf writes directly to the
		// TCP connection — the client reads it as a newline-terminated string.
		fmt.Fprintf(conn, "%s\n", response)
	}
	// scanner.Scan() returns false on EOF (client disconnected) or error.
	// Either way we just return, and defer closes the connection.
}

// dispatch parses a command string and routes it to the right handler.
// Returns the string that should be sent back to the client.
func (srv *Server) dispatch(line string) string {
	// SplitN with n=3 means: split into at most 3 parts.
	// This lets values contain spaces: "PUT key hello world" → ["PUT", "key", "hello world"]
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
	// Write to WAL FIRST. If we crash after this line but before
	// store.Put(), the WAL replay on restart will re-apply the PUT.
	// If we crash before this line, neither disk nor memory has the
	// value — the client will get an error and can retry.
	if err := srv.wal.Append(wal.Entry{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Sprintf("ERR wal append: %v", err)
	}
	srv.store.Put(key, value)
	return "OK"
}

// handleGet looks up a key. Reads don't touch the WAL — they're not mutations.
func (srv *Server) handleGet(key string) string {
	val, ok := srv.store.Get(key)
	if !ok {
		return "NULL"
	}
	return val
}

// handleDelete persists and applies a DELETE operation.
func (srv *Server) handleDelete(key string) string {
	// Same WAL-first discipline as PUT.
	if err := srv.wal.Append(wal.Entry{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Sprintf("ERR wal append: %v", err)
	}
	srv.store.Delete(key)
	return "OK"
}

// transport.go handles all the raw TCP + net/rpc plumbing for Raft.
//
// There are two sides:
//
//  SERVER side — this node listens for incoming RPCs from other nodes.
//                election.go and replication.go register handlers via the
//                net/rpc package (RequestVote, AppendEntries).
//
//  CLIENT side — this node dials other nodes and calls their RPC methods.
//                callRPC() is used by election.go and replication.go wherever
//                they need to send a message to a peer.
//
// We use Go's standard net/rpc package (no extra dependencies).
// net/rpc uses encoding/gob under the hood — it automatically serialises
// our RequestVoteArgs, AppendEntriesArgs, etc. structs over the wire.
package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

// rpcTimeout is the maximum time we wait for a single RPC to complete.
// It must be shorter than electionTimeoutMin so a slow peer doesn't cause
// us to miss every heartbeat and start a spurious election.
const rpcTimeout = 100 * time.Millisecond

// startRPCServer registers this Node as an RPC service and starts listening
// on listenAddr for incoming connections from peers.
//
// net/rpc automatically exposes every exported method on *Node that has the
// signature:  func (n *Node) MethodName(args *T, reply *R) error
// That covers RequestVote and AppendEntries defined in election.go and
// replication.go respectively. They are reachable as "Node.RequestVote"
// and "Node.AppendEntries".
func (n *Node) startRPCServer(listenAddr string) error {
	// Create a dedicated RPC server for this node (not the default global one).
	// This is important when running multiple nodes in one process (tests).
	srv := rpc.NewServer()
	if err := srv.Register(n); err != nil {
		return fmt.Errorf("rpc register: %w", err)
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	n.listener = ln

	// Accept loop runs in a goroutine so startRPCServer returns immediately.
	// Each accepted connection gets its own goroutine for concurrent handling.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// ln.Close() is called in Node.Stop() — that's the expected
				// shutdown path. Any other error is logged.
				return
			}
			// srv.ServeConn handles one connection: reads RPC requests,
			// dispatches them to our handler methods, writes responses.
			go srv.ServeConn(conn)
		}
	}()

	log.Printf("[raft %s] RPC server listening on %s", n.id, listenAddr)
	return nil
}

// callRPC makes an RPC call to peerID using the cached client connection.
// If no connection exists (or the previous one broke), it dials a new one.
//
// The call is wrapped in a timeout so a slow or dead peer never blocks
// the caller indefinitely.
//
// method is the RPC method name, e.g. "Node.RequestVote".
// args and reply are the request/response structs defined in rpc.go.
func (n *Node) callRPC(peerID, method string, args, reply interface{}) error {
	// ── Get (or create) a client connection ──────────────────────────────
	n.mu.Lock()
	client := n.rpcClients[peerID]
	peerAddr := n.peers[peerID]
	n.mu.Unlock()

	if client == nil {
		// No cached connection — dial the peer now.
		// rpc.Dial opens a persistent TCP connection and wraps it in an
		// RPC client. We cache it so future calls reuse the same connection.
		var err error
		client, err = rpc.Dial("tcp", peerAddr)
		if err != nil {
			// Peer is down or not yet started; this is normal during startup.
			return fmt.Errorf("dial %s (%s): %w", peerID, peerAddr, err)
		}

		n.mu.Lock()
		n.rpcClients[peerID] = client
		n.mu.Unlock()
	}

	// ── Make the call with a timeout ──────────────────────────────────────
	// client.Go starts the call asynchronously and returns a channel.
	// We then select on the channel and a timer so we don't block forever.
	call := client.Go(method, args, reply, nil)

	select {
	case result := <-call.Done:
		if result.Error != nil {
			// The call failed (e.g. connection dropped mid-call).
			// Discard the broken connection so we redial on the next attempt.
			n.mu.Lock()
			n.rpcClients[peerID] = nil
			n.mu.Unlock()
			client.Close()
			return fmt.Errorf("rpc %s→%s: %w", method, peerID, result.Error)
		}
		return nil

	case <-time.After(rpcTimeout):
		// The call took too long. Discard the connection; it may be in a bad state.
		n.mu.Lock()
		n.rpcClients[peerID] = nil
		n.mu.Unlock()
		client.Close()
		return fmt.Errorf("rpc %s→%s timed out after %s", method, peerID, rpcTimeout)
	}
}

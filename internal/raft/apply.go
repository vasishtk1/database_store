// apply.go implements the apply loop — the goroutine that bridges the Raft
// layer and the key-value store built in Week 1.
//
// ── Why is there a gap between commitIndex and lastApplied? ──────────────────
//
// Raft deliberately separates two concepts:
//
//   commitIndex  — the highest log index that a MAJORITY of nodes have.
//                  Advancing this is Raft's job (replication.go).
//
//   lastApplied  — the highest log index that has actually been RUN against
//                  the in-memory KV store. Advancing this is our job here.
//
// The gap exists so the two operations can happen asynchronously. Raft can
// commit many entries quickly while the state machine applies them one by one.
// It also means that after a crash, we can replay the log to rebuild the store
// without worrying about double-applying entries that were already committed.
//
// The flow is:
//
//   replication.go advances commitIndex
//         ↓
//   apply loop sees commitIndex > lastApplied
//         ↓
//   sends ApplyMsg on applyCh
//         ↓
//   server.go receives the ApplyMsg and calls store.Put / store.Delete
//         ↓
//   client gets its "OK" response
package raft

import (
	"log"
	"time"
)

// runApplyLoop runs forever as a background goroutine (started in node.go).
// Every 10 ms it checks whether any newly committed entries need to be
// delivered to the KV store. When it finds some, it sends them on applyCh
// in strict log order so the store always sees a consistent sequence.
//
// Why poll every 10ms instead of using a channel signal?
// Simplicity. A production system would use a sync.Cond to wake this loop
// the instant commitIndex advances, but polling is correct and much easier
// to reason about for a first implementation.
func (n *Node) runApplyLoop() {
	for {
		// Sleep first so we don't spin-loop on startup before anything is committed.
		time.Sleep(10 * time.Millisecond)

		// ── Collect all newly committed entries ───────────────────────────
		// We hold the lock only while reading state, then release it before
		// sending on applyCh. This prevents a deadlock where the channel
		// receiver (server.go) is waiting for the lock while we're blocked
		// trying to send on the channel.
		n.mu.Lock()
		var toApply []LogEntry
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			toApply = append(toApply, n.log[n.lastApplied])
		}
		n.mu.Unlock()

		// ── Deliver each entry to the KV store (outside the lock) ─────────
		for _, entry := range toApply {
			log.Printf("[raft %s] applying index=%d op=%s key=%s",
				n.id, entry.Index, entry.Op, entry.Key)

			// Send the ApplyMsg to the channel. The server.go goroutine
			// on the other end will call store.Put() or store.Delete().
			// This send blocks if the receiver is slow — that's intentional
			// back-pressure to prevent unbounded memory growth.
			n.applyCh <- ApplyMsg{
				Index: entry.Index,
				Op:    entry.Op,
				Key:   entry.Key,
				Value: entry.Value,
			}
		}
	}
}

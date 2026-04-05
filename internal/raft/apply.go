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
	"sync/atomic"
	"time"
)

// runApplyLoop runs forever as a background goroutine (started in node.go).
//
// On startup it first delivers any pending snapshot (loaded from disk or
// received via InstallSnapshot). Then it enters its normal loop: every 10 ms
// it checks whether any newly committed log entries need to be delivered to
// the KV store via applyCh.
//
// Why poll every 10ms instead of using a channel signal?
// Simplicity. A production system would use a sync.Cond to wake this loop
// the instant commitIndex advances, but polling is correct and much easier
// to reason about for a first implementation.
func (n *Node) runApplyLoop() {
	for {
		if atomic.LoadInt32(&n.shutdown) != 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)

		// ── Step 1: Deliver any pending snapshot ──────────────────────────
		// This happens once on startup (if we loaded a snapshot from disk)
		// or after an InstallSnapshot RPC arrives from the leader.
		n.mu.Lock()
		var snapMsg *ApplyMsg
		if n.pendingSnapshotApply && n.loadedSnapshot != nil {
			snapMsg = &ApplyMsg{
				IsSnapshot:    true,
				Snapshot:      n.loadedSnapshot,
				SnapshotIndex: n.snapshotIndex,
				SnapshotTerm:  n.snapshotTerm,
				Index:         n.snapshotIndex, // for WaitForApply compatibility
			}
			n.pendingSnapshotApply = false
			n.loadedSnapshot = nil
		}
		n.mu.Unlock()

		if snapMsg != nil {
			if atomic.LoadInt32(&n.shutdown) != 0 {
				return
			}
			log.Printf("[raft %s] delivering snapshot to store | index=%d",
				n.id, snapMsg.SnapshotIndex)
			// Send outside the lock to avoid deadlock with the server's receiver.
			n.applyCh <- *snapMsg
		}

		// ── Step 2: Collect all newly committed log entries ───────────────
		// Hold the lock only while reading state, release before sending
		// on applyCh. This prevents a deadlock where the channel receiver
		// (server.go) is waiting for the lock while we're blocked on send.
		n.mu.Lock()
		var toApply []LogEntry
		for n.lastApplied < n.commitIndex {
			nextIdx := n.lastApplied + 1
			arrIdx := n.arrayIdx(nextIdx)
			if arrIdx < 0 || arrIdx >= len(n.log) {
				// Do not advance lastApplied — wait until log catches up (e.g. race with compaction).
				log.Printf("[raft %s] apply loop: arrIdx %d out of range for logIdx %d (commitIndex=%d)",
					n.id, arrIdx, nextIdx, n.commitIndex)
				break
			}
			n.lastApplied = nextIdx
			toApply = append(toApply, n.log[arrIdx])
		}
		n.mu.Unlock()

		// ── Step 3: Deliver each entry to the KV store ────────────────────
		for _, entry := range toApply {
			if atomic.LoadInt32(&n.shutdown) != 0 {
				return
			}
			if entry.Index == 0 {
				continue // sentinel — never applied
			}
			log.Printf("[raft %s] applying index=%d op=%s key=%s",
				n.id, entry.Index, entry.Op, entry.Key)

			n.applyCh <- ApplyMsg{
				IsSnapshot: false,
				Index:      entry.Index,
				Op:         entry.Op,
				Key:        entry.Key,
				Value:      entry.Value,
			}
		}
	}
}

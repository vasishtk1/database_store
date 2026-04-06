// Package httpapi provides the HTTP/JSON API and serves the live dashboard.
//
// ── Routes ───────────────────────────────────────────────────────────────────
//
//	GET    /keys/{key}   → read a value from the KV store
//	PUT    /keys/{key}   → write a value (routed through Raft)
//	DELETE /keys/{key}   → delete a key (routed through Raft)
//	GET    /status       → node identity + Raft role/term
//	GET    /metrics      → full NodeMetrics + recent ops + uptime
//	GET    /             → serve the live dashboard HTML
//
// ── Operations feed ───────────────────────────────────────────────────────────
// Every GET/PUT/DELETE request is recorded in a ring buffer (last 50 ops).
// The /metrics endpoint returns them so the dashboard can show a live feed.
//
// ── CORS ─────────────────────────────────────────────────────────────────────
// All responses include Access-Control-Allow-Origin: * so the dashboard can
// be opened as a plain file:// URL during development.
package httpapi

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"database_store/internal/raft"
	"database_store/internal/store"
)

// ── Embedded dashboard ────────────────────────────────────────────────────────

//go:embed web/dashboard.html
var dashboardFS embed.FS

// ── Operation record ─────────────────────────────────────────────────────────

// Op is one entry in the recent-operations feed.
type Op struct {
	// Time is when the operation was processed, formatted as HH:MM:SS.
	Time string `json:"time"`

	// Method is GET, PUT, or DELETE.
	Method string `json:"method"`

	// Key is the key that was accessed.
	Key string `json:"key"`

	// Value is the value written (PUT only; empty for GET/DELETE).
	Value string `json:"value,omitempty"`

	// Result is "ok", "not_found", "not_leader", or "timeout".
	Result string `json:"result"`
}

// ── Handler ───────────────────────────────────────────────────────────────────

// Handler holds everything the HTTP routes need.
// Create one with New() and register it with http.ServeMux via Register().
type Handler struct {
	store    *store.Store // in-memory KV state machine
	raft     *raft.Node   // Raft node for submitting writes
	nodeID   string       // e.g. "node1"
	peerIDs  []string     // IDs of all other nodes (for /status)
	startTime time.Time   // when this node started (for uptime)

	// ops is a ring buffer of the last maxOps operations.
	opsMu sync.Mutex
	ops   []Op
}

// maxOps is the size of the operations ring buffer.
const maxOps = 50

// New creates an HTTP handler for the given node.
//
// Parameters:
//
//	s       — the in-memory KV store (reads happen here directly)
//	r       — the Raft node (writes are submitted through this)
//	nodeID  — this node's string ID, e.g. "node1"
//	peerIDs — IDs of every other node in the cluster
func New(s *store.Store, r *raft.Node, nodeID string, peerIDs []string) *Handler {
	return &Handler{
		store:     s,
		raft:      r,
		nodeID:    nodeID,
		peerIDs:   peerIDs,
		startTime: time.Now(),
		ops:       make([]Op, 0, maxOps),
	}
}

// Register wires all routes onto mux.
// Typically called with http.DefaultServeMux or a fresh http.ServeMux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/keys/", h.handleKeys)
	mux.HandleFunc("/status", h.handleStatus)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/", h.handleDashboard)
}

// ── /keys/{key} ───────────────────────────────────────────────────────────────

// handleKeys dispatches GET / PUT / DELETE on /keys/{key}.
func (h *Handler) handleKeys(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Extract the key from the URL path.
	// URL is /keys/{key} — everything after "/keys/" is the key.
	key := strings.TrimPrefix(r.URL.Path, "/keys/")
	if key == "" {
		jsonError(w, http.StatusBadRequest, "key must not be empty")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	default:
		jsonError(w, http.StatusMethodNotAllowed, "use GET, PUT, or DELETE")
	}
}

// handleGet reads a key and returns its value.
//
// Response 200: {"key":"city","value":"London","found":true}
// Response 404: {"key":"city","value":"","found":false}
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	val, found := h.store.Get(key)

	result := "ok"
	if !found {
		result = "not_found"
	}
	h.recordOp("GET", key, "", result)

	status := http.StatusOK
	if !found {
		status = http.StatusNotFound
	}
	jsonResponse(w, status, map[string]interface{}{
		"key":   key,
		"value": val,
		"found": found,
	})
}

// handlePut writes a key through Raft consensus.
//
// Request body: {"value":"London"}
// Response 200: {"ok":true}
// Response 503: {"error":"not the leader"} — client should retry another node
// Response 408: {"error":"timed out waiting for commit"}
func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// Decode the JSON body to get the value.
	var body struct {
		Value string `json:"value"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		jsonError(w, http.StatusBadRequest, "body must be JSON: {\"value\":\"...\"}")
		return
	}

	// Submit the write to Raft. If this node is not the leader, reject it.
	// Op must match server.RunApplyLoop (same as TCP protocol: PUT / DELETE).
	idx, _, isLeader := h.raft.Submit("PUT", key, body.Value)
	if !isLeader {
		h.recordOp("PUT", key, body.Value, "not_leader")
		jsonError(w, http.StatusServiceUnavailable, "not the leader — retry on another node")
		return
	}

	// Wait up to 2 seconds for the entry to be committed and applied.
	if !h.raft.WaitForApply(idx, 2000) {
		h.recordOp("PUT", key, body.Value, "timeout")
		jsonError(w, http.StatusRequestTimeout, "timed out waiting for commit")
		return
	}

	h.recordOp("PUT", key, body.Value, "ok")
	jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleDelete removes a key through Raft consensus.
//
// Response 200: {"ok":true}
// Response 503: {"error":"not the leader"}
// Response 408: {"error":"timed out waiting for commit"}
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	idx, _, isLeader := h.raft.Submit("DELETE", key, "")
	if !isLeader {
		h.recordOp("DELETE", key, "", "not_leader")
		jsonError(w, http.StatusServiceUnavailable, "not the leader — retry on another node")
		return
	}

	if !h.raft.WaitForApply(idx, 2000) {
		h.recordOp("DELETE", key, "", "timeout")
		jsonError(w, http.StatusRequestTimeout, "timed out waiting for commit")
		return
	}

	h.recordOp("DELETE", key, "", "ok")
	jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
}

// ── /status ───────────────────────────────────────────────────────────────────

// handleStatus returns a lightweight summary of this node's identity and role.
//
// Response 200:
//
//	{
//	  "node_id":   "node1",
//	  "peer_ids":  ["node2","node3"],
//	  "role":      "Leader",
//	  "term":      3,
//	  "is_leader": true,
//	  "uptime_s":  42
//	}
func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	term, isLeader := h.raft.State()
	role := "Follower"
	if isLeader {
		role = "Leader"
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"node_id":   h.nodeID,
		"peer_ids":  h.peerIDs,
		"role":      role,
		"term":      term,
		"is_leader": isLeader,
		"uptime_s":  int(time.Since(h.startTime).Seconds()),
	})
}

// ── /metrics ──────────────────────────────────────────────────────────────────

// metricsResponse is the full payload returned by /metrics.
// The dashboard polls this every 1.5 seconds.
type metricsResponse struct {
	// Raft embeds all the NodeMetrics fields at the top level.
	Raft raft.NodeMetrics `json:"raft"`

	// UptimeS is how many seconds this node has been running.
	UptimeS int `json:"uptime_s"`

	// RecentOps is the last ≤50 operations in chronological order.
	RecentOps []Op `json:"recent_ops"`
}

// handleMetrics returns the full metrics payload.
func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.opsMu.Lock()
	// Copy the ops slice so we release the lock quickly.
	opsCopy := make([]Op, len(h.ops))
	copy(opsCopy, h.ops)
	h.opsMu.Unlock()

	jsonResponse(w, http.StatusOK, metricsResponse{
		Raft:      h.raft.GetMetrics(),
		UptimeS:   int(time.Since(h.startTime).Seconds()),
		RecentOps: opsCopy,
	})
}

// ── / (dashboard) ─────────────────────────────────────────────────────────────

// handleDashboard serves the single-file HTML dashboard.
// Any path that isn't matched by a more-specific route lands here.
func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Only serve the dashboard at the root path.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data, err := dashboardFS.ReadFile("web/dashboard.html")
	if err != nil {
		// Shouldn't happen — the file is compiled in via go:embed.
		log.Printf("[http] failed to read embedded dashboard: %v", err)
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// setCORS adds permissive CORS headers so the dashboard works as a local file.
func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

// jsonResponse encodes v as JSON and writes it with the given status code.
func jsonResponse(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] encode response: %v", err)
	}
}

// jsonError writes a JSON error body: {"error": "..."}
func jsonError(w http.ResponseWriter, status int, msg string) {
	jsonResponse(w, status, map[string]string{"error": msg})
}

// recordOp appends one operation to the ring buffer.
// If the buffer is full the oldest entry is dropped (FIFO).
func (h *Handler) recordOp(method, key, value, result string) {
	h.opsMu.Lock()
	defer h.opsMu.Unlock()

	op := Op{
		Time:   fmt.Sprintf("%s", time.Now().Format("15:04:05")),
		Method: method,
		Key:    key,
		Value:  value,
		Result: result,
	}

	if len(h.ops) >= maxOps {
		// Shift left — drop the oldest entry.
		copy(h.ops, h.ops[1:])
		h.ops[len(h.ops)-1] = op
	} else {
		h.ops = append(h.ops, op)
	}
}

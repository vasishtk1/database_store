package httpapi

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"database_store/internal/raft"
	"database_store/internal/store"
)

func TestDashboardRoot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := filepath.Join(dir, "r.raft")
	applyCh := make(chan raft.ApplyMsg, 8)
	n, err := raft.New("n1", nil, db, "127.0.0.1:0", applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	h := New(store.New(), n, "n1", nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /: status %d", rec.Code)
	}
	body := rec.Body.String()
	if len(body) < 100 || rec.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("unexpected dashboard response: ct=%q len=%d", rec.Header().Get("Content-Type"), len(body))
	}
}

func TestMetricsJSONShape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := filepath.Join(dir, "r2.raft")
	applyCh := make(chan raft.ApplyMsg, 8)
	n, err := raft.New("n1", nil, db, "127.0.0.1:0", applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	h := New(store.New(), n, "n1", []string{"n2"})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q want application/json", ct)
	}
}

// Package store implements the in-memory key-value state machine.
//
// This is the core data structure the entire system is built around.
// In Week 2, when we add Raft, the Raft layer will be the ONLY thing
// that calls Put and Delete — clients will never touch the store directly.
// That's what makes it a "state machine": it only advances when Raft commits an entry.
package store

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Store is a thread-safe in-memory key-value map.
// sync.RWMutex allows many concurrent readers OR one writer at a time.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get returns the value for key and true if it exists, or ("", false) if not.
// Uses a read lock — multiple goroutines can call Get concurrently.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Put sets key to value, overwriting any previous value.
// Uses a write lock — exclusive access while the map is modified.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes key from the store. It is a no-op if key doesn't exist.
// Uses a write lock for the same reason as Put.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot serialises the entire store to a JSON byte slice.
//
// Called by the server every 100 applied entries to give Raft a compact
// representation of the current state. Raft saves this to disk and can
// ship it to followers that are too far behind to catch up via log replay.
//
// Uses a read lock so ongoing GETs are not blocked.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := json.Marshal(s.data)
	if err != nil {
		return nil, fmt.Errorf("store snapshot: %w", err)
	}
	return data, nil
}

// LoadSnapshot replaces the entire store contents with the data from a snapshot.
//
// Called by the server when Raft delivers an ApplyMsg with IsSnapshot=true.
// This happens on restart (loading from disk) or when a follower receives an
// InstallSnapshot RPC because it fell too far behind the leader.
//
// Uses a write lock because we're replacing all data at once.
func (s *Store) LoadSnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		s.data = make(map[string]string)
		return nil
	}
	newData := make(map[string]string)
	if err := json.Unmarshal(data, &newData); err != nil {
		return fmt.Errorf("store load snapshot: %w", err)
	}
	s.data = newData
	return nil
}

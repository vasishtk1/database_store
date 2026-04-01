// Package store implements the in-memory key-value state machine.
//
// This is the core data structure the entire system is built around.
// In Week 2, when we add Raft, the Raft layer will be the ONLY thing
// that calls Put and Delete — clients will never touch the store directly.
// That's what makes it a "state machine": it only advances when Raft commits an entry.
package store

import "sync"

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

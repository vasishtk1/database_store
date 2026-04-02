// the tests for verifying the correctness of the key value store


package store

import (
	"sync"
	"testing"
)

func TestGetMissing(t *testing.T) {
	s := New()
	_, ok := s.Get("nope")
	if ok {
		t.Fatal("expected missing key")
	}
}

func TestPutGet(t *testing.T) {
	s := New()
	s.Put("k", "v")
	val, ok := s.Get("k")
	if !ok || val != "v" {
		t.Fatalf("Get(k) = %q, %v; want v, true", val, ok)
	}
}

func TestPutOverwrite(t *testing.T) {
	s := New()
	s.Put("k", "a")
	s.Put("k", "b")
	val, _ := s.Get("k")
	if val != "b" {
		t.Fatalf("after overwrite Get(k) = %q; want b", val)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Put("k", "v")
	s.Delete("k")
	_, ok := s.Get("k")
	if ok {
		t.Fatal("expected key gone after Delete")
	}
}

func TestDeleteMissingNoOp(t *testing.T) {
	s := New()
	s.Delete("never-existed")
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	s := New()
	const goroutines = 32
	const iters = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if id == 0 {
					s.Put("x", "1")
					s.Delete("x")
				} else {
					s.Get("x")
				}
			}
		}(g)
	}
	wg.Wait()
}

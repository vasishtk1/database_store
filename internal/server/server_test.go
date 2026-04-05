// ensures that TCP server establishes connections, handles commands, and responds correctly

package server

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"database_store/internal/store"
	"database_store/internal/wal"
)

func sendLine(t *testing.T, addr, line string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "%s\n", line)
	sc := bufio.NewScanner(conn)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("no response line")
	}
	return sc.Text()
}

func startTestServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s := store.New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New("", s, w, nil)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = ln.Close()
		<-errCh
		_ = w.Close()
	})
	return ln.Addr().String()
}

func TestGETMissing(t *testing.T) {
	addr := startTestServer(t)
	got := sendLine(t, addr, "GET missing")
	if got != "NULL" {
		t.Fatalf("got %q want NULL", got)
	}
}

func TestPUTGET(t *testing.T) {
	addr := startTestServer(t)
	if sendLine(t, addr, "PUT mykey myval") != "OK" {
		t.Fatal("PUT")
	}
	if sendLine(t, addr, "GET mykey") != "myval" {
		t.Fatal("GET")
	}
}

func TestPUTValueWithSpaces(t *testing.T) {
	addr := startTestServer(t)
	if sendLine(t, addr, "PUT k hello world") != "OK" {
		t.Fatal("PUT")
	}
	if sendLine(t, addr, "GET k") != "hello world" {
		t.Fatal("GET value with spaces")
	}
}

func TestDELETE(t *testing.T) {
	addr := startTestServer(t)
	sendLine(t, addr, "PUT k v")
	if sendLine(t, addr, "DELETE k") != "OK" {
		t.Fatal("DELETE")
	}
	if sendLine(t, addr, "GET k") != "NULL" {
		t.Fatal("GET after delete")
	}
}

func TestUnknownCommand(t *testing.T) {
	addr := startTestServer(t)
	got := sendLine(t, addr, "NOPE x")
	want := `ERR unknown command "NOPE"`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestErrUsage(t *testing.T) {
	addr := startTestServer(t)
	cases := []struct {
		line, want string
	}{
		{"PUT", "ERR usage: PUT <key> <value>"},
		{"PUT onlykey", "ERR usage: PUT <key> <value>"},
		{"GET", "ERR usage: GET <key>"},
		{"DELETE", "ERR usage: DELETE <key>"},
	}
	for _, tc := range cases {
		got := sendLine(t, addr, tc.line)
		if got != tc.want {
			t.Errorf("%q: got %q want %q", tc.line, got, tc.want)
		}
	}
}

func TestCommandCaseInsensitive(t *testing.T) {
	addr := startTestServer(t)
	sendLine(t, addr, "put lower v")
	if sendLine(t, addr, "get lower") != "v" {
		t.Fatal("lowercase commands")
	}
}

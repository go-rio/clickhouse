package chproto

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// Cancelling a context without a deadline fails the blocked read promptly
// with the context's error and poisons the connection.
func TestCancelAbortsBlockedRead(t *testing.T) {
	c, server := pipeConn()
	defer server.Close()
	go io.Copy(io.Discard, server) // swallow the query, never answer
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	_, err := c.Query(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("query returned after %v", time.Since(start))
	}
	if !c.Broken() {
		t.Fatal("a cancelled operation must poison the connection")
	}
}

// Cancelling after an operation completed leaves the connection reusable.
func TestWatchReleasedAfterCompletion(t *testing.T) {
	c, server := pipeConn()
	defer server.Close()
	go func() {
		var b [1]byte
		server.Read(b[:])
		server.Write([]byte{serverPong})
	}()
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if c.Broken() {
		t.Fatal("a released watch must not poison the connection")
	}
}

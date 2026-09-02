package chproto

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// pipeConn builds a Conn over an in-memory pipe; the far end simulates the
// server side.
func pipeConn() (*Conn, net.Conn) {
	client, server := net.Pipe()
	return &Conn{
		netc:     client,
		r:        bufio.NewReader(client),
		w:        bufio.NewWriter(client),
		dialedAt: time.Now(),
	}, server
}

// A connection past maxLife is discarded even when recently used.
func TestPoolExpiresLifetime(t *testing.T) {
	p := NewPool(Config{}, 2, time.Minute, 0)
	c, server := pipeConn()
	c.dialedAt = time.Now().Add(-2 * time.Hour)
	p.slots <- struct{}{}
	p.Release(c)

	closed := make(chan struct{})
	go func() {
		var b [1]byte
		server.Read(b[:])
		close(closed)
	}()
	if _, err := p.Acquire(context.Background()); err == nil {
		t.Fatal("over-lifetime conn must not be served")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("over-lifetime conn was not closed")
	}
}

// An expired idle connection is discarded, not handed out.
func TestPoolExpiresIdle(t *testing.T) {
	p := NewPool(Config{}, 2, time.Millisecond, 0)
	c, server := pipeConn()
	p.slots <- struct{}{} // stand in for the Acquire that produced c
	p.Release(c)

	time.Sleep(5 * time.Millisecond)
	closed := make(chan struct{})
	go func() {
		var b [1]byte
		server.Read(b[:]) // EOF once the pool closes the expired conn
		close(closed)
	}()
	// Acquire must fall through to Dial (which fails on the empty address)
	// rather than serve the expired conn.
	if _, err := p.Acquire(context.Background()); err == nil {
		t.Fatal("expired idle conn must not be served")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("expired conn was not closed")
	}
}

// A live idle connection within maxIdle is reused.
func TestPoolReusesFreshIdle(t *testing.T) {
	p := NewPool(Config{}, 2, time.Minute, 0)
	c, _ := pipeConn()
	p.slots <- struct{}{}
	p.Release(c)
	got, err := p.Acquire(context.Background())
	if err != nil || got != c {
		t.Fatalf("got %v, %v", got, err)
	}
}

// A transmission failure surfaces as SendError.
func TestQuerySendFailureIsSendError(t *testing.T) {
	c, server := pipeConn()
	server.Close()
	_, err := c.Query(context.Background(), "SELECT 1")
	var send *SendError
	if !errors.As(err, &send) {
		t.Fatalf("want SendError, got %v", err)
	}
	if !c.Broken() {
		t.Fatal("a failed send must poison the connection")
	}
}

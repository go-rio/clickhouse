//go:build unix

package chproto

import (
	"net"
	"testing"
	"time"
)

// liveCheck flags a peer-closed TCP connection and passes a healthy one.
func TestLiveCheckDetectsPeerClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		s, _ := ln.Accept()
		accepted <- s
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted

	if err := liveCheck(client); err != nil {
		t.Fatalf("healthy connection flagged: %v", err)
	}
	server.Close()
	deadline := time.Now().Add(2 * time.Second)
	for liveCheck(client) == nil {
		if time.Now().After(deadline) {
			t.Fatal("peer close was never detected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

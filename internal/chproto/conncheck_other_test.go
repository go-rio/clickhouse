//go:build !unix

package chproto

import (
	"net"
	"testing"
)

// Without raw socket access liveCheck presumes every idle connection live,
// even one the peer has closed.
func TestLiveCheckPresumesLive(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	server.Close()
	if err := liveCheck(client); err != nil {
		t.Fatalf("stub flagged a connection: %v", err)
	}
}

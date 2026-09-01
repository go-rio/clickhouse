//go:build !unix

package chproto

import "net"

// liveCheck cannot probe without raw socket access; idle connections are
// presumed live.
func liveCheck(net.Conn) error { return nil }

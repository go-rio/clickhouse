//go:build !unix

package chproto

import "net"

func liveCheck(net.Conn) error { return nil }

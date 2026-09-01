//go:build unix

package chproto

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"syscall"
)

// liveCheck detects a peer-closed idle connection with one non-blocking
// read: EOF or stray bytes mean the connection is unusable.
func liveCheck(nc net.Conn) error {
	if tc, ok := nc.(*tls.Conn); ok {
		nc = tc.NetConn()
	}
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return nil
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var probe error
	err = raw.Read(func(fd uintptr) bool {
		var buf [1]byte
		n, rerr := syscall.Read(int(fd), buf[:])
		switch {
		case n == 0 && rerr == nil:
			probe = io.EOF
		case n > 0:
			probe = errors.New("chproto: unexpected bytes on idle connection")
		case rerr == syscall.EAGAIN || rerr == syscall.EWOULDBLOCK:
			probe = nil
		default:
			probe = rerr
		}
		return true
	})
	if err != nil {
		return err
	}
	return probe
}

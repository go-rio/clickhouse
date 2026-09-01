// Package chproto speaks the ClickHouse native TCP protocol directly: no
// driver, no third-party dependencies, no compression. The client pins
// protocol revision 54460 — the server negotiates down to it, which freezes
// every frame layout this package implements (no chunked framing, nonces, or
// password-complexity exchanges).
package chproto

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// Revision is the pinned client protocol revision.
const Revision = 54460

// client packet types
const (
	clientHello = 0
	clientQuery = 1
	clientData  = 2
	clientPing  = 4
)

// server packet types
const (
	serverHello         = 0
	serverData          = 1
	serverException     = 2
	serverProgress      = 3
	serverPong          = 4
	serverEndOfStream   = 5
	serverProfileInfo   = 6
	serverTotals        = 7
	serverExtremes      = 8
	serverLog           = 10
	serverTableColumns  = 11
	serverProfileEvents = 14
)

// Config carries what Dial needs.
type Config struct {
	Addr     string // host:port
	Database string
	User     string
	Password string
	TLS      *tls.Config // nil for plaintext
	Timeout  time.Duration
}

// Conn is one native-protocol connection. Not safe for concurrent use; the
// pool serializes access.
type Conn struct {
	netc net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	serverName string
	major      uint64
	minor      uint64
	patch      uint64
	revision   uint64 // negotiated: min(Revision, server)
	timezone   *time.Location

	// scratch buffers reused across queries
	varbuf [binary.MaxVarintLen64]byte
	broken atomic.Bool // an I/O or protocol error poisons the connection
}

// Dial connects and completes the handshake.
func Dial(ctx context.Context, cfg Config) (*Conn, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	netc, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}
	if cfg.TLS != nil {
		tc := tls.Client(netc, cfg.TLS)
		if err := tc.HandshakeContext(ctx); err != nil {
			netc.Close()
			return nil, err
		}
		netc = tc
	}
	c := &Conn{
		netc: netc,
		r:    bufio.NewReaderSize(netc, 1<<20),
		w:    bufio.NewWriterSize(netc, 256<<10),
	}
	if err := c.handshake(ctx, cfg); err != nil {
		netc.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) handshake(ctx context.Context, cfg Config) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.netc.SetDeadline(deadline)
		defer c.netc.SetDeadline(time.Time{})
	}
	c.writeUvarint(clientHello)
	c.writeString("go-rio/clickhouse")
	c.writeUvarint(0)
	c.writeUvarint(1)
	c.writeUvarint(Revision)
	c.writeString(cfg.Database)
	c.writeString(cfg.User)
	c.writeString(cfg.Password)
	if err := c.flush(); err != nil {
		return err
	}

	pt, err := c.readUvarint()
	if err != nil {
		return err
	}
	switch pt {
	case serverException:
		return c.readException()
	case serverHello:
	default:
		return fmt.Errorf("chproto: handshake: unexpected packet %d", pt)
	}
	if c.serverName, err = c.readString(); err != nil {
		return err
	}
	if c.major, err = c.readUvarint(); err != nil {
		return err
	}
	if c.minor, err = c.readUvarint(); err != nil {
		return err
	}
	srvRev, err := c.readUvarint()
	if err != nil {
		return err
	}
	c.revision = min(srvRev, Revision)
	if c.revision < Revision {
		return fmt.Errorf("chproto: server %s %d.%d speaks revision %d; rio requires ClickHouse 26+ (revision %d)",
			c.serverName, c.major, c.minor, srvRev, Revision)
	}
	tz, err := c.readString() // >= 54058, guaranteed by the floor
	if err != nil {
		return err
	}
	if c.timezone, err = time.LoadLocation(tz); err != nil {
		c.timezone = time.UTC
	}
	if _, err := c.readString(); err != nil { // display name, >= 54372
		return err
	}
	if c.patch, err = c.readUvarint(); err != nil { // >= 54401
		return err
	}
	c.writeString("") // addendum: quota key, >= 54458
	return c.flush()
}

// ServerVersion reports the negotiated server identity.
func (c *Conn) ServerVersion() string {
	return fmt.Sprintf("%s %d.%d.%d", c.serverName, c.major, c.minor, c.patch)
}

// Ping round-trips a protocol ping.
func (c *Conn) Ping(ctx context.Context) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.netc.SetDeadline(deadline)
		defer c.netc.SetDeadline(time.Time{})
	}
	c.writeUvarint(clientPing)
	if err := c.flush(); err != nil {
		return c.fail(err)
	}
	for {
		pt, err := c.readUvarint()
		if err != nil {
			return c.fail(err)
		}
		switch pt {
		case serverPong:
			return nil
		case serverProgress:
			if err := c.skipProgress(); err != nil {
				return c.fail(err)
			}
		default:
			return c.fail(fmt.Errorf("chproto: ping: unexpected packet %d", pt))
		}
	}
}

// Close tears the connection down.
func (c *Conn) Close() error { return c.netc.Close() }

// Broken reports whether an earlier error poisoned the connection.
func (c *Conn) Broken() bool { return c.broken.Load() }

// fail marks the connection unusable and returns err.
func (c *Conn) fail(err error) error {
	c.broken.Store(true)
	return err
}

// --- wire primitives ---

func (c *Conn) readUvarint() (uint64, error) {
	return binary.ReadUvarint(c.r)
}

func (c *Conn) readString() (string, error) {
	n, err := c.readUvarint()
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(c.r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

// readStringInto appends the next string's bytes to dst, returning the
// extended slice — the zero-garbage path for column payloads.
func (c *Conn) readStringInto(dst []byte) ([]byte, error) {
	n, err := c.readUvarint()
	if err != nil {
		return dst, err
	}
	off := len(dst)
	if cap(dst)-off < int(n) {
		grown := make([]byte, off, max(2*cap(dst), off+int(n), 4096))
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:off+int(n)]
	if _, err := io.ReadFull(c.r, dst[off:]); err != nil {
		return dst, err
	}
	return dst, nil
}

func (c *Conn) readFull(b []byte) error {
	_, err := io.ReadFull(c.r, b)
	return err
}

func (c *Conn) readByte() (byte, error) { return c.r.ReadByte() }

func (c *Conn) readInt32() (int32, error) {
	var b [4]byte
	if err := c.readFull(b[:]); err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b[:])), nil
}

func (c *Conn) skipN(n int) error {
	_, err := c.r.Discard(n)
	return err
}

func (c *Conn) skipString() error {
	n, err := c.readUvarint()
	if err != nil {
		return err
	}
	return c.skipN(int(n))
}

func (c *Conn) writeUvarint(v uint64) {
	n := binary.PutUvarint(c.varbuf[:], v)
	c.w.Write(c.varbuf[:n])
}

func (c *Conn) writeString(s string) {
	c.writeUvarint(uint64(len(s)))
	c.w.WriteString(s)
}

func (c *Conn) writeBytes(b []byte) {
	c.writeUvarint(uint64(len(b)))
	c.w.Write(b)
}

func (c *Conn) writeByte(b byte) { c.w.WriteByte(b) }

func (c *Conn) flush() error { return c.w.Flush() }

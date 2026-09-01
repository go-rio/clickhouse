// Package chproto speaks the ClickHouse native TCP protocol directly, with
// no third-party dependencies and no compression. The client pins protocol
// revision 54460; the server negotiates down to it, freezing every frame
// layout implemented here.
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

// Conn is one native-protocol connection; not safe for concurrent use.
type Conn struct {
	netc     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	dialedAt time.Time

	timezone *time.Location
	ctx      context.Context // watched context of the operation in flight

	// scratch reused across queries
	rows    *Rows
	varbuf  [binary.MaxVarintLen64]byte
	typeBuf []byte      // type-string scratch
	broken  atomic.Bool // an I/O or protocol error poisons the connection
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
		netc:     netc,
		r:        bufio.NewReaderSize(netc, 1<<20),
		w:        bufio.NewWriterSize(netc, 256<<10),
		dialedAt: time.Now(),
	}
	if err := c.handshake(ctx, cfg); err != nil {
		netc.Close()
		return nil, err
	}
	return c, nil
}

// watch arms ctx on the connection: its deadline bounds every read and write,
// and its cancellation aborts them. stop releases the watch.
func (c *Conn) watch(ctx context.Context) (stop func() bool) {
	deadline, _ := ctx.Deadline()
	c.netc.SetDeadline(deadline)
	c.ctx = ctx
	return context.AfterFunc(ctx, c.abort)
}

// abort fails the I/O in flight and poisons the connection.
func (c *Conn) abort() {
	c.broken.Store(true)
	c.netc.SetDeadline(time.Unix(1, 0))
}

func (c *Conn) handshake(ctx context.Context, cfg Config) error {
	defer c.watch(ctx)()
	defer c.netc.SetDeadline(time.Time{})
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
	name, err := c.readString()
	if err != nil {
		return err
	}
	major, err := c.readUvarint()
	if err != nil {
		return err
	}
	minor, err := c.readUvarint()
	if err != nil {
		return err
	}
	srvRev, err := c.readUvarint()
	if err != nil {
		return err
	}
	if srvRev < Revision {
		return fmt.Errorf("chproto: server %s %d.%d speaks revision %d; rio requires ClickHouse 26+ (revision %d)",
			name, major, minor, srvRev, Revision)
	}
	tz, err := c.readString() // >= 54058, guaranteed by the floor
	if err != nil {
		return err
	}
	if c.timezone, err = time.LoadLocation(tz); err != nil {
		c.timezone = time.UTC
	}
	if err := c.skipString(); err != nil { // display name, >= 54372
		return err
	}
	if _, err := c.readUvarint(); err != nil { // version patch, >= 54401
		return err
	}
	c.writeString("") // addendum: quota key, >= 54458
	return c.flush()
}

// Ping round-trips a protocol ping.
func (c *Conn) Ping(ctx context.Context) error {
	defer c.watch(ctx)()
	c.writeUvarint(clientPing)
	if err := c.flush(); err != nil {
		return c.fail(err)
	}
	pt, err := c.nextPacket()
	if err != nil {
		return err
	}
	if pt != serverPong {
		return c.fail(fmt.Errorf("chproto: ping: unexpected packet %d", pt))
	}
	return nil
}

// Close tears the connection down.
func (c *Conn) Close() error { return c.netc.Close() }

// Broken reports whether an earlier error poisoned the connection.
func (c *Conn) Broken() bool { return c.broken.Load() }

// fail marks the connection unusable and returns err, or the watched
// context's error once that context is done.
func (c *Conn) fail(err error) error {
	c.broken.Store(true)
	if cerr := c.ctx.Err(); cerr != nil {
		return cerr
	}
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
// extended slice.
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

func (c *Conn) writeByte(b byte) { c.w.WriteByte(b) }

func (c *Conn) flush() error { return c.w.Flush() }

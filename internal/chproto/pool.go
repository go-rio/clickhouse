package chproto

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("chproto: pool is closed")

// Pool hands out connections one query at a time. Broken connections are
// discarded on release; idle ones expire after maxIdle, every connection
// after maxLife.
type Pool struct {
	cfg     Config
	maxIdle time.Duration
	maxLife time.Duration
	slots   chan struct{} // capacity = max open

	mu     sync.Mutex
	idle   []idleConn // oldest first
	closed bool
}

type idleConn struct {
	c     *Conn
	since time.Time
}

// NewPool creates a pool of at most maxOpen connections that dial lazily.
// maxOpen defaults to 8, maxIdle to five minutes, maxLife to one hour.
func NewPool(cfg Config, maxOpen int, maxIdle, maxLife time.Duration) *Pool {
	if maxOpen <= 0 {
		maxOpen = 8
	}
	if maxIdle <= 0 {
		maxIdle = 5 * time.Minute
	}
	if maxLife <= 0 {
		maxLife = time.Hour
	}
	return &Pool{cfg: cfg, maxIdle: maxIdle, maxLife: maxLife, slots: make(chan struct{}, maxOpen)}
}

// Acquire returns a connection, dialing when no live idle one exists.
// Idle candidates are probed for a peer close before reuse.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	now := time.Now()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			<-p.slots
			return nil, ErrPoolClosed
		}
		var expired []*Conn
		for len(p.idle) > 0 && now.Sub(p.idle[0].since) > p.maxIdle {
			expired = append(expired, p.idle[0].c)
			p.idle = p.idle[1:]
		}
		var c *Conn
		if n := len(p.idle); n > 0 {
			c = p.idle[n-1].c
			p.idle = p.idle[:n-1]
		}
		p.mu.Unlock()
		for _, e := range expired {
			e.Close()
		}
		if c == nil {
			break
		}
		if now.Sub(c.dialedAt) >= p.maxLife || liveCheck(c.netc) != nil {
			c.Close()
			continue
		}
		return c, nil
	}
	c, err := Dial(ctx, p.cfg)
	if err != nil {
		<-p.slots
		return nil, err
	}
	return c, nil
}

// Release returns a connection to the pool; broken ones are closed.
func (p *Pool) Release(c *Conn) {
	p.mu.Lock()
	dead := c.Broken() || p.closed
	if !dead {
		p.idle = append(p.idle, idleConn{c: c, since: time.Now()})
	}
	p.mu.Unlock()
	if dead {
		c.Close()
	}
	<-p.slots
}

// Close closes idle connections and fails future Acquires. In-flight
// connections close on their Release.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, ic := range idle {
		ic.c.Close()
	}
	return nil
}

// Ping verifies connectivity.
func (p *Pool) Ping(ctx context.Context) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer p.Release(c)
	return c.Ping(ctx)
}

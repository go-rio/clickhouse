package chproto

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("chproto: pool is closed")

// Pool hands out connections one query at a time. Broken connections are
// discarded on release; there is no background health checking — a stale
// idle connection surfaces its error on first use and the caller retries.
type Pool struct {
	cfg   Config
	slots chan struct{} // capacity = max open

	mu     sync.Mutex
	idle   []*Conn
	closed bool
}

// NewPool creates a pool of at most maxOpen connections; connections dial
// lazily.
func NewPool(cfg Config, maxOpen int) *Pool {
	if maxOpen <= 0 {
		maxOpen = 8
	}
	return &Pool{cfg: cfg, slots: make(chan struct{}, maxOpen)}
}

// Acquire returns a connection, dialing when no idle one exists.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, ErrPoolClosed
	}
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
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
		p.idle = append(p.idle, c)
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
	for _, c := range idle {
		c.Close()
	}
	return nil
}

// Ping verifies connectivity by acquiring a connection and pinging it.
func (p *Pool) Ping(ctx context.Context) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer p.Release(c)
	return c.Ping(ctx)
}

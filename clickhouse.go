// Package clickhouse connects rio to ClickHouse over its native TCP
// protocol, implemented in-repo with no third-party dependencies.
//
// ClickHouse 26.7 or newer is required for rio's offset-carrying time
// values. No constraint-error translator is installed (ClickHouse has no
// unique or foreign key constraints); server errors remain reachable as
// *clickhouse.Exception through errors.As.
package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-rio/clickhouse/internal/chproto"
	"github.com/go-rio/rio"
)

// Exception is a ClickHouse server error.
type Exception = chproto.Exception

// Open connects the native channel and verifies it with a ping. The DSN is
// clickhouse://user:password@host:port/database with optional parameters
// secure, skip_verify, dial_timeout, max_open_conns, and
// conn_max_idle_time.
//
// The returned DB's Unwrap serves a database/sql view over its own
// connections (what go-rio/migrate consumes); rio itself executes on the
// protocol directly.
func Open(ctx context.Context, dsn string, opts ...rio.Option) (*rio.DB, error) {
	cfg, po, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	pool := chproto.NewPool(cfg, po.maxOpen, po.maxIdle)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	view := sql.OpenDB(shimConnector{cfg: cfg})
	nd := &nativeDB{pool: pool}
	return rio.NewNative(rio.NativeConfig{DB: nd, Handle: pool, SQLView: view}, rio.ClickHouse, opts...), nil
}

// poolOptions carries the DSN's pool tuning.
type poolOptions struct {
	maxOpen int
	maxIdle time.Duration
}

// parseDSN interprets clickhouse:// URLs.
func parseDSN(dsn string) (chproto.Config, poolOptions, error) {
	var po poolOptions
	u, err := url.Parse(dsn)
	if err != nil {
		return chproto.Config{}, po, fmt.Errorf("clickhouse: bad DSN: %w", err)
	}
	if u.Scheme != "clickhouse" {
		return chproto.Config{}, po, fmt.Errorf("clickhouse: bad DSN scheme %q (want clickhouse://)", u.Scheme)
	}
	cfg := chproto.Config{
		Addr:     u.Host,
		Database: strings.TrimPrefix(u.Path, "/"),
		Timeout:  10 * time.Second,
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	if cfg.User == "" {
		cfg.User = "default"
	}
	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		cfg.Addr = net.JoinHostPort(u.Host, "9000")
	}
	var secure, skipVerify bool
	for key, vals := range u.Query() {
		val := vals[len(vals)-1]
		var err error
		switch key {
		case "username":
			cfg.User = val
		case "password":
			cfg.Password = val
		case "database":
			cfg.Database = val
		case "secure":
			secure, err = strconv.ParseBool(val)
		case "skip_verify":
			skipVerify, err = strconv.ParseBool(val)
		case "dial_timeout":
			cfg.Timeout, err = time.ParseDuration(val)
		case "max_open_conns":
			po.maxOpen, err = strconv.Atoi(val)
		case "conn_max_idle_time":
			po.maxIdle, err = time.ParseDuration(val)
		default:
			return chproto.Config{}, po, fmt.Errorf(
				"clickhouse: unsupported DSN parameter %q (supported: username, password, database, secure, skip_verify, dial_timeout, max_open_conns, conn_max_idle_time)", key)
		}
		if err != nil {
			return chproto.Config{}, po, fmt.Errorf("clickhouse: bad DSN parameter %s=%q: %w", key, val, err)
		}
	}
	if secure {
		host, _, _ := net.SplitHostPort(cfg.Addr)
		cfg.TLS = &tls.Config{ServerName: host, InsecureSkipVerify: skipVerify}
	}
	return cfg, po, nil
}

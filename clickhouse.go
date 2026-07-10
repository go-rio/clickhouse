// Package clickhouse connects rio, the zero-surprise Go ORM, to ClickHouse
// through the official clickhouse-go v2 driver's database/sql interface.
//
// Driver modules are deliberately thin. This package contains exactly two
// things, and nothing else:
//
//   - Constructors: Open (from a DSN) and New (bring your own *sql.DB), both
//     returning a *rio.DB speaking the built-in rio.ClickHouse dialect.
//   - Eager DSN validation: Open parses the DSN with clickhouse.ParseDSN so a
//     malformed one fails at startup instead of on the first query. The DSN
//     is then handed to the driver untouched — no parameter is ever pinned or
//     rewritten, because ClickHouse has none that rio's correctness depends
//     on (time encoding carries its own offset, see the rio docs).
//
// The supported server floor is ClickHouse 26.0+: that is where INSERT and
// comparisons natively parse rio's offset-carrying time text — on earlier
// servers a time.Time in a query condition fails with TYPE_MISMATCH (see
// the README's Requirements section).
//
// Unlike the other go-rio driver modules there is no error translator here,
// and that is a documented dialect fact, not a gap: ClickHouse has no unique
// constraints and no foreign keys, so no server error honestly maps to
// rio.ErrDuplicateKey or rio.ErrForeignKeyViolated — they can never happen on
// this dialect. Server errors reach you as *clickhouse.Exception via
// errors.As, code and message intact.
//
// All SQL grammar — including which rio APIs the dialect supports and which
// it rejects with an explanation — lives in github.com/go-rio/rio; this
// package never implements a dialect.
package clickhouse

import (
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-rio/rio"
)

// driverName is the name clickhouse-go registers with database/sql.
const driverName = "clickhouse"

// Open opens a ClickHouse database via clickhouse-go's database/sql driver
// and wraps it in a *rio.DB speaking the built-in rio.ClickHouse dialect.
//
// Both DSN forms clickhouse-go accepts work: the native protocol
// (clickhouse://user:pass@host:9000/db, multiple hosts comma-separated) and
// the HTTP protocol (http:// or https://). Every driver parameter —
// dial_timeout, read_timeout, compress, secure, and the rest — passes
// through untouched.
//
// Open validates the DSN eagerly with clickhouse.ParseDSN but does not
// connect; ping the underlying pool (db.Unwrap().PingContext) to verify
// connectivity. Pool tuning also happens on the *sql.DB returned by Unwrap —
// rio never replaces or configures the connection pool.
func Open(dsn string, opts ...rio.Option) (*rio.DB, error) {
	if _, err := clickhouse.ParseDSN(dsn); err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return New(db, opts...), nil
}

// New wraps an existing *sql.DB in a *rio.DB with the ClickHouse dialect.
// Use it when you bring your own pool — most commonly one built
// programmatically for TLS or custom auth:
//
//	sqlDB := clickhouse.OpenDB(&clickhouse.Options{...}) // clickhouse-go v2
//	db := rioch.New(sqlDB)
//
// New installs no error translator (see the package documentation for why
// there is nothing to translate on ClickHouse).
func New(db *sql.DB, opts ...rio.Option) *rio.DB {
	return rio.New(db, rio.ClickHouse, opts...)
}

// Package clickhouse connects rio to ClickHouse through clickhouse-go v2's
// database/sql driver.
//
// ClickHouse 26.7 or newer is required for rio's offset-carrying time values.
// No constraint-error translator is installed (ClickHouse has no unique or
// foreign key constraints); server errors remain reachable as
// *clickhouse.Exception through errors.As.
package clickhouse

import (
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-rio/rio"
)

const driverName = "clickhouse"

// Open validates a ClickHouse DSN and wraps the resulting database/sql pool.
// The DSN passes through unchanged; it does not connect (ping via
// db.Unwrap().PingContext).
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

// New wraps an existing *sql.DB with the ClickHouse dialect. It installs no
// constraint-error translator; see the package documentation.
func New(db *sql.DB, opts ...rio.Option) *rio.DB {
	return rio.New(db, rio.ClickHouse, opts...)
}

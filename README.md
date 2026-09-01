# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver for [rio](https://github.com/go-rio/rio), speaking the
native TCP protocol directly. The wire protocol lives in this module, so
`go.mod` requires only `go-rio/rio`.

```go
import rioch "github.com/go-rio/clickhouse"

db, err := rioch.Open(ctx, "clickhouse://default:secret@localhost:9000/analytics")
if err != nil {
	return err
}
defer db.Close()

events, err := rio.From[Event]().Where("kind = ?", "click").All(ctx, db)
```

## Getting started

```sh
go get github.com/go-rio/clickhouse
```

```go
package main

import (
	"context"
	"log"
	"time"

	rioch "github.com/go-rio/clickhouse"
	"github.com/go-rio/rio"
)

// CREATE TABLE events (id UInt64, kind String, at DateTime64(6, 'UTC'))
// ENGINE = MergeTree ORDER BY id
type Event struct {
	ID   uint64 `rio:",pk,noautoincr"` // ClickHouse never backfills generated values
	Kind string
	At   time.Time
}

func main() {
	ctx := context.Background()
	db, err := rioch.Open(ctx, "clickhouse://default:secret@localhost:9000/analytics")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := rio.Insert(ctx, db, &Event{ID: 1, Kind: "click", At: time.Now()}); err != nil {
		log.Fatal(err)
	}
	events, err := rio.From[Event]().Where("kind = ?", "click").OrderBy("at").All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d click events", len(events))
}
```

## Features

### Native protocol, zero dependencies

- The wire protocol is implemented in-repo; `go.mod` requires only
  `go-rio/rio`.
- ClickHouse 26+: the client pins protocol revision 54460 and the server
  negotiates down to it. Connections are uncompressed. CI tests against
  ClickHouse 26.7.
- Column blocks decode straight into rio's typed sinks and `InsertAll`
  streams native column blocks: ~2× faster reads and ~3.7× faster bulk
  inserts than going through `clickhouse-go`, at one allocation per row
  read.

### DSN

`clickhouse://user:password@host:port/database` plus:

| Parameter | Meaning |
|---|---|
| `username`, `password`, `database` | alternatives to the URL fields |
| `secure` | TLS (`true`/`false`) |
| `skip_verify` | skip TLS certificate verification |
| `dial_timeout` | connect timeout, e.g. `5s` (default `10s`) |
| `max_open_conns` | pool size (default 8) |
| `conn_max_idle_time` | idle connection expiry, e.g. `90s` (default `5m`) |
| `conn_max_lifetime` | connection age limit (default `1h`) |

The port defaults to `9000`, the user and database to `default`. Unknown
parameters are rejected.

### Supported column types

Integers of every width plus `Int128`/`UInt128` (as decimal text),
`Float32/64`, `Decimal` up to 38 digits (as fixed-scale text), `Bool`,
`String`, `FixedString`, `Enum8/16`, `UUID`, `IPv4`/`IPv6`, `Date`,
`Date32`, `DateTime`, `DateTime64`, `SimpleAggregateFunction` of the above,
and `Nullable` of each. `LowCardinality` columns read and write as their
plain type. `Array`, `Map`, `Tuple`, and `Decimal256` are not supported.

### rio on ClickHouse

Reads support the full builder, relations, and Raw. Writes are `Insert`,
`InsertAll`, and `Exec`; rio rejects transactions, row locks, synchronous
update/delete, conflict upserts, and statement caching at the dialect
level. Generated values are never backfilled — supply IDs yourself
(`rio:",noautoincr"`).

Server errors are `*clickhouse.Exception` via `errors.As`, carrying the
server's `Code`, `Name`, and `Message`. There is no constraint-error
translator: ClickHouse has no unique or foreign key constraints.

### Parameters

Parameters bind client-side: each `?` is replaced by a literal under
ClickHouse quoting rules (`'...'` strings with `\` escapes, backquoted and
double-quoted identifiers, and `--` and `/* */` comments are left alone;
`\?` renders a literal question mark).

- `time.Time` renders as `toDateTime64('YYYY-MM-DD HH:MM:SS.ffffff', 6, 'UTC')`,
  which the VALUES parser casts into every date/time column type and the
  primary-key analyzer folds, so range predicates on sorting-key time
  columns work, including pre-1970 instants.
- Every query runs with `date_time_overflow_behavior = 'throw'`, so an
  instant that does not fit a `Date` or `DateTime` column fails instead
  of wrapping.
- `uint64` values bind as-is, including values above `math.MaxInt64`.
- `float32` values interpolate at 32-bit precision; NaN and infinities
  are rejected.

### Range guards on the block insert path

`InsertAll` encodes values into native column blocks typed by the server's
schema sample:

- Integer columns range-check by width and sign instead of wrapping; a
  `uint64` with the high bit set passes into `UInt64` and `UInt128`, is
  rejected for `Int64`, and converts without sign extension into float
  columns.
- `Date` accepts `[1970-01-01, 2149-06-06]` and `DateTime`
  `[1970-01-01, 2106-02-07]`; instants outside those ranges — the zero
  `time.Time` is the usual offender — fail the insert instead of wrapping.
- `Date32` floors pre-1970 instants to the previous day. `DateTime64`
  covers the server's whole range (1900–2299) at precisions below 9.
- A value that does not fit its column fails the insert, and the
  connection is discarded.

### Connections and cancellation

- Each connection runs one query at a time. The pool dials lazily up to
  `max_open_conns`, expires idle connections after `conn_max_idle_time`,
  and replaces every connection after `conn_max_lifetime`. Idle
  connections are probed for a peer close before reuse, so a server
  restart or a load-balancer idle kill does not surface as an EOF on the
  next query.
- A failure while transmitting a query — the server cannot have acted on
  it — redials once. The database/sql shim reports the same failure as
  `driver.ErrBadConn`, so database/sql retries it.
- Every operation honors its context: a deadline bounds every read and
  write, and cancellation aborts the in-flight I/O immediately, surfaces
  as the context error, and discards the connection.
- A server exception leaves its connection reusable; I/O and protocol
  errors poison it.

### database/sql

`OpenSQL` returns a plain `*sql.DB` over the same protocol — what
[go-rio/migrate](https://github.com/go-rio/migrate) consumes, also available
as `db.Unwrap()`. Placeholders are `?`; no transactions, no prepared
statements, affected-row counts are always zero.

```go
sqlDB, err := rioch.OpenSQL(dsn)
m, err := migrate.New(sqlDB, migrate.ClickHouse)
```

## Contributing

Please read the [contributing guide](CONTRIBUTING.md) before opening a
pull request.

## Contributors

Thanks to everyone in the [contributors graph](https://github.com/go-rio/clickhouse/graphs/contributors).

## License

Released under the [MIT License](LICENSE), © 2026-now TreeNewBee.

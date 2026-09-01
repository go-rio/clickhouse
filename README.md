# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver for [rio](https://github.com/go-rio/rio), speaking the
native TCP protocol directly.

- **Zero third-party dependencies** — the wire protocol is implemented
  in-repo; `go.mod` requires only `go-rio/rio`.
- **Fast** — column blocks decode straight into rio's typed sinks and
  `InsertAll` streams native column blocks: ~2× faster reads and ~3.7×
  faster bulk inserts than going through `clickhouse-go`, at one allocation
  per row read.
- **ClickHouse 26+**, protocol revision 54460, uncompressed.

## Getting started

```sh
go get github.com/go-rio/clickhouse
```

```go
import rioch "github.com/go-rio/clickhouse"

db, err := rioch.Open(ctx, "clickhouse://default:secret@localhost:9000/analytics")
if err != nil {
	return err
}
defer db.Close()

events, err := rio.From[Event]().Where("kind = ?", "click").All(ctx, db)
```

## DSN

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

Unknown parameters are rejected.

## Supported column types

Integers of every width plus `Int128`/`UInt128` (as decimal text),
`Float32/64`, `Decimal` up to 38 digits (as fixed-scale text), `Bool`,
`String`, `FixedString`, `Enum8/16`, `UUID`, `IPv4`/`IPv6`, `Date`,
`Date32`, `DateTime`, `DateTime64`, `SimpleAggregateFunction` of the above,
and `Nullable` of each. `LowCardinality` columns read and write as their
plain type. `Array`, `Map`, and `Tuple` are not supported.

## rio on ClickHouse

Reads support the full builder, relations, and Raw. Writes are `Insert`,
`InsertAll`, and `Exec`; rio rejects transactions, row locks, synchronous
update/delete, conflict upserts, and statement caching at the dialect
level. Generated values are never backfilled — supply IDs yourself
(`rio:",noautoincr"`). Parameters bind client-side. Every query runs with
`date_time_overflow_behavior = 'throw'`, so an instant that does not fit a
`Date` or `DateTime` column fails instead of wrapping.

Server errors are `*clickhouse.Exception` via `errors.As`. There is no
constraint-error translator: ClickHouse has no unique or foreign key
constraints.

## database/sql

`OpenSQL` returns a plain `*sql.DB` over the same protocol — what
[go-rio/migrate](https://github.com/go-rio/migrate) consumes, also available
as `db.Unwrap()`. Placeholders are `?`; no transactions, no prepared
statements, affected-row counts are always zero.

```go
sqlDB, err := rioch.OpenSQL(dsn)
m, err := migrate.New(sqlDB, migrate.ClickHouse)
```

## License

Released under the [MIT License](LICENSE), © 2026-now TreeNewBee.

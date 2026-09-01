# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver module for [rio](https://github.com/go-rio/rio),
implementing the ClickHouse native TCP protocol in-repo with zero
third-party dependencies. SQL rendering and capability checks live in rio;
this module owns the wire.

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

DSN parameters: `username`, `password`, `database`, `secure`, `skip_verify`,
`dial_timeout`, `max_open_conns`. Unknown parameters are rejected. The
protocol runs uncompressed.

## The native protocol

The module speaks ClickHouse's native TCP protocol directly — no driver, no
third-party dependencies. The client pins protocol revision 54460 and the
server negotiates down to it, freezing every frame layout it implements.
ClickHouse 26+ is the supported floor.

- Result blocks decode through typed per-column readers into rio's
  `NativeCell` sinks; buffers are reused across blocks.
- `InsertAll` streams native column blocks (`NativeCopier`) — ClickHouse
  never backfills, so every batch takes this path. A background reader
  drains the response during the stream, so a server abort surfaces instead
  of deadlocking.
- Parameters interpolate client-side under ClickHouse quoting rules, as
  rio's ClickHouse channel always has.
- Server errors surface as `*clickhouse.Exception` via `errors.As`. No
  constraint-error translator exists: ClickHouse has no unique or foreign
  key constraints.

Column types: integers, floats, `Bool`, `String`, `FixedString`, `Enum8/16`,
`UUID`, `Date`, `Date32`, `DateTime`, `DateTime64`, and `Nullable` of each.
`Array`/`Map`/`Tuple` are rejected; `LowCardinality` columns arrive as their
plain type (the channel sets `low_cardinality_allow_in_native_format=0`).

## database/sql

`OpenSQL` serves a plain `database/sql` handle over the same protocol — the
surface [go-rio/migrate](https://github.com/go-rio/migrate) consumes, also
reachable as `db.Unwrap()`. Placeholders are `?`; there are no transactions
or prepared statements, and affected-row counts are always zero, matching
ClickHouse itself.

## rio semantics on ClickHouse

Reads support the full builder, relations, and Raw. Writes are `Insert`,
`InsertAll`, and explicit `Exec`; rio rejects transactions, row locks,
synchronous update/delete, conflict upserts, and statement caching at the
dialect level. Generated values are never backfilled — supply IDs yourself
(`rio:",noautoincr"`).

## License

Released under the [MIT License](LICENSE), © 2026-now TreeNewBee.

# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver module for [rio](https://github.com/go-rio/rio), using
[clickhouse-go v2](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)
through `database/sql`. The module provides `Open` and `New`; SQL rendering
and capability checks live in rio.

## Getting started

```sh
go get github.com/go-rio/clickhouse
```

```go
import rioch "github.com/go-rio/clickhouse"

db, err := rioch.Open("clickhouse://default@localhost:9000/analytics?compress=lz4")
if err != nil {
	return err
}
defer db.Close()

events, err := rio.From[Event]().Where("kind = ?", "click").All(ctx, db)
```

`Open` validates the DSN but does not connect; ping with
`db.Unwrap().PingContext(ctx)`. Native (`clickhouse://`) and HTTP
(`http://`/`https://`) DSNs pass to clickhouse-go unchanged. `New` wraps an
existing `*sql.DB`, such as one from `clickhouse.OpenDB`.

## Capabilities

ClickHouse is a read-and-append rio dialect.

| Area | Supported |
|---|---|
| Reads | `From`, `Find`, `First`, `Sole`, `All`, `Rows`, `Count`, `Exists`, `Query.Pluck`, scopes, joins, grouping, ordering, limits and offsets |
| Relations | `With`, `WithCount`, `WhereHas`, `WhereHasNot`, `RelWhere`, `RelOrder`, `RelLimit`, `RelWithTrashed` |
| Soft-delete reads | Default filtering, `WithTrashed`, `OnlyTrashed` |
| Writes | `Insert`, `InsertAll`; ClickHouse returns no generated IDs and rio does not backfill rows |
| ClickHouse extras | `Query.Final()` adds the `FINAL` table modifier |
| Escape hatches | `Raw`, `Exec`, query hooks, `Must`/`Validate`, `Unwrap` |

Unsupported APIs fail with a ClickHouse-specific alternative in the error:

| APIs | Use instead |
|---|---|
| `Update`, `UpdateAll` | `rio.Exec` with `ALTER TABLE ... UPDATE`, or append a new `ReplacingMergeTree` version |
| `Delete`, `DeleteAll`, `ForceDelete`, `ForceDeleteAll` | `rio.Exec` with `DELETE FROM` or `ALTER TABLE ... DELETE` |
| `Restore`, `RestoreAll` | `rio.Exec` with `ALTER TABLE ... UPDATE` |
| `Upsert`, `UpsertAll`, `FirstOrCreate`, `CreateOrFirst` | Append a new `ReplacingMergeTree` version and read with `Final()`; ClickHouse has no unique constraint to arbitrate the race |
| `db.Tx`, `TxWith` | One `InsertAll` per atomic statement, a clickhouse-go batch through `Unwrap`, or a separate native connection |
| `Attach`, `Detach`, `SyncRelation` | Manage the join table with `rio.Exec` or append-only rows |
| `ForUpdate` | Remove it; ClickHouse has no row locks |
| `rio.WithStmtCache` | Leave it disabled; clickhouse-go prepares only INSERT batches |

Assign IDs before `Insert` — ClickHouse cannot generate them — or tag the
field `rio:",noautoincr"` when zero is a valid stored value.

Mutations issued with `rio.Exec` are asynchronous unless the statement adds
`SETTINGS mutations_sync = 1` (`2` waits for all replicas). clickhouse-go
reports zero affected rows, and `LastInsertId` is unavailable.

## `FINAL` and `ReplacingMergeTree`

With `ReplacingMergeTree(version)`, update a row by incrementing its version
and inserting it again; read merged rows with `Final()`:

```go
p.Version++
p.Name = "new name"
err := rio.Insert(ctx, db, &p)

profiles, err := rio.From[Profile]().Final().All(ctx, db)
```

`Final()` affects only the query's main SELECT, including `Count`, `Exists`,
and `Pluck` — not preloads, `WithCount`, or `WhereHas` subqueries.
Non-ClickHouse dialects reject it; an engine without versioned merges may
return `ILLEGAL_FINAL`. Without `FINAL`, a read may observe any unmerged
version.

## Placeholders

Use `?` arguments for values in `Where`, `Raw`, and `Exec`. rio validates
placeholder arity; clickhouse-go interpolates the values into the SQL sent to
the server. Runtime slices expand in `IN (?)`.

- clickhouse-go v2.48.0+ protects `?` inside quoted regions and supported
  comments. rio rejects `?` in `$tag$...$tag$` heredocs and `//` comments,
  which the driver does not protect — bind the value, or use a quoted
  string and a `--` comment.
- Write `??` for a literal question mark; rio emits clickhouse-go's `\?` escape.
- Interpolated values can appear in server query logs, and large `IN (?)`
  expansions count against `max_query_size`. rio chunks relation preload
  keys automatically; chunk application queries yourself.

## Time and bytes

rio normalizes `time.Time` to UTC at microsecond precision and binds text
with an explicit offset, so values round-trip through `DateTime64(6)` as the
same instant regardless of the column's timezone. `DateTime64(3)` and
`DateTime` truncate to their schema precision; `Date` and `Date32` reject
this binding. rio accepts years 0001–9999 (ClickHouse 26.7's `DateTime64(6)`
text range), including zero `time.Time`, and rejects values outside it. Use
`*time.Time` with `Nullable(DateTime64(6))` when the value is absent.

`[]byte`, `json.RawMessage`, and other named byte slices bind as one
ClickHouse `String`, not `Array(UInt8)`; a typed nil slice binds SQL `NULL`.
Types implementing `driver.Valuer` keep control of their encoding.

## Bulk and native access

`InsertAll` emits chunked multi-value INSERT statements. For larger loads,
use `db.Unwrap()` and clickhouse-go's database/sql batch sequence
(`Begin` → `Prepare` → repeated `Exec` → `Commit`) — a batch shim, not a
transaction, which is why rio rejects `db.Tx`. `Unwrap` returns the
`*sql.DB` and exposes its pool tuning. For `PrepareBatch`, columnar append,
or native scans, open a separate `clickhouse.Conn` with `clickhouse.Open`.

## Error semantics

The module installs no constraint-error translator because ClickHouse has no
unique or foreign key constraints, so `rio.ErrDuplicateKey` and
`rio.ErrForeignKeyViolated` never match. Server errors stay in the chain as
`*clickhouse.Exception`, reachable with `errors.As`.

## Requirements

| Component | Minimum | Reason |
|---|---|---|
| Go | **1.27.0** | Module language version |
| ClickHouse server | **26.7** | Extended DateTime64 range and offset-carrying time text |
| clickhouse-go | **v2.48.0** | Quote-aware client-side placeholder binding |

## Contributing

Use Go 1.27 or newer, then run `go test ./...`, `go test -race ./...`, and
`go vet ./...` before opening a pull request.

## License

[MIT](LICENSE)

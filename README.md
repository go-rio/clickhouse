# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver module for [rio](https://github.com/go-rio/rio), using
[clickhouse-go v2](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)
through `database/sql`. This module provides `Open` and `New`; SQL rendering
and capability checks live in rio.

## Getting started

```sh
go get github.com/go-rio/clickhouse
```

```go
import (
	rioch "github.com/go-rio/clickhouse"
	"github.com/go-rio/rio"
)

db, err := rioch.Open("clickhouse://default@localhost:9000/analytics?compress=lz4")
if err != nil {
	return err
}
defer db.Close()

events, err := rio.From[Event]().
	Where("kind = ?", "click").
	All(ctx, db)
```

`Open` validates the DSN but does not connect. Use
`db.Unwrap().PingContext(ctx)` when startup must verify connectivity. Native
(`clickhouse://`) and HTTP (`http://` or `https://`) DSNs are accepted and
passed to clickhouse-go unchanged.

Use `New` with an existing `*sql.DB`:

```go
sqlDB := clickhouse.OpenDB(&clickhouse.Options{/* TLS, auth, pool settings */})
db := rioch.New(sqlDB)
```

## Capabilities

ClickHouse is a read-and-append rio dialect.

| Area | Supported APIs and behavior |
|---|---|
| Reads | `From`, `Find`, `First`, `Sole`, `All`, `Rows`, `Count`, `Exists`, `Query.Pluck`, scopes, joins, grouping, ordering, limits and offsets |
| Relations | `With`, `WithCount`, `WhereHas`, `WhereHasNot`, `RelWhere`, `RelOrder`, `RelLimit`, `RelWithTrashed` |
| Soft-delete reads | Default filtering, `WithTrashed`, `OnlyTrashed` |
| Writes | `Insert`, `InsertAll`; ClickHouse returns no generated IDs and rio does not backfill rows |
| ClickHouse | `Query.Final()` adds the `FINAL` table modifier |
| Escape hatches | `Raw`, `Exec`, query hooks, reusable queries with `Must`/`Validate`, and `Unwrap` |

Unsupported APIs fail with a ClickHouse-specific alternative in the error:

| APIs | Use instead |
|---|---|
| `Update`, `UpdateAll` | `rio.Exec` with `ALTER TABLE ... UPDATE`, or append a new `ReplacingMergeTree` version |
| `Delete`, `DeleteAll`, `ForceDelete`, `ForceDeleteAll` | `rio.Exec` with `DELETE FROM` or `ALTER TABLE ... DELETE` |
| `Restore`, `RestoreAll` | `rio.Exec` with `ALTER TABLE ... UPDATE` |
| `Upsert`, `UpsertAll` | Append a new `ReplacingMergeTree` version and read with `Final()` |
| `FirstOrCreate`, `CreateOrFirst` | Coordinate in the application or use `ReplacingMergeTree`; ClickHouse has no unique constraint to arbitrate the race |
| `db.Tx`, `TxWith` | One `InsertAll` per atomic statement, a clickhouse-go batch through `Unwrap`, or a separate native connection |
| `Attach`, `Detach`, `SyncRelation` | Manage the join table explicitly with `rio.Exec` or append-only rows |
| `ForUpdate` | Remove it; ClickHouse has no row locks |
| `rio.WithStmtCache` | Leave it disabled; clickhouse-go prepares INSERT batches, not reusable SELECT statements |

ClickHouse cannot generate conventional IDs. Assign IDs before `Insert`, or
tag the field `rio:",noautoincr"` when zero is a valid stored value.

Mutations issued with `rio.Exec` are asynchronous unless the statement uses
`SETTINGS mutations_sync = 1` (`2` waits for all replicas). clickhouse-go
reports zero affected rows, and `LastInsertId` is unavailable.

## `FINAL` and `ReplacingMergeTree`

With `ReplacingMergeTree(version)`, update a row by incrementing its version
and inserting it again:

```go
p.Version++
p.Name = "new name"
if err := rio.Insert(ctx, db, &p); err != nil {
	return err
}

profiles, err := rio.From[Profile]().Final().All(ctx, db)
```

`Final()` affects only the query's main SELECT, including `Count`, `Exists`,
and `Pluck`. It does not propagate to preloads, `WithCount`, or `WhereHas`
subqueries. Non-ClickHouse dialects reject it; a ClickHouse engine that does
not support versioned merges may return `ILLEGAL_FINAL`.

Without `FINAL`, a read may observe any unmerged version because the sorting
key is not unique. `OPTIMIZE TABLE ... FINAL` performs an eager merge;
ClickHouse's `final=1` setting applies finalization to every table in a query.

## Placeholders

Use `?` arguments for values in `Where`, `Raw`, and `Exec`. rio validates
placeholder arity; clickhouse-go then interpolates the values into the SQL sent
to the server. Runtime slices expand in `IN (?)`.

Important details:

- clickhouse-go v2.48.0 or newer protects `?` in quoted regions and supported
  comments.
- With arguments, rio rejects `?` in `$tag$...$tag$` heredocs and `//`
  comments because the driver does not protect those regions. Bind the value
  or use a quoted string and `--` comment.
- Write `??` when the SQL needs a literal question mark; rio emits
  clickhouse-go's `\?` escape.
- Interpolated values can appear in server query logs. Large `IN (?)`
  expansions also count against `max_query_size`; rio automatically chunks
  relation preload keys, but application queries must be chunked by the caller.

## Time and bytes

rio normalizes `time.Time` to UTC and microsecond precision, then binds text
with an explicit offset. Values round-trip through `DateTime64(6)` as the same
instant even when the column has another timezone. `DateTime64(3)` and
`DateTime` truncate to their schema precision; `Date` and `Date32` reject this
binding.

ClickHouse 26.7 extends `DateTime64(6)` text values to years 0001–9999. rio
accepts that range, including zero `time.Time`, and rejects values outside it.
Use `*time.Time` with `Nullable(DateTime64(6))` when the value is absent.

`[]byte`, `json.RawMessage`, and other named byte slices bind as one ClickHouse
`String`, not `Array(UInt8)`; a typed nil slice binds SQL `NULL`. Types that
implement `driver.Valuer` keep control of their encoding. Use a native batch
for large binary or columnar loads.

## Bulk and native access

`InsertAll` emits chunked multi-value INSERT statements. For larger loads, use
`db.Unwrap()` and clickhouse-go's database/sql batch sequence
(`Begin` → `Prepare` → repeated `Exec` → `Commit`). This is a batch shim, not a
general transaction; rio rejects `db.Tx` because separate statements would not
be atomic. `Unwrap` also exposes database/sql pool tuning.

For `PrepareBatch`, columnar append, or driver-specific native scans, open a
separate `clickhouse.Conn` with `clickhouse.Open`. `Unwrap` returns the
`*sql.DB`, not a native connection.

## Error semantics

This module installs no rio constraint-error translator because ClickHouse has
no unique or foreign key constraints. `rio.ErrDuplicateKey` and
`rio.ErrForeignKeyViolated` therefore never match. Server errors remain in the
chain as `*clickhouse.Exception`:

```go
var exception *clickhouse.Exception
if errors.As(err, &exception) {
	log.Printf("ClickHouse error %d: %s", exception.Code, exception.Message)
}
```

`Open` prefixes DSN and `sql.Open` failures with `clickhouse: open:`. Server
and execution errors otherwise remain in rio's normal error chain.

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

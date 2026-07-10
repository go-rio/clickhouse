# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse.svg)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Release](https://img.shields.io/github/release/go-rio/clickhouse.svg)](https://github.com/go-rio/clickhouse/releases)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver module for [rio](https://github.com/go-rio/rio), built on the
official
[clickhouse-go v2](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)
driver's `database/sql` interface.

This module provides constructors and eager DSN validation only. All SQL
grammar, including which rio APIs the dialect supports and rejects, lives in
rio. Two things the other driver modules ship are absent:

- **No error translator.** ClickHouse has no unique constraints and no foreign
  keys, so `rio.ErrDuplicateKey` and `rio.ErrForeignKeyViolated` cannot occur.
  Server errors reach you as `*clickhouse.Exception` via `errors.As`, code and
  message intact.
- **No DSN pinning.** sqlite pins pragmas and mysql pins `parseTime` for
  correctness; ClickHouse has no such parameter. rio's time encoding carries
  its own UTC offset, so no timezone setting is needed; `compress`, timeouts,
  and `secure` are preference, not correctness. The DSN passes through byte for
  byte.

## Install

```sh
go get github.com/go-rio/clickhouse
```

## Usage

```go
import (
	"github.com/go-rio/rio"
	rioch "github.com/go-rio/clickhouse"
)

db, err := rioch.Open("clickhouse://default@localhost:9000/analytics?compress=lz4")
if err != nil {
	// ...
}
defer db.Close()

events, err := rio.From[Event]().Where("kind = ?", "click").All(ctx, db)
```

DSN forms: native protocol (`clickhouse://user:pass@h1:9000,h2:9000/db`) or
HTTP (`http://` / `https://`). Pass your own `*sql.DB` (e.g. built for TLS) to
`New`:

```go
sqlDB := clickhouse.OpenDB(&clickhouse.Options{...}) // clickhouse-go v2
db := rioch.New(sqlDB)
```

## What works, what doesn't

ClickHouse is a **read + append** dialect.

Fully supported, same semantics as the other dialects:

| Area | APIs |
|---|---|
| Reads | `From`, `Where`, `OrderBy`, `GroupBy`, `Having`, `Join`, `Limit`, bare `Offset`, `Find`, `First`, `Sole`, `All`, `Rows`, `Count`, `Exists`, `Pluck`, `Scope` |
| Relations | `With` (all four kinds), `RelWhere`, `RelOrder`, `RelLimit`, `RelWithTrashed`, `WithCount`, `WhereHas` / `WhereHasNot` (server ≥ 25.8) |
| Soft-delete reads | `WithTrashed`, `OnlyTrashed`, default filtering |
| Writes | `Insert`, `InsertAll` (multi-VALUES, chunked) — **never backfilled** (no `RETURNING`, no generated IDs; what you insert is what the struct holds) |
| Escape hatches | `Raw` (full ClickHouse SQL: `FINAL`, `SAMPLE`, `ARRAY JOIN`, `SETTINGS`, …), `Exec`, `MustCompile`/`Compile`, hooks, `Unwrap` |
| ClickHouse-only | `Query.Final()` — reads through the `FINAL` table modifier (see the ReplacingMergeTree recipe below) |

Rejected — each API returns an error naming the ClickHouse-native way out:

| API | Why | The way out |
|---|---|---|
| `Update`, `UpdateAll` | updates are asynchronous mutations without an affected-row count | `rio.Exec` + `ALTER TABLE … UPDATE`, or model updates as inserts into a ReplacingMergeTree |
| `Delete`, `DeleteAll`, `ForceDelete`, `ForceDeleteAll` | same, for deletes | `rio.Exec` + lightweight `DELETE FROM` (23.3+) or `ALTER TABLE … DELETE` |
| `Restore`, `RestoreAll` | soft-delete writes are UPDATEs | `rio.Exec` + `ALTER TABLE … UPDATE` |
| `Upsert`, `UpsertAll` | no unique constraints, no conflict clause | insert a new row version into a ReplacingMergeTree, read with `Final()` |
| `FirstOrCreate`, `CreateOrFirst` | no unique constraint to arbitrate the race | ReplacingMergeTree semantics, or coordinate in the application |
| `db.Tx` / `TxWith` | clickhouse-go's `Begin` is a no-op — statements would silently commit independently | one `InsertAll` per atomic batch, or native batches via `Unwrap()` |
| `Attach`, `Detach`, `SyncRelation` | need unique keys / synchronous deletes / transactions | `rio.Exec` on the join table |
| `ForUpdate` | no row locks exist; reads are lock-free snapshots of parts | remove it |
| `rio.WithStmtCache` | the driver prepares only INSERT batches; a cached SELECT fails on first use | leave it off (panics at `New`) |
| `Insert` with a zero conventional `ID` | ClickHouse cannot generate IDs | assign it yourself (UUID/Snowflake/…), or tag `rio:",noautoincr"` if zero is a real value |

## Schema guide

| rio concept | ClickHouse schema |
|---|---|
| primary key (`ID`, `rio:",pk"`) | the `ORDER BY` sorting key of a MergeTree table — **not unique**; `Find` on an unmerged ReplacingMergeTree can see any version (use `From().Where().Final().First()` for the merged view) |
| `time.Time` field | `DateTime64(6[, 'UTC'])` — see Time below |
| pointer field (`*string`, `*time.Time`) | `Nullable(T)` |
| soft-delete column | `Nullable(DateTime64(6))` — read-side filtering works; the *writes* (`Delete`/`Restore`) are rejected, so set it via `rio.Exec` mutations if you use it |
| conventional `ID` | generated by **your application** (UUID, Snowflake, …); zero-ID inserts are rejected |
| `version` field | legal to model; on ClickHouse its natural use is the `ReplacingMergeTree(ver)` version column, incremented by the application |

```sql
CREATE TABLE users (
    id         UInt64,
    email      String,
    age        Int64,
    bio        Nullable(String),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
) ENGINE = MergeTree ORDER BY id
```

## ReplacingMergeTree recipe (Upsert replacement)

Insert a newer row version; background merges collapse them.

```sql
CREATE TABLE profiles (
    id         UInt64,
    name       String,
    version    UInt64,
    updated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(version) ORDER BY id
```

```go
p.Version++            // the application owns the version counter
p.Name = "new name"
err := rio.Insert(ctx, db, &p)                    // a new row version

merged, err := rio.From[Profile]().Final().All(ctx, db) // read collapsed
n, err := rio.From[Profile]().Final().Count(ctx, db)    // count collapsed
```

`Final()` applies the `FINAL` table modifier to this query's own SELECT
(including `Count`/`Exists` shapes); it does not propagate into preload,
`WithCount`, or `WhereHas` subqueries. Engines without versioned merges reject
it (`ILLEGAL_FINAL`); every other rio dialect rejects it at render.

Alternatives:

- `OPTIMIZE TABLE … FINAL` — eager merge, expensive.
- `final=1` setting (ClickHouse 22.11+) on the DSN or profile — applies FINAL
  server-side to every table in every query.

## Mutations

`rio.Exec` passes any statement through and is the supported way to update or
delete:

```go
_, err := rio.Exec(ctx, db, "ALTER TABLE users UPDATE age = ? WHERE id = ?", 31, id)
_, err  = rio.Exec(ctx, db, "DELETE FROM users WHERE id = ?", id) // lightweight DELETE, 23.3+
```

- `sql.Result` reports **0 rows affected, always**: clickhouse-go returns
  `driver.RowsAffected(0)` unconditionally, and `LastInsertId` always errors.
  Do not branch on either.
- Mutations are **asynchronous** by default: the statement returns once the
  mutation is queued. Append `SETTINGS mutations_sync = 1` (or `2` for all
  replicas) to wait, at a latency cost. Lightweight `DELETE FROM` is immediately
  *visible* but still an asynchronous mutation underneath.

## Time

rio binds `time.Time` as fixed-format text with an explicit UTC offset
(`2006-01-02 15:04:05.000000+00:00`); clickhouse-go's client-side binder
otherwise truncates a `time.Time` argument to whole seconds. On ClickHouse 26+
(the server floor, see Requirements) that text parses natively in INSERT and
comparisons, microseconds intact, the offset pinning the instant regardless of
any column timezone attribute. The same instant lands correctly in
`DateTime64(6)` and `DateTime64(6, 'Asia/Shanghai')` and survives an
insert-then-reload `Equal` comparison.

| Column type | Behavior |
|---|---|
| `DateTime64(6[, tz])` | **recommended** — full microsecond round-trip, any timezone attribute |
| `DateTime64(3)` | stores, silently truncating to milliseconds (schema responsibility, like MySQL `DATETIME(3)`) |
| `DateTime` (seconds) | stores, silently truncating to whole seconds (same schema-responsibility class) |
| `Date` / `Date32` | rejected server-side for rio's time binding — use `DateTime64` for `time.Time` fields |

Range: ClickHouse silently clamps out-of-range times to the
`[1900-01-01, 2299-12-31]` DateTime64 bounds, even on INSERT; rio refuses to
bind such values instead. The common case is the zero `time.Time` (year 1),
which is not storable; use a `*time.Time` with a `Nullable` column for "no
value".

Reads return in the **column's** timezone location; the instant compares
`Equal` to what you wrote (epoch storage), as with PostgreSQL/MySQL location
differences.

## Requirements

**ClickHouse server 26.0+.** 26 natively parses rio's offset-carrying time text
in INSERT and comparisons. On 25.x and earlier, any `time.Time` in a query
condition fails with `TYPE_MISMATCH` (the implicit `String→DateTime64` cast
ignores every input-format session setting), so those servers are unsupported.
The floor subsumes the per-feature ones (`WhereHas` needs 25.8's correlated
`EXISTS`, `RelLimit` 22.x's window functions); 26 also fixed the 25.x parser
defect that mirrored the fractional part of pre-1970 times.

## Performance notes

- **Every argument is interpolated client-side.** clickhouse-go's
  `database/sql` path has no server-side parameter binding; rio's `?`
  placeholders become literals in the statement text. Consequences: statement
  text can contain your data (mind query logs and `system.query_log`), and
  `IN (?)` expansions count against `max_query_size` (default 256 KiB). rio
  chunks preload key lists at 8192 parameters, which fits numeric keys; raise
  `max_query_size` if huge string-keyed preloads hit the ceiling. Multi-VALUES
  INSERT data is exempt from `max_query_size`.
- **Bulk imports belong in native batches.** `InsertAll` is correct and chunked,
  but for millions of rows use clickhouse-go's `PrepareBatch` API via
  `db.Unwrap()`.
- `compress=lz4` on the DSN is cheap and usually worthwhile; rio never sets it.
- `[]byte` values bind as `String` and travel as interpolated text. For bulk
  binary data, use the native batch API.

## Version floor

| Component | Floor | Why |
|---|---|---|
| ClickHouse server | **26.0** | native offset-text time parsing in INSERT and comparisons (see Requirements); subsumes the per-feature floors (`WhereHas` 25.8, `RelLimit` 22.x, lightweight `DELETE` 23.3) |
| clickhouse-go | **v2.47.0** (go.mod enforced) | first release whose client-side binder is quote-aware: earlier versions rewrite `?` inside string literals (#1860), breaking any SQL that contains one |

## License

[MIT](LICENSE)

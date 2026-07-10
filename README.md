# clickhouse

[![Doc](https://pkg.go.dev/badge/github.com/go-rio/clickhouse)](https://pkg.go.dev/github.com/go-rio/clickhouse)
[![Go](https://img.shields.io/github/go-mod/go-version/go-rio/clickhouse)](https://go.dev/)
[![Test](https://github.com/go-rio/clickhouse/actions/workflows/test.yml/badge.svg)](https://github.com/go-rio/clickhouse/actions)
[![License](https://img.shields.io/github/license/go-rio/clickhouse)](https://opensource.org/license/MIT)

ClickHouse driver module for [rio](https://github.com/go-rio/rio), the
zero-surprise Go ORM. Built on the official
[clickhouse-go v2](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)
driver's `database/sql` interface.

Driver modules are deliberately thin — this one is the thinnest: constructors
and eager DSN validation, nothing else. All SQL grammar, including which rio
APIs the dialect supports and which it rejects with an explanation, lives in
rio itself. Two things the other go-rio driver modules ship are deliberately
absent here:

- **No error translator.** ClickHouse has no unique constraints and no
  foreign keys, so `rio.ErrDuplicateKey` and `rio.ErrForeignKeyViolated`
  cannot happen on this dialect — there is nothing to translate. Server
  errors reach you as `*clickhouse.Exception` via `errors.As`, code and
  message intact.
- **No DSN pinning.** sqlite pins pragmas and mysql pins `parseTime` because
  rio's correctness depends on them. ClickHouse has no such parameter: rio's
  time encoding carries its own UTC offset, so no timezone setting is needed,
  and everything else (`compress`, timeouts, `secure`) is preference, not
  correctness. Your DSN passes through byte for byte.

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

Both DSN forms work: native protocol (`clickhouse://user:pass@h1:9000,h2:9000/db`)
and HTTP (`http://` / `https://`). Bring your own `*sql.DB` — e.g. one built
programmatically for TLS — with `New`:

```go
sqlDB := clickhouse.OpenDB(&clickhouse.Options{...}) // clickhouse-go v2
db := rioch.New(sqlDB)
```

## What works, what doesn't

ClickHouse in rio is a **read + append** dialect. The support matrix is the
honest shape of an OLAP store, not a subset chosen for convenience.

Fully supported — same semantics as the other dialects:

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

`errors.Is(err, rio.ErrDuplicateKey)` and `rio.ErrForeignKeyViolated` never
match on ClickHouse — there are no constraints to violate. This is a dialect
fact, not a missing feature.

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

## The ReplacingMergeTree recipe (Upsert's replacement)

ClickHouse's answer to "update this row" is "insert a newer version and let
background merges collapse them":

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
(including its `Count`/`Exists` shapes). It does not propagate into preload,
`WithCount`, or `WhereHas` subqueries. On table engines without versioned
merges the server rejects it (`ILLEGAL_FINAL`); on every other rio dialect it
is rejected at render. Alternatives with different trade-offs:
`OPTIMIZE TABLE … FINAL` (eager merge, expensive), or the `final=1` setting
(ClickHouse 22.11+) on the DSN or profile, which applies FINAL server-side to
every table in every query.

## Mutations: the escape hatch

`rio.Exec` passes any statement through, and is the official way to update or
delete on ClickHouse:

```go
_, err := rio.Exec(ctx, db, "ALTER TABLE users UPDATE age = ? WHERE id = ?", 31, id)
_, err  = rio.Exec(ctx, db, "DELETE FROM users WHERE id = ?", id) // lightweight DELETE, 23.3+
```

Two facts to internalize:

- **`sql.Result` reports 0 rows affected, always** — clickhouse-go returns
  `driver.RowsAffected(0)` unconditionally, and `LastInsertId` always errors.
  Do not branch on either.
- **Mutations are asynchronous** by default: the statement returns once the
  mutation is *queued*. Append `SETTINGS mutations_sync = 1` (or `2` for all
  replicas) to wait, at a latency cost. Lightweight `DELETE FROM` is
  immediately *visible* but still an asynchronous mutation underneath.

## Time

rio binds `time.Time` as fixed-format text with an explicit UTC offset —
`2006-01-02 15:04:05.000000+00:00` — because clickhouse-go's client-side
binder silently truncates a `time.Time` argument to whole seconds. On
ClickHouse 26+ (this module's server floor, see Requirements) that text
parses natively in INSERT and comparisons alike, with microseconds intact
and the offset pinning the instant regardless of any column timezone
attribute: the same instant lands correctly in `DateTime64(6)` and
`DateTime64(6, 'Asia/Shanghai')` and survives an insert-then-reload `Equal`
comparison.

| Column type | Behavior |
|---|---|
| `DateTime64(6[, tz])` | **recommended** — full microsecond round-trip, any timezone attribute |
| `DateTime64(3)` | stores, silently truncating to milliseconds (schema responsibility, like MySQL `DATETIME(3)`) |
| `DateTime` (seconds) | stores, silently truncating to whole seconds (same schema-responsibility class) |
| `Date` / `Date32` | rejected server-side for rio's time binding — use `DateTime64` for `time.Time` fields |

Range: ClickHouse silently clamps out-of-range times to the
`[1900-01-01, 2299-12-31]` DateTime64 bounds — even on INSERT. rio refuses to
bind such values instead, loudly. The most common case is the zero
`time.Time` (year 1): a zero time is not storable; use a `*time.Time` with a
`Nullable` column for "no value".

Reads come back in the **column's** timezone location; the instant compares
`Equal` to what you wrote (epoch storage), same as the location differences
you may see on PostgreSQL/MySQL.

## Requirements

**ClickHouse server 26.0+.** 26 is where the server natively parses rio's
offset-carrying time text in both INSERT and comparisons; on 25.x and
earlier, any `time.Time` used in a query condition fails with
`TYPE_MISMATCH` (the comparison's implicit `String→DateTime64` cast ignores
every input-format session setting), so those servers are not supported. The
floor also subsumes the older per-feature ones — `WhereHas` needed 25.8's
correlated `EXISTS`, `RelLimit` 22.x's window functions — and 26 fixed the
25.x parser defect that mirrored the fractional part of pre-1970 times.

## Performance notes

- **Every argument is interpolated client-side.** clickhouse-go's
  `database/sql` path has no server-side parameter binding: rio's `?`
  placeholders become literals in the statement text. Two consequences:
  statement text can contain your data (mind query logs and `system.query_log`),
  and `IN (?)` expansions count against the server's `max_query_size`
  (default 256 KiB) — rio chunks preload key lists at 8192 parameters, which
  fits comfortably for numeric keys; raise `max_query_size` if huge
  string-keyed preloads hit the ceiling. Multi-VALUES INSERT data is exempt
  from `max_query_size` by design.
- **Bulk imports belong in native batches.** `InsertAll` is correct and
  chunked, but for millions of rows use clickhouse-go's `PrepareBatch` API
  directly — three lines away via `db.Unwrap()`.
- `compress=lz4` on the DSN is cheap and usually worth it. rio never sets it
  for you.
- Large binary blobs travel as interpolated text on this dialect; `[]byte`
  values bind as `String`. For bulk binary data, prefer the native batch API.

## Version floor

| Component | Floor | Why |
|---|---|---|
| ClickHouse server | **26.0** | native offset-text time parsing in INSERT and comparisons (see Requirements); subsumes the per-feature floors (`WhereHas` 25.8, `RelLimit` 22.x, lightweight `DELETE` 23.3) |
| clickhouse-go | **v2.47.0** (go.mod enforced) | first release whose client-side binder is quote-aware: earlier versions rewrite `?` inside string literals (#1860), breaking any SQL that contains one |

## License

[MIT](LICENSE)

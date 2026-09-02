# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.3] - 2026-09-02

### Fixed

- The peer-close probe test runs only where `liveCheck` probes (unix); on other platforms the test pins the stub's contract that idle connections are presumed live. Windows CI had failed since v0.8.0.

## [0.9.2] - 2026-09-02

### Changed

- rio v0.17.0.

## [0.9.1] - 2026-09-02

### Fixed

- A negative `float64` bound into a `Decimal` column through `InsertAll` kept its sign; the encoder negated an already two's-complement value, so `-3.5` landed as `3.50`.
- A nested server exception reports its outermost entry (the primary error) instead of the innermost cause.

### Added

- `CONTRIBUTING.md`, `CHANGELOG.md`, `llms.txt`, and compile-only examples for `Open` and `OpenSQL`.

### Changed

- README restructured; package comments name the entry points; source files follow one declaration order. No API change.

## [0.9.0] - 2026-09-02

### Changed

- Time parameters render as `toDateTime64('YYYY-MM-DD HH:MM:SS.ffffff', 6, 'UTC')` literals, which every date/time column's VALUES cast accepts; the previous `fromUnixTimestamp64Micro` form failed on `DateTime`, `Date`, and `Date32` targets with `TYPE_MISMATCH`.
- Every query carries `date_time_overflow_behavior = 'throw'`, so an instant outside a `Date` or `DateTime` column's range fails instead of wrapping.
- Integer encoders range-check bindings by the column's width and sign instead of wrapping; a `uint64` with the high bit set binds as-is into `UInt64` and `UInt128`, is rejected for `Int64`, and converts without sign extension into float columns.
- Requires rio v0.16.0.

### Fixed

- `DateTime64` columns decode and encode the server's whole 1900–2299 range at precisions below 9; the nanosecond arithmetic overflowed past 2262 on both native paths.
- `Date32` floors pre-1970 instants to the previous day instead of truncating toward the epoch; `Date` rejects the day before it.
- Every operation honors context cancellation: the in-flight I/O aborts immediately, the context's error surfaces, and the connection is discarded; the watch is released when the stream, ping, or insert completes.
- `Abort` after a failed insert flush no longer blocks forever.

## [0.8.0] - 2026-09-01

### Added

- Idle connections are probed with a non-blocking read before reuse, so a peer-closed connection (server restart, load-balancer idle kill) is discarded instead of surfacing as an EOF on the next query.
- `conn_max_lifetime` DSN parameter: connections are replaced after it (default one hour).

### Fixed

- `Date` and `DateTime` encoders reject instants outside their unsigned ranges instead of wrapping; the zero `time.Time` used to insert silently as 1974-10-01.
- `float32` parameters interpolate at 32-bit precision.

## [0.7.0] - 2026-09-01

### Changed

- Requires rio v0.14.0, which binds times as `time.Time`.

### Fixed

- Time parameters render as epoch-microsecond `fromUnixTimestamp64Micro` expressions, so range predicates on sorting-key time columns (`ORDER BY created_at` with `created_at >= ?`) pass the primary-key analyzer instead of failing with `TYPE_MISMATCH`; pre-1970 instants are accepted.

## [0.6.0] - 2026-09-01

### Added

- `conn_max_idle_time` DSN parameter: idle connections expire after it (default five minutes) instead of being presumed live.
- A failure while transmitting a query redials once, since the server cannot have acted on it; the database/sql shim reports the same failure as `driver.ErrBadConn` so database/sql retries it.
- Column types, reading and writing on both insert paths: `Decimal` up to 38 digits (as fixed-scale text), `Int128`/`UInt128` (as decimal text), `IPv4`/`IPv6` (as text), and `SimpleAggregateFunction` unwrapping to its inner type.
- `SELECT NULL` (a `Nullable(Nothing)` column) scans as nil instead of failing as an unsupported type.

## [0.5.0] - 2026-09-01

### Added

- The ClickHouse native TCP protocol, implemented in this module: `go.mod` requires only `go-rio/rio`. The client pins protocol revision 54460 and the server negotiates down to it; connections are uncompressed.
- `Open(ctx, dsn)` connects rio's native channel: result blocks decode through typed per-column readers straight into rio's `NativeCell` sinks, and `InsertAll` streams native column blocks through `NativeCopier`.
- `OpenSQL(dsn)` serves a `database/sql` handle over the same protocol, also reachable through `DB.Unwrap`: `?` placeholders, no transactions or prepared statements, zero affected-row counts.
- `Exception` exposes server errors (`Code`, `Name`, `Message`) through `errors.As`.
- Parameters interpolate client-side under ClickHouse quoting rules, including the `\?` literal escape rio renders.
- `InsertAll` aborts cleanly: an abandoned stream poisons its connection instead of returning it to the pool half-sent.

### Changed

- **Breaking:** `clickhouse-go` is no longer used. `Open` takes a context and the `clickhouse://` DSN documented in the README; unknown DSN parameters are rejected.
- Requires ClickHouse 26 or newer.
- Server exceptions no longer poison the connection; cancellation-shaped failures do.
- Against the previous `clickhouse-go` channel (5 columns, MergeTree, same machine): `InsertAll` of 10k rows 270 ms → 73 ms, `Select` of 100k rows 27 ms → 14 ms at one allocation per row. Result state and decoders are recycled across queries: Select 100k allocates 18.5 MB instead of 20.0 MB with 91 fewer allocations per query, InsertAll 10k 1.28 MB instead of 1.73 MB per batch.

### Fixed

- A mid-stream server abort during `InsertAll` surfaces as its `Exception` instead of deadlocking both sides.
- `Err` read after `Close` on a re-pooled connection no longer races; the final error is cached before release.

## [0.3.1] - 2026-08-31

### Changed

- Requires rio v0.13.0.

## [0.3.0] - 2026-08-30

### Changed

- Requires rio v0.11.0: `[]byte` binds as `String`, natively typed integer widths convert in every direction (unsigned, float, bool), and the query lexers are hardened.

## [0.2.1] - 2026-08-20

### Changed

- Builds on Go 1.27 GA and is tested against ClickHouse 26.7. Requires clickhouse-go v2.48.0 and rio v0.10.1.

## [0.2.0] - 2026-08-09

### Changed

- Requires rio v0.10.0 (`Query.Pluck`, `Query.Must`, and `Query.Validate`) and Go 1.27 (release candidate at the time).

## [0.1.3] - 2026-07-11

### Changed

- Requires rio v0.9.0.

## [0.1.2] - 2026-07-10

### Changed

- Requires ClickHouse 26.0 or newer and rio v0.7.2, which binds time values plainly.

## [0.1.1] - 2026-07-10

### Fixed

- Time comparisons work on ClickHouse 25.8 LTS: time arguments inline server-side and no longer depend on any input-format setting (rio v0.7.1).

## [0.1.0] - 2026-07-10

### Added

- ClickHouse dialect driver for rio over clickhouse-go v2.47.0, the first release with a quote-aware client-side binder; requires rio v0.7.0.
- Eager DSN validation at `Open`.
- No constraint-error translation: ClickHouse has no unique or foreign-key constraints, so rio's constraint sentinels never fire.

[Unreleased]: https://github.com/go-rio/clickhouse/compare/v0.9.3...HEAD
[0.9.3]: https://github.com/go-rio/clickhouse/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/go-rio/clickhouse/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/go-rio/clickhouse/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/go-rio/clickhouse/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/go-rio/clickhouse/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/go-rio/clickhouse/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/go-rio/clickhouse/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/go-rio/clickhouse/compare/v0.3.1...v0.5.0
[0.3.1]: https://github.com/go-rio/clickhouse/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/go-rio/clickhouse/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/go-rio/clickhouse/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/go-rio/clickhouse/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/go-rio/clickhouse/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/go-rio/clickhouse/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/go-rio/clickhouse/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/go-rio/clickhouse/releases/tag/v0.1.0

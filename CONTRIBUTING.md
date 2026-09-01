# Contributing to go-rio/clickhouse

## Prerequisites

- Go 1.27 or later (`go.mod` pins `go 1.27.0`)
- Docker, for the integration tests and benchmarks: they need a ClickHouse
  26.7 server

## Clone

```sh
git clone https://github.com/go-rio/clickhouse.git
cd clickhouse
```

## Tests

Unit tests need no server:

```sh
go vet ./...
go test -race ./...
```

Integration tests and benchmarks run only when `RIO_CLICKHOUSE_DSN` is
set; without it they skip. Start a server and point the variable at it:

```sh
docker run -d --name rio-ch -e CLICKHOUSE_PASSWORD=rio -p 127.0.0.1:19000:9000 clickhouse/clickhouse-server:26.7-alpine
export RIO_CLICKHOUSE_DSN='clickhouse://default:rio@127.0.0.1:19000/default'

go test -race ./...
go test -run '^$' -bench . -benchmem .
```

The integration tests create and drop their own tables in the DSN's
database. `docker rm -f rio-ch` removes the server. CI runs the same
matrix: `go vet` and `go test -race` on Linux, macOS, and Windows, plus the
integration job against `clickhouse/clickhouse-server:26.7-alpine`.

## Pull requests

- Every change ships with tests: unit tests over in-memory pipes for
  protocol behavior, integration tests for anything the server decides.
- One test file per source file: `insert.go` is tested by
  `insert_test.go`, `pool.go` by `pool_test.go`, and so on.
- Commit subjects use conventional prefixes: `feat:`, `fix:`, `docs:`,
  `style:`, `refactor:`, `test:`, `ci:`, `chore:`; `feat!:` marks a
  breaking change.
- `gofmt -l .` prints nothing and `go vet ./...` is clean.
- The public API and the wire behavior are the contract; a `style:` or
  `refactor:` commit changes neither.

## Comment style

- Every exported identifier has a doc comment that starts with its name
  and states purpose, when to use it, constraints, and error cases. No
  "Parameters:" or "Returns:" lists, no marketing words, no history.
- Internal comments are one line, two at most, and state only a contract
  the code cannot: packet codes, block layout, the settings sent with
  every query, the time literal form, range guards, the context watch and
  connection-poisoning rules. Paraphrase, signature restatement, and
  what-was-tried narrative are deleted on sight.
- Package comments say what the package is and name its entry points.
- Within a file, declarations read imports, constants, types with each
  type's constructor and methods grouped immediately after it, then
  helpers.

## Releases

1. Tag the rio core first. This module follows it: bump the `go-rio/rio`
   requirement in `go.mod` to the released version.
2. Add the version's section to `CHANGELOG.md` (Keep a Changelog: Added,
   Changed, Fixed; user-visible changes only) and update the compare links
   at the bottom.
3. Push a signed annotated tag (`git tag -s vX.Y.Z`); the release workflow
   publishes the GitHub Release from it.

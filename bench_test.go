package clickhouse

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/go-rio/rio"
)

// Gated throughput guards; run with RIO_CLICKHOUSE_DSN set.

func benchDB(b *testing.B) *rio.DB {
	b.Helper()
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		b.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

type benchRow struct {
	ID    uint64 `rio:",pk,noautoincr"`
	Label string
	Val   float64
}

func (benchRow) TableName() string { return "bench_rows" }

func BenchmarkInsertAll10k(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS bench_rows"); err != nil {
		b.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE bench_rows (
		id UInt64, label String, val Float64,
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY id`); err != nil {
		b.Fatal(err)
	}
	rows := make([]benchRow, 10_000)
	next := uint64(1)
	b.ReportAllocs()
	for b.Loop() {
		for i := range rows {
			rows[i] = benchRow{ID: next, Label: fmt.Sprintf("l%d", i%97), Val: float64(i)}
			next++
		}
		if err := rio.InsertAll(ctx, db, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelect100k(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS bench_rows"); err != nil {
		b.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE bench_rows (
		id UInt64, label String, val Float64,
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY id`); err != nil {
		b.Fatal(err)
	}
	seed := make([]benchRow, 100_000)
	for i := range seed {
		seed[i] = benchRow{ID: uint64(i + 1), Label: fmt.Sprintf("l%d", i%97), Val: float64(i)}
	}
	if err := rio.InsertAll(ctx, db, seed); err != nil {
		b.Fatal(err)
	}
	q := rio.From[benchRow]().Must()
	b.ReportAllocs()
	for b.Loop() {
		out, err := q.All(ctx, db)
		if err != nil || len(out) != len(seed) {
			b.Fatalf("len=%d err=%v", len(out), err)
		}
	}
}

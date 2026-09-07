package clickhouse

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/go-rio/clickhouse/internal/chproto"
)

// A zero Rows panics when used: the released guards must answer without it.
func TestNativeRowsReleasedGuards(t *testing.T) {
	r := &nativeRows{rows: &chproto.Rows{}, released: true}
	if r.Next() {
		t.Fatal("Next after Close reported a row")
	}
	if err := r.Scan(new(int)); !errors.Is(err, errRowsClosed) {
		t.Fatalf("Scan after Close = %v, want errRowsClosed", err)
	}
}

func TestNativeRowsColumnsSurviveConnectionReuse(t *testing.T) {
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	cfg, _, err := parseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool := chproto.NewPool(cfg, 1, 0, 0)
	defer pool.Close()
	nd := &nativeDB{pool: pool}
	ctx := context.Background()
	empty, err := nd.Query(ctx, "SELECT 1 AS sku_id WHERE 0", nil)
	if err != nil {
		t.Fatal(err)
	}
	empty.Close()
	wider, err := nd.Query(ctx, "SELECT 1 AS next_query, 2 AS other", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wider.Close()
	if empty.(*nativeRows).conn != wider.(*nativeRows).conn {
		t.Fatal("the pool did not reuse the released connection")
	}
	if cols := wider.Columns(); !slices.Equal(cols, []string{"next_query", "other"}) {
		t.Fatalf("columns = %v, want [next_query other]", cols)
	}
	if cols := empty.Columns(); !slices.Equal(cols, []string{"sku_id"}) {
		t.Fatalf("columns after connection reuse = %v, want [sku_id]", cols)
	}
	if empty.Next() {
		t.Fatal("Next after Close reported a row")
	}
}

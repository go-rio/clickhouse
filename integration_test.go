package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-rio/rio"
)

func openTest(t *testing.T) *rio.DB {
	t.Helper()
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type Event struct {
	ID     uint64 `rio:",pk,noautoincr"`
	Name   string
	Note   *string
	Score  float64
	Active bool
	Stars  int32
	At     time.Time
}

func TestNativeRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS events"); err != nil {
		t.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE events (
		id UInt64, name String, note Nullable(String), score Float64,
		active Bool, stars Int32, at DateTime64(6, 'UTC'),
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rio.Exec(ctx, db, "DROP TABLE IF EXISTS events") })

	note := "hello 'quoted' \\ back"
	at := time.Date(2026, 9, 1, 8, 30, 15, 123456000, time.UTC)
	e := Event{ID: 1, Name: "first", Note: &note, Score: 2.5, Active: true, Stars: -7, At: at}
	if err := rio.Insert(ctx, db, &e); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := rio.Insert(ctx, db, &Event{ID: 2, Name: "second", At: at.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	got, err := rio.From[Event]().OrderBy("id").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %d", len(got))
	}
	g := got[0]
	if g.ID != 1 || g.Name != "first" || g.Note == nil || *g.Note != note ||
		g.Score != 2.5 || !g.Active || g.Stars != -7 || !g.At.Equal(at) {
		t.Fatalf("row drifted: %+v (note=%v)", g, g.Note)
	}
	if got[1].Note != nil || got[1].Active {
		t.Fatalf("zero row drifted: %+v", got[1])
	}
	if g.CreatedAt().IsZero() {
		t.Fatal("timestamps must persist")
	}

	one, err := rio.From[Event]().Where("name = ?", "first").First(ctx, db)
	if err != nil || one.ID != 1 {
		t.Fatalf("First: %+v %v", one, err)
	}
	n, err := rio.From[Event]().Count(ctx, db)
	if err != nil || n != 2 {
		t.Fatalf("Count: %d %v", n, err)
	}
}

func (e Event) CreatedAt() time.Time { return e.At }

func TestInsertAllStreamsBlocks(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS bulk_items"); err != nil {
		t.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE bulk_items (
		id UInt64, label String, val Float64,
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rio.Exec(ctx, db, "DROP TABLE IF EXISTS bulk_items") })

	type BulkItem struct {
		ID    uint64 `rio:",pk,noautoincr"`
		Label string
		Val   float64
	}
	const total = 150_000 // spans multiple 64k blocks
	rows := make([]BulkItem, total)
	for i := range rows {
		rows[i] = BulkItem{ID: uint64(i + 1), Label: fmt.Sprintf("item-%d", i%97), Val: float64(i) * 0.5}
	}
	if err := rio.InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}
	n, err := rio.From[BulkItem]().Count(ctx, db)
	if err != nil || n != total {
		t.Fatalf("count = %d, err = %v", n, err)
	}
	sum, err := rio.From[BulkItem]().Where("id <= ?", 4).Pluck[float64](ctx, db, "val")
	if err != nil || len(sum) != 4 || sum[3] != 1.5 {
		t.Fatalf("pluck = %v %v", sum, err)
	}
}

func TestExceptionSurfacesAndConnectionRecovers(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	_, err := rio.Exec(ctx, db, "SELECT definitely_not_a_function()")
	var ex *Exception
	if !errors.As(err, &ex) || ex.Code == 0 {
		t.Fatalf("want *Exception, got %v", err)
	}
	// the pool must hand out a working connection afterwards
	var out []struct{ N uint64 }
	rowsOut, err := rio.Raw[struct{ N uint64 }]("SELECT 42 AS n").All(ctx, db)
	if err != nil || len(rowsOut) != 1 || rowsOut[0].N != 42 {
		t.Fatalf("recovery query: %v %v", rowsOut, err)
	}
	_ = out
}

func TestConcurrentQueries(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			type row struct{ X uint64 }
			out, err := rio.Raw[row]("SELECT ? + 0 AS x", uint64(i)).All(ctx, db)
			if err == nil && (len(out) != 1 || out[0].X != uint64(i)) {
				err = fmt.Errorf("got %v", out)
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
}

func TestSQLShim(t *testing.T) {
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := OpenSQL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS shim_t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE shim_t (v String, n Int64, at DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY n`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE IF EXISTS shim_t")
	if _, err := db.Exec("INSERT INTO shim_t (v, n, at) VALUES (?, ?, ?)", "x'y", int64(-5), "2026-09-01 10:00:00.000000+00:00"); err != nil {
		t.Fatal(err)
	}
	var v string
	var n int64
	var at time.Time
	if err := db.QueryRow("SELECT v, n, at FROM shim_t WHERE n = ?", int64(-5)).Scan(&v, &n, &at); err != nil {
		t.Fatal(err)
	}
	if v != "x'y" || n != -5 || !at.Equal(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("v=%q n=%d at=%v", v, n, at)
	}
}

func TestUnwrapServesSQLView(t *testing.T) {
	db := openTest(t)
	view := db.Unwrap()
	if view == nil {
		t.Fatal("Unwrap must serve the database/sql view")
	}
	if err := view.Ping(); err != nil {
		t.Fatal(err)
	}
}

// After sitting idle past the expiry, the pool must serve queries without
// surfacing a stale-connection error.
func TestIdleExpiryRecovers(t *testing.T) {
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := Open(context.Background(), dsn+"?conn_max_idle_time=50ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for round := range 3 {
		type row struct{ N uint64 }
		out, err := rio.Raw[row]("SELECT 7 AS n").All(ctx, db)
		if err != nil || len(out) != 1 || out[0].N != 7 {
			t.Fatalf("round %d: %v %v", round, out, err)
		}
		time.Sleep(120 * time.Millisecond) // past expiry every round
	}
}

func TestConnLifetimeRecovers(t *testing.T) {
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := Open(context.Background(), dsn+"?conn_max_lifetime=50ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for round := range 3 {
		type row struct{ N uint64 }
		out, err := rio.Raw[row]("SELECT 7 AS n").All(ctx, db)
		if err != nil || len(out) != 1 || out[0].N != 7 {
			t.Fatalf("round %d: %v %v", round, out, err)
		}
		time.Sleep(80 * time.Millisecond) // past every connection's lifetime
	}
}

type ExtType struct {
	ID     uint64 `rio:",pk,noautoincr"`
	Amount string // Decimal(18, 4)
	Big    string // Int128
	IP     string // IPv4
	IP6    string // IPv6
	Tag    string // LowCardinality(String)
	Peak   int64  // SimpleAggregateFunction(max, Int64)
}

// Decimals, 128-bit integers, IPs, LowCardinality strings, and simple
// aggregate wrappers must round-trip on both insert paths.
func TestExtendedTypesRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS ext_types"); err != nil {
		t.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE ext_types (
		id UInt64, amount Decimal(18, 4), big Int128, ip IPv4, ip6 IPv6,
		tag LowCardinality(String), peak SimpleAggregateFunction(max, Int64),
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY id`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rio.Exec(ctx, db, "DROP TABLE IF EXISTS ext_types") })

	rows := []ExtType{
		{ID: 1, Amount: "1234.5678", Big: "170141183460469231731687303715884105727", IP: "10.20.30.40", IP6: "2001:db8::1", Tag: "hot", Peak: 99},
		{ID: 2, Amount: "-0.0001", Big: "-170141183460469231731687303715884105728", IP: "255.255.255.255", IP6: "::1", Tag: "cold", Peak: -5},
	}
	if err := rio.InsertAll(ctx, db, rows); err != nil { // native block path
		t.Fatalf("InsertAll: %v", err)
	}
	if err := rio.Insert(ctx, db, &ExtType{ID: 3, Amount: "42.0000", Big: "7", IP: "192.168.1.1", IP6: "fe80::42", Tag: "hot", Peak: 1}); err != nil { // interpolated path
		t.Fatalf("Insert: %v", err)
	}

	got, err := rio.From[ExtType]().OrderBy("id").All(ctx, db)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d", len(got))
	}
	for i, want := range append(rows, ExtType{ID: 3, Amount: "42.0000", Big: "7", IP: "192.168.1.1", IP6: "fe80::42", Tag: "hot", Peak: 1}) {
		g := got[i]
		if g.Amount != want.Amount || g.Big != want.Big || g.IP != want.IP || g.IP6 != want.IP6 || g.Tag != want.Tag || g.Peak != want.Peak {
			t.Fatalf("row %d drifted:\n got %+v\nwant %+v", i, g, want)
		}
	}

	// Decimal comparisons through interpolated parameters.
	n, err := rio.From[ExtType]().Where("amount > ?", "100").Count(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("decimal where: %d %v", n, err)
	}
}

type TimeKeyed struct {
	ID uint64 `rio:",pk,noautoincr"`
	At time.Time
}

// Sorting-key time columns route range predicates through the primary-key
// analyzer, whose constant parser is stricter than ordinary comparisons.
func TestTimePredicateOnSortingKey(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS time_keyeds"); err != nil {
		t.Fatal(err)
	}
	if _, err := rio.Exec(ctx, db, `CREATE TABLE time_keyeds (
		id UInt64, at DateTime64(3, 'UTC'),
		created_at DateTime64(6, 'UTC'), updated_at DateTime64(6, 'UTC')
	) ENGINE = MergeTree() ORDER BY at`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rio.Exec(ctx, db, "DROP TABLE IF EXISTS time_keyeds") })

	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	rows := []TimeKeyed{
		{ID: 1, At: time.Date(1969, 6, 1, 0, 0, 0, 0, time.UTC)},
		{ID: 2, At: base},
		{ID: 3, At: base.Add(90 * time.Minute)},
	}
	if err := rio.InsertAll(ctx, db, rows); err != nil {
		t.Fatalf("InsertAll: %v", err)
	}

	got, err := rio.From[TimeKeyed]().Where("at >= ?", base).OrderBy("at").All(ctx, db)
	if err != nil {
		t.Fatalf("range over sorting key: %v", err)
	}
	if len(got) != 2 || !got[0].At.Equal(base) || got[1].ID != 3 {
		t.Fatalf("rows drifted: %+v", got)
	}
	// Pre-epoch bound plus a half-open window, still through the analyzer.
	n, err := rio.From[TimeKeyed]().Where("at >= ? AND at < ?", rows[0].At.Add(-time.Hour), base).Count(ctx, db)
	if err != nil || n != 1 {
		t.Fatalf("pre-epoch window: %d %v", n, err)
	}
}

// SELECT NULL produces a Nullable(Nothing) column; scanning it into a
// pointer must yield nil, not an unsupported-type error.
func TestSelectNullLiteral(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	type row struct {
		V *string
		N uint64
	}
	out, err := rio.Raw[row]("SELECT NULL AS v, 42 AS n").All(ctx, db)
	if err != nil || len(out) != 1 || out[0].V != nil || out[0].N != 42 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

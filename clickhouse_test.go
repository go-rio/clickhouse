package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-rio/rio"
)

// --- eager DSN validation ---

func TestOpenValidatesDSNEagerly(t *testing.T) {
	valid := []string{
		"clickhouse://127.0.0.1:9000",
		"clickhouse://default:secret@127.0.0.1:9000/analytics",
		"clickhouse://user:pass@h1:9000,h2:9000/db?dial_timeout=30s&read_timeout=5m&compress=lz4",
		"clickhouse://127.0.0.1:9440/db?secure=true",
		"http://127.0.0.1:8123/db",
		"https://user:pass@127.0.0.1:8443/db?secure=true&dial_timeout=10s",
	}
	for _, dsn := range valid {
		db, err := Open(dsn)
		if err != nil {
			t.Fatalf("Open(%q): %v", dsn, err)
		}
		_ = db.Close()
	}

	invalid := []string{
		"",
		"not a dsn",
		"clickhouse://127.0.0.1:port", // non-numeric port
		"clickhouse://127.0.0.1:9000?dial_timeout=notaduration", // malformed parameter
		"https://127.0.0.1:8443/db",                             // https requires secure=true
	}
	for _, dsn := range invalid {
		db, err := Open(dsn)
		if err == nil {
			_ = db.Close()
			t.Fatalf("Open(%q) must fail eagerly", dsn)
		}
		if !strings.Contains(err.Error(), "clickhouse: open:") {
			t.Fatalf("Open(%q) error must carry the package prefix: %v", dsn, err)
		}
	}
}

// Open validates but never dials: a DSN pointing at a dead port succeeds and
// only PingContext (or the first query) reports the connection failure —
// same contract as the other go-rio driver modules and database/sql itself.
func TestOpenDoesNotConnect(t *testing.T) {
	db, err := Open("clickhouse://127.0.0.1:1/nowhere?dial_timeout=200ms")
	if err != nil {
		t.Fatalf("Open must not dial: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.Unwrap().PingContext(ctx); err == nil {
		t.Fatal("ping against a dead port must fail")
	}
}

// --- New: dialect wiring and option pass-through ---

// stubDB is a minimal database/sql driver recording every statement, so the
// tests can assert what rio renders under the ClickHouse dialect without a
// server.
type stubDB struct {
	mu      sync.Mutex
	queries []string
	failErr error // returned by every Exec/Query when set
}

func (s *stubDB) logged() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queries...)
}

func (s *stubDB) record(q string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, q)
	return s.failErr
}

type stubConnector struct{ s *stubDB }

func (c stubConnector) Connect(context.Context) (driver.Conn, error) { return stubConn(c), nil }
func (c stubConnector) Driver() driver.Driver                        { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("stub: use OpenDB") }

type stubConn struct{ s *stubDB }

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("stub: no prepare") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("stub: no begin") }

func (c stubConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.s.record(q); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (c stubConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := c.s.record(q); err != nil {
		return nil, err
	}
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Columns() []string              { return []string{"id", "name"} }
func (emptyRows) Close() error                   { return nil }
func (emptyRows) Next(dest []driver.Value) error { return io.EOF }

type widget struct {
	ID   int64
	Name string
}

func TestNewSpeaksClickHouseDialect(t *testing.T) {
	s := &stubDB{}
	db := New(sql.OpenDB(stubConnector{s}))
	t.Cleanup(func() { _ = db.Close() })

	if _, err := rio.From[widget]().Where("name = ?", "x").All(context.Background(), db); err != nil {
		t.Fatalf("All: %v", err)
	}
	got := s.logged()[0]
	want := "SELECT `widgets`.`id`, `widgets`.`name` FROM `widgets` WHERE (name = ?)"
	if got != want {
		t.Fatalf("rendered SQL is not the ClickHouse grammar:\n got: %s\nwant: %s", got, want)
	}
}

func TestNewOptionCombinations(t *testing.T) {
	s := &stubDB{}
	fixed := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	var hookSaw []string
	db := New(sql.OpenDB(stubConnector{s}),
		rio.WithClock(func() time.Time { return fixed }),
		rio.WithTableNamer(func(name string) string { return "app_" + strings.ToLower(name) }),
		rio.WithQueryHook(recordingHook{&hookSaw}),
	)
	t.Cleanup(func() { _ = db.Close() })

	w := widget{ID: 7, Name: "gauge"}
	if err := rio.Insert(context.Background(), db, &w); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got := s.logged()[0]
	if !strings.Contains(got, "INSERT INTO `app_widget`") {
		t.Fatalf("WithTableNamer must reach the grammar: %s", got)
	}
	if len(hookSaw) == 0 || !strings.Contains(hookSaw[0], "insert") {
		t.Fatalf("WithQueryHook must observe the insert: %v", hookSaw)
	}
}

type recordingHook struct{ saw *[]string }

func (recordingHook) BeforeQuery(ctx context.Context, _ *rio.QueryEvent) context.Context { return ctx }
func (h recordingHook) AfterQuery(_ context.Context, e *rio.QueryEvent) {
	*h.saw = append(*h.saw, e.Op+":"+e.Query)
}

// WithStmtCache is a construction-time misuse on ClickHouse — clickhouse-go
// implements Prepare only for INSERT batching, so a cached SELECT would fail
// on first use. The core panics in rio.New; this pins that it surfaces
// through the driver module's constructors too.
func TestWithStmtCachePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("New with rio.WithStmtCache must panic on clickhouse")
		}
		if !strings.Contains(fmt.Sprint(r), "WithStmtCache") {
			t.Fatalf("panic must name the offending option: %v", r)
		}
	}()
	New(sql.OpenDB(stubConnector{&stubDB{}}), rio.WithStmtCache())
}

// --- no error translator, by design ---

// ClickHouse has no unique or foreign key constraints, so this module
// installs no translator: a server exception must come back untranslated —
// never rio.ErrDuplicateKey / rio.ErrForeignKeyViolated — with the
// *clickhouse.Exception intact in the chain for errors.As. Installing a
// translator here would break this documented dialect fact.
func TestNoErrorTranslatorInstalled(t *testing.T) {
	exc := &ch.Exception{Code: 60, Name: "UNKNOWN_TABLE", Message: "table widgets does not exist"}
	s := &stubDB{failErr: exc}
	db := New(sql.OpenDB(stubConnector{s}))
	t.Cleanup(func() { _ = db.Close() })

	_, err := rio.From[widget]().All(context.Background(), db)
	if err == nil {
		t.Fatal("the stub error must propagate")
	}
	if errors.Is(err, rio.ErrDuplicateKey) || errors.Is(err, rio.ErrForeignKeyViolated) {
		t.Fatalf("no rio sentinel may match on clickhouse: %v", err)
	}
	var got *ch.Exception
	if !errors.As(err, &got) || got.Code != 60 {
		t.Fatalf("*clickhouse.Exception must stay reachable via errors.As: %v", err)
	}
}

// --- README examples must compile ---

// compile-time shadow of the README usage snippets; never executed.
var _ = func() {
	db, err := Open("clickhouse://default@127.0.0.1:9000/analytics?compress=lz4")
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	type Event struct {
		ID   uint64
		Kind string
		At   time.Time
	}
	_ = rio.Insert(ctx, db, &Event{ID: 1, Kind: "click", At: time.Now()})
	_, _ = rio.From[Event]().Where("kind = ?", "click").Final().All(ctx, db)
	_, _ = rio.Exec(ctx, db, "ALTER TABLE events UPDATE kind = ? WHERE id = ?", "tap", 1)
	_ = db.Unwrap() // native batch & pool tuning live on the *sql.DB
}

// --- integration, gated by RIO_CLICKHOUSE_DSN ---

// openTestDB connects to RIO_CLICKHOUSE_DSN or skips, e.g.
// RIO_CLICKHOUSE_DSN="clickhouse://default@localhost:19000".
func openTestDB(t *testing.T) *rio.DB {
	t.Helper()
	dsn := os.Getenv("RIO_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("RIO_CLICKHOUSE_DSN not set")
	}
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open(%q): %v", dsn, err)
	}
	if err := db.Unwrap().Ping(); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type Reading struct {
	ID uint64
	At time.Time
	V  float64
}

func (Reading) TableName() string { return "rio_ch_readings" }

// TestIntegration smokes the full constructor→insert→read path against a
// real server, including rio's time encoding surviving a reload Equal.
func TestIntegration(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if _, err := rio.Exec(ctx, db, "DROP TABLE IF EXISTS rio_ch_readings"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := rio.Exec(ctx, db,
		"CREATE TABLE rio_ch_readings (id UInt64, at DateTime64(6, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = rio.Exec(ctx, db, "DROP TABLE IF EXISTS rio_ch_readings") })

	r := Reading{ID: 1, At: time.Date(2026, 7, 9, 3, 4, 5, 123456789, time.UTC), V: 1.5}
	if err := rio.Insert(ctx, db, &r); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := rio.Find[Reading](ctx, db, uint64(1))
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !got.At.Equal(r.At.Truncate(time.Microsecond)) {
		t.Fatalf("time round-trip drifted: wrote %v, read %v", r.At, got.At)
	}
}

// TestIntegrationQuoteAwareBinding pins the go.mod floor: clickhouse-go ≥
// v2.47.0 is the first release whose client-side binder skips string
// literals and comments while scanning for ?. On an older driver this query
// would have its quoted ? substituted and the real placeholder starved —
// a manual downgrade below the floor fails here first.
func TestIntegrationQuoteAwareBinding(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	type pair struct {
		Lit string
		Val string
	}
	got, err := rio.Raw[pair]("SELECT '?' AS lit, ? AS val -- trailing ? stays\n", "bound").First(ctx, db)
	if err != nil {
		t.Fatalf("quote-aware probe: %v", err)
	}
	if got.Lit != "?" || got.Val != "bound" {
		t.Fatalf("binder rewrote protected regions: %+v", got)
	}
}

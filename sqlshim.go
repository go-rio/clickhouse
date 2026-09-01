package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/go-rio/clickhouse/internal/chproto"
)

// OpenSQL opens a plain database/sql handle over the native protocol — the
// surface go-rio/migrate consumes. Placeholders are ? and interpolate
// client-side; there are no transactions or prepared statements, and
// affected-row counts are always zero, matching ClickHouse itself.
func OpenSQL(dsn string) (*sql.DB, error) {
	cfg, _, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(shimConnector{cfg: cfg}), nil
}

type shimConnector struct {
	cfg chproto.Config
}

func (c shimConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := chproto.Dial(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return &shimConn{conn: conn}, nil
}

func (c shimConnector) Driver() driver.Driver { return shimDriver{} }

type shimDriver struct{}

func (shimDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("clickhouse: use OpenSQL")
}

type shimConn struct {
	conn *chproto.Conn
}

func (c *shimConn) Prepare(string) (driver.Stmt, error) {
	// Unreachable: database/sql prefers the Context interfaces below.
	return nil, errors.New("clickhouse: prepared statements are not supported")
}

func (c *shimConn) Close() error { return c.conn.Close() }

func (c *shimConn) Begin() (driver.Tx, error) {
	return nil, errors.New("clickhouse: transactions are not supported")
}

func (c *shimConn) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }

// IsValid reports connection health so database/sql discards poisoned ones.
func (c *shimConn) IsValid() bool { return !c.conn.Broken() }

func (c *shimConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q, err := chproto.Interpolate(query, namedToAny(args))
	if err != nil {
		return nil, err
	}
	if err := c.conn.Exec(ctx, q); err != nil {
		return nil, badConnOr(err)
	}
	return driver.RowsAffected(0), nil
}

func (c *shimConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := chproto.Interpolate(query, namedToAny(args))
	if err != nil {
		return nil, err
	}
	rows, err := c.conn.Query(ctx, q)
	if err != nil {
		return nil, badConnOr(err)
	}
	return &shimRows{rows: rows}, nil
}

// badConnOr maps transmission failures to ErrBadConn so database/sql retries
// them on a fresh connection — the query never reached the server.
func badConnOr(err error) error {
	var send *chproto.SendError
	if errors.As(err, &send) {
		return driver.ErrBadConn
	}
	return err
}

func namedToAny(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

type shimRows struct {
	rows *chproto.Rows
}

func (r *shimRows) Columns() []string { return r.rows.Names() }

func (r *shimRows) Close() error { return r.rows.Close() }

func (r *shimRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	row := r.rows.Row()
	for i := range dest {
		dec := r.rows.Decoder(i)
		if dec.Null(row) {
			dest[i] = nil
			continue
		}
		switch dec.Kind() {
		case chproto.KindInt:
			dest[i] = dec.Int64At(row)
		case chproto.KindUint:
			// database/sql's reflect fallback converts non-canonical values.
			dest[i] = dec.Uint64At(row)
		case chproto.KindFloat:
			dest[i] = dec.Float64At(row)
		case chproto.KindBool:
			dest[i] = dec.BoolAt(row)
		case chproto.KindTime:
			dest[i] = dec.TimeAt(row)
		default:
			dest[i] = string(dec.BytesAt(row)) // owned copy
		}
	}
	return nil
}

var _ interface {
	driver.Pinger
	driver.ExecerContext
	driver.QueryerContext
	driver.Validator
} = (*shimConn)(nil)

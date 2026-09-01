package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-rio/clickhouse/internal/chproto"
	"github.com/go-rio/rio"
)

// nativeDB adapts the protocol pool to rio's NativeDB SPI.
type nativeDB struct {
	pool *chproto.Pool
}

// acquire runs do on a pooled connection, redialing once on SendError.
func (d *nativeDB) acquire(ctx context.Context, do func(*chproto.Conn) error) (*chproto.Conn, error) {
	for attempt := 0; ; attempt++ {
		c, err := d.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		if err = do(c); err == nil {
			return c, nil
		}
		d.pool.Release(c)
		var send *chproto.SendError
		if attempt > 0 || !errors.As(err, &send) {
			return nil, err
		}
	}
}

func (d *nativeDB) Query(ctx context.Context, sqlText string, args []any) (rio.NativeRows, error) {
	q, err := chproto.Interpolate(sqlText, args)
	if err != nil {
		return nil, err
	}
	var rows *chproto.Rows
	c, err := d.acquire(ctx, func(c *chproto.Conn) (err error) {
		rows, err = c.Query(ctx, q)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &nativeRows{pool: d.pool, conn: c, rows: rows}, nil
}

func (d *nativeDB) Exec(ctx context.Context, sqlText string, args []any) (int64, error) {
	q, err := chproto.Interpolate(sqlText, args)
	if err != nil {
		return 0, err
	}
	c, err := d.acquire(ctx, func(c *chproto.Conn) error {
		return c.Exec(ctx, q)
	})
	if err != nil {
		return 0, err
	}
	d.pool.Release(c)
	// ClickHouse reports no affected-row counts.
	return 0, nil
}

func (d *nativeDB) Begin(context.Context, *sql.TxOptions) (rio.NativeTx, error) {
	// Unreachable through rio — the dialect rejects transactions first.
	return nil, errors.New("clickhouse: transactions are not supported")
}

func (d *nativeDB) Close() error { return d.pool.Close() }

// CopyIn implements rio.NativeCopier by streaming native column blocks.
func (d *nativeDB) CopyIn(ctx context.Context, table []string, columns []string, next func() ([]any, error)) (int64, error) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	for i, seg := range table {
		if i > 0 {
			b.WriteByte('.')
		}
		quoteIdent(&b, seg)
	}
	b.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		quoteIdent(&b, col)
	}
	b.WriteString(") VALUES")

	var in *chproto.Insert
	c, err := d.acquire(ctx, func(c *chproto.Conn) (err error) {
		in, err = c.BeginInsert(ctx, b.String())
		return err
	})
	if err != nil {
		return 0, err
	}
	defer d.pool.Release(c)
	committed := false
	defer func() {
		if !committed {
			in.Abort()
		}
	}()
	var n int64
	for {
		vals, err := next()
		if err != nil {
			return n, err
		}
		if vals == nil {
			break
		}
		if err := in.Append(vals); err != nil {
			return n, err
		}
		n++
	}
	committed = true
	return n, in.Commit()
}

// scanStep is one column's row-invariant scan strategy.
type scanStep struct {
	dec      chproto.Decoder
	kind     chproto.Kind
	rawBytes bool // the cell takes SetBytes over an owned-string copy
}

// nativeRows adapts a protocol result stream to rio's NativeRows. rio passes
// the same dest slots every row, so the scan plan is built once.
type nativeRows struct {
	pool     *chproto.Pool
	conn     *chproto.Conn
	rows     *chproto.Rows
	plan     []scanStep
	released bool
	err      error // cached by Close
}

func (r *nativeRows) Columns() []string { return r.rows.Names() }

func (r *nativeRows) Next() bool { return r.rows.Next() }

func (r *nativeRows) Scan(dest ...any) error {
	if r.plan == nil {
		r.plan = make([]scanStep, len(dest))
		for i, d := range dest {
			cell, ok := d.(rio.NativeCell)
			if !ok {
				return fmt.Errorf("clickhouse: unsupported scan destination %T", d)
			}
			dec := r.rows.Decoder(i)
			sk := cell.ScanKind()
			r.plan[i] = scanStep{
				dec:      dec,
				kind:     dec.Kind(),
				rawBytes: sk == rio.NativeKindBytes || sk == rio.NativeKindJSON,
			}
		}
	}
	row := r.rows.Row()
	for i, d := range dest {
		step := &r.plan[i]
		cell := d.(rio.NativeCell)
		if step.dec.Null(row) {
			if err := cell.SetNull(); err != nil {
				return err
			}
			continue
		}
		var err error
		switch step.kind {
		case chproto.KindInt:
			err = cell.SetInt64(step.dec.Int64At(row))
		case chproto.KindUint:
			err = cell.SetUint64(step.dec.Uint64At(row))
		case chproto.KindFloat:
			err = cell.SetFloat64(step.dec.Float64At(row))
		case chproto.KindBool:
			err = cell.SetBool(step.dec.BoolAt(row))
		case chproto.KindTime:
			err = cell.SetTime(step.dec.TimeAt(row))
		case chproto.KindBytes:
			b := step.dec.BytesAt(row)
			if step.rawBytes {
				err = cell.SetBytes(b) // SetBytes never retains its argument
			} else {
				err = cell.SetString(string(b))
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *nativeRows) Err() error {
	if r.released {
		return r.err
	}
	return r.rows.Err()
}

// Close drains the stream and releases the connection, caching the final
// error first: rio reads Err after Close, when rows may be recycled.
func (r *nativeRows) Close() {
	if r.released {
		return
	}
	r.released = true
	r.err = r.rows.Close()
	r.pool.Release(r.conn)
}

// quoteIdent appends name as a backquoted identifier.
func quoteIdent(b *strings.Builder, name string) {
	b.WriteByte('`')
	for i := range len(name) {
		if name[i] == '`' || name[i] == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(name[i])
	}
	b.WriteByte('`')
}

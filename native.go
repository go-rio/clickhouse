package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-rio/clickhouse/internal/chproto"
	"github.com/go-rio/rio"
)

// chTimeFormat mirrors rio's ClickHouse time binding: the core binds
// time.Time fields as this text, and the copy path parses it back into
// column ticks.
const chTimeFormat = "2006-01-02 15:04:05.000000+00:00"

// nativeDB adapts the protocol pool to rio's NativeDB SPI.
type nativeDB struct {
	pool *chproto.Pool
}

func (d *nativeDB) Query(ctx context.Context, sqlText string, args []any) (rio.NativeRows, error) {
	q, err := chproto.Interpolate(sqlText, args)
	if err != nil {
		return nil, err
	}
	c, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.Query(ctx, q)
	if err != nil {
		d.pool.Release(c)
		return nil, err
	}
	return &nativeRows{pool: d.pool, conn: c, rows: rows}, nil
}

func (d *nativeDB) Exec(ctx context.Context, sqlText string, args []any) (int64, error) {
	q, err := chproto.Interpolate(sqlText, args)
	if err != nil {
		return 0, err
	}
	c, err := d.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	err = c.Exec(ctx, q)
	d.pool.Release(c)
	// ClickHouse reports no affected-row counts; rio's dialect rejects every
	// API whose contract needs one.
	return 0, err
}

func (d *nativeDB) Begin(context.Context, *sql.TxOptions) (rio.NativeTx, error) {
	// Unreachable through rio — the dialect rejects transactions first.
	return nil, errors.New("clickhouse: transactions are not supported")
}

func (d *nativeDB) Close() error { return d.pool.Close() }

// CopyIn implements rio.NativeCopier: InsertAll streams straight into native
// column blocks. ClickHouse never backfills, so every batch takes this path.
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

	c, err := d.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer d.pool.Release(c)
	in, err := c.BeginInsert(ctx, b.String())
	if err != nil {
		return 0, err
	}
	// rio binds time fields as chTimeFormat text; date/time columns parse it
	// back into ticks once per value.
	timeCol := make([]bool, len(in.Columns()))
	for i, col := range in.Columns() {
		t := strings.TrimPrefix(col.Type, "Nullable(")
		timeCol[i] = strings.HasPrefix(t, "DateTime") || strings.HasPrefix(t, "Date")
	}
	var n int64
	for {
		vals, err := next()
		if err != nil {
			return n, err
		}
		if vals == nil {
			break
		}
		for i, v := range vals {
			if !timeCol[i] {
				continue
			}
			if s, ok := v.(string); ok {
				t, err := time.Parse(chTimeFormat, s)
				if err != nil {
					return n, fmt.Errorf("clickhouse: time column %q: %w", in.Columns()[i].Name, err)
				}
				vals[i] = t
			}
		}
		if err := in.Append(vals); err != nil {
			return n, err
		}
		n++
	}
	if err := in.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

func quoteIdent(b *strings.Builder, name string) {
	b.WriteByte('`')
	for i := 0; i < len(name); i++ {
		if name[i] == '`' || name[i] == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(name[i])
	}
	b.WriteByte('`')
}

// nativeRows adapts a protocol result stream to rio's NativeRows: typed
// column decoders feed NativeCell sinks with no driver.Value detour.
type nativeRows struct {
	pool     *chproto.Pool
	conn     *chproto.Conn
	rows     *chproto.Rows
	names    []string
	released bool
}

func (r *nativeRows) Columns() []string {
	if r.names == nil {
		cols := r.rows.Columns()
		r.names = make([]string, len(cols))
		for i, c := range cols {
			r.names[i] = c.Name
		}
	}
	return r.names
}

func (r *nativeRows) Next() bool { return r.rows.Next() }

func (r *nativeRows) Scan(dest ...any) error {
	row := r.rows.Row()
	for i, d := range dest {
		dec := r.rows.Decoder(i)
		cell, ok := d.(rio.NativeCell)
		if !ok {
			if err := scanPlain(d, dec, row); err != nil {
				return err
			}
			continue
		}
		if dec.Null(row) {
			if err := cell.SetNull(); err != nil {
				return err
			}
			continue
		}
		var err error
		switch dec.Kind() {
		case chproto.KindInt:
			err = cell.SetInt64(dec.Int64At(row))
		case chproto.KindUint:
			err = cell.SetUint64(dec.Uint64At(row))
		case chproto.KindFloat:
			err = cell.SetFloat64(dec.Float64At(row))
		case chproto.KindBool:
			err = cell.SetBool(dec.BoolAt(row))
		case chproto.KindTime:
			err = cell.SetTime(dec.TimeAt(row))
		case chproto.KindBytes:
			b := dec.BytesAt(row)
			if cell.ScanKind() == rio.NativeKindBytes || cell.ScanKind() == rio.NativeKindJSON {
				err = cell.SetBytes(b) // SetBytes never retains its argument
			} else {
				err = cell.SetString(string(b)) // owned copy per the contract
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// scanPlain serves rio's one non-cell slot: the *int64 a count query's
// second column scans into (count(*) is a UInt64, never NULL).
func scanPlain(d any, dec chproto.Decoder, row int) error {
	p, ok := d.(*int64)
	if !ok {
		return fmt.Errorf("clickhouse: unsupported scan destination %T", d)
	}
	if dec.Kind() == chproto.KindInt {
		*p = dec.Int64At(row)
	} else {
		*p = int64(dec.Uint64At(row))
	}
	return nil
}

func (r *nativeRows) Err() error { return r.rows.Err() }

func (r *nativeRows) Close() {
	if r.released {
		return
	}
	r.released = true
	r.rows.Close()
	r.pool.Release(r.conn)
}

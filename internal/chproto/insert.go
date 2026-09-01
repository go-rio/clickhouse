package chproto

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// flushRows is the row threshold at which Append flushes a block.
const flushRows = 65536

// Insert streams native column blocks for one INSERT statement. The column
// set and types come from the server's schema sample, so encoders never
// guess. Append accumulates rows, Commit sends the trailing block and the
// end-of-data marker.
//
// A background reader consumes the server's response while data streams. If
// the server aborts mid-stream (a quota or parsing error), it stops reading
// our data and reports an Exception; without the reader both sides would
// deadlock — the server waiting for us to read, us blocked on a full send
// buffer. The reader unblocks a stuck writer by expiring its deadline.
type Insert struct {
	conn *Conn
	cols []Column
	encs []encoder
	rows int

	readDone chan error // buffered; the reader's terminal result
}

// BeginInsert sends "INSERT INTO ... VALUES" (no inline data) and consumes
// the response up to the server's schema sample block.
func (c *Conn) BeginInsert(ctx context.Context, query string) (*Insert, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.netc.SetDeadline(deadline)
	} else {
		c.netc.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(query); err != nil {
		return nil, c.fail(err)
	}
	for {
		pt, err := c.readUvarint()
		if err != nil {
			return nil, c.fail(err)
		}
		switch pt {
		case serverData: // schema sample
			if err := c.skipString(); err != nil {
				return nil, c.fail(err)
			}
			cols, err := c.readMetaColumns()
			if err != nil {
				return nil, c.fail(err)
			}
			in := &Insert{conn: c, cols: cols, encs: make([]encoder, len(cols)), readDone: make(chan error, 1)}
			for i, col := range cols {
				enc, err := newEncoder(col.Type)
				if err != nil {
					return nil, c.fail(err)
				}
				in.encs[i] = enc
			}
			go in.readLoop()
			return in, nil
		case serverException:
			return nil, c.readException()
		case serverProgress:
			if err := c.skipProgress(); err != nil {
				return nil, c.fail(err)
			}
		case serverLog, serverProfileEvents:
			if err := c.skipMetaBlock(); err != nil {
				return nil, c.fail(err)
			}
		case serverTableColumns:
			if err := c.skipString(); err != nil {
				return nil, c.fail(err)
			}
			if err := c.skipString(); err != nil {
				return nil, c.fail(err)
			}
		default:
			return nil, c.fail(fmt.Errorf("chproto: insert: unexpected packet %d", pt))
		}
	}
}

// readLoop drains server packets during the data stream and delivers the
// terminal outcome: nil on EndOfStream, the Exception on abort.
func (in *Insert) readLoop() {
	c := in.conn
	for {
		pt, err := c.readUvarint()
		if err != nil {
			in.readDone <- c.fail(err)
			return
		}
		switch pt {
		case serverEndOfStream:
			in.readDone <- nil
			return
		case serverException:
			err := c.readException()
			// Unblock a writer stuck on a full send buffer; the connection
			// is done for either way once the server aborted the insert.
			c.broken.Store(true)
			c.netc.SetWriteDeadline(time.Unix(1, 0))
			in.readDone <- err
			return
		case serverProgress:
			err = c.skipProgress()
		case serverProfileInfo:
			err = c.skipProfileInfo()
		case serverData, serverLog, serverProfileEvents:
			err = c.skipMetaBlock()
		case serverTableColumns:
			if err = c.skipString(); err == nil {
				err = c.skipString()
			}
		default:
			err = fmt.Errorf("chproto: insert: unexpected packet %d", pt)
		}
		if err != nil {
			in.readDone <- c.fail(err)
			return
		}
	}
}

// abortErr reports the reader's verdict when the writer hit err mid-stream:
// a server Exception explains a write failure better than the broken pipe it
// caused.
func (in *Insert) abortErr(err error) error {
	select {
	case rerr := <-in.readDone:
		if rerr != nil {
			return rerr
		}
	case <-time.After(time.Second):
	}
	return err
}

// Columns returns the target column descriptors in insertion order.
func (in *Insert) Columns() []Column { return in.cols }

// Append adds one row; vals is only read during the call. A full buffer
// flushes a block to the server.
func (in *Insert) Append(vals []any) error {
	if len(vals) != len(in.encs) {
		return fmt.Errorf("chproto: insert row has %d values for %d columns", len(vals), len(in.encs))
	}
	for i, v := range vals {
		if err := in.encs[i].append(v); err != nil {
			return in.conn.fail(fmt.Errorf("column %q: %w", in.cols[i].Name, err))
		}
	}
	in.rows++
	if in.rows >= flushRows {
		if err := in.flush(); err != nil {
			return in.abortErr(err)
		}
	}
	return nil
}

func (in *Insert) flush() error {
	c := in.conn
	c.writeUvarint(clientData)
	c.writeString("")
	c.writeBlockHeader(len(in.cols), in.rows)
	for i, enc := range in.encs {
		c.writeString(in.cols[i].Name)
		c.writeString(in.cols[i].Type)
		c.writeByte(0)
		enc.writeTo(c)
		enc.reset()
	}
	in.rows = 0
	return c.flush()
}

// Commit sends buffered rows and the end-of-data block, then waits for the
// reader's verdict. ClickHouse reports no row count; callers track their own.
func (in *Insert) Commit() error {
	c := in.conn
	if in.rows > 0 {
		if err := in.flush(); err != nil {
			return in.abortErr(c.fail(err))
		}
	}
	c.writeEmptyBlock()
	if err := c.flush(); err != nil {
		return in.abortErr(c.fail(err))
	}
	return <-in.readDone
}

// --- column encoders ---

// encoder buffers one column's values and writes the block payload.
type encoder interface {
	append(v any) error
	appendZero()
	writeTo(c *Conn)
	reset()
}

func newEncoder(typ string) (encoder, error) {
	switch typ {
	case "UInt8":
		return &intEnc{size: 1}, nil
	case "UInt16":
		return &intEnc{size: 2}, nil
	case "UInt32":
		return &intEnc{size: 4}, nil
	case "UInt64":
		return &intEnc{size: 8}, nil
	case "Int8":
		return &intEnc{size: 1}, nil
	case "Int16":
		return &intEnc{size: 2}, nil
	case "Int32":
		return &intEnc{size: 4}, nil
	case "Int64":
		return &intEnc{size: 8}, nil
	case "Bool":
		return &intEnc{size: 1, boolish: true}, nil
	case "Float32":
		return &floatEnc{}, nil
	case "Float64":
		return &floatEnc{wide: true}, nil
	case "String":
		return &strEnc{}, nil
	case "UUID":
		return &uuidEnc{}, nil
	case "Date":
		return &timeEnc{size: 2, conv: func(t time.Time) int64 {
			return t.Unix() / 86400
		}}, nil
	case "Date32":
		return &timeEnc{size: 4, conv: func(t time.Time) int64 {
			return t.Unix() / 86400
		}}, nil
	}
	switch {
	case strings.HasPrefix(typ, "Nullable(") && strings.HasSuffix(typ, ")"):
		inner, err := newEncoder(typ[len("Nullable(") : len(typ)-1])
		if err != nil {
			return nil, err
		}
		return &nullEnc{inner: inner}, nil
	case strings.HasPrefix(typ, "DateTime64("):
		precision, _, err := parseDateTime64(typ, time.UTC)
		if err != nil {
			return nil, err
		}
		mul := int64(1)
		for i := precision; i < 9; i++ {
			mul *= 10
		}
		return &timeEnc{size: 8, conv: func(t time.Time) int64 {
			return t.UnixNano() / mul
		}}, nil
	case typ == "DateTime" || strings.HasPrefix(typ, "DateTime("):
		return &timeEnc{size: 4, conv: func(t time.Time) int64 {
			return t.Unix()
		}}, nil
	case strings.HasPrefix(typ, "FixedString("):
		n, err := strconv.Atoi(typ[len("FixedString(") : len(typ)-1])
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("chproto: bad type %q", typ)
		}
		return &fixedStrEnc{n: n}, nil
	case strings.HasPrefix(typ, "Enum8("):
		vals, err := enumValues(typ[len("Enum8(") : len(typ)-1])
		if err != nil {
			return nil, err
		}
		return &enumEnc{size: 1, vals: vals}, nil
	case strings.HasPrefix(typ, "Enum16("):
		vals, err := enumValues(typ[len("Enum16(") : len(typ)-1])
		if err != nil {
			return nil, err
		}
		return &enumEnc{size: 2, vals: vals}, nil
	}
	return nil, fmt.Errorf("chproto: unsupported insert column type %q", typ)
}

// asInt64 converts any integer-shaped binding, including named types.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint:
		return int64(n), true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true
	}
	return 0, false
}

type intEnc struct {
	size    int
	boolish bool
	buf     []byte
}

func (e *intEnc) append(v any) error {
	if e.boolish {
		if b, ok := v.(bool); ok {
			if b {
				e.buf = append(e.buf, 1)
			} else {
				e.buf = append(e.buf, 0)
			}
			return nil
		}
	}
	n, ok := asInt64(v)
	if !ok {
		if b, ok := v.(bool); ok { // Bool column bound as 0/1 or true/false
			n = 0
			if b {
				n = 1
			}
		} else {
			return fmt.Errorf("cannot bind %T as an integer", v)
		}
	}
	switch e.size {
	case 8:
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(n))
	case 4:
		e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(n))
	case 2:
		e.buf = binary.LittleEndian.AppendUint16(e.buf, uint16(n))
	default:
		e.buf = append(e.buf, byte(n))
	}
	return nil
}

func (e *intEnc) appendZero() {
	var zero [8]byte
	e.buf = append(e.buf, zero[:e.size]...)
}

func (e *intEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *intEnc) reset()          { e.buf = e.buf[:0] }

type floatEnc struct {
	wide bool
	buf  []byte
}

func (e *floatEnc) append(v any) error {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	default:
		if i, ok := asInt64(v); ok {
			f = float64(i)
		} else {
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Float32 || rv.Kind() == reflect.Float64 {
				f = rv.Float()
			} else {
				return fmt.Errorf("cannot bind %T as a float", v)
			}
		}
	}
	if e.wide {
		e.buf = binary.LittleEndian.AppendUint64(e.buf, math.Float64bits(f))
	} else {
		e.buf = binary.LittleEndian.AppendUint32(e.buf, math.Float32bits(float32(f)))
	}
	return nil
}

func (e *floatEnc) appendZero() {
	size := 4
	if e.wide {
		size = 8
	}
	var zero [8]byte
	e.buf = append(e.buf, zero[:size]...)
}

func (e *floatEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *floatEnc) reset()          { e.buf = e.buf[:0] }

func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

type strEnc struct {
	arena []byte
	ends  []int
}

func (e *strEnc) append(v any) error {
	s, ok := asString(v)
	if !ok {
		return fmt.Errorf("cannot bind %T as a string", v)
	}
	e.arena = append(e.arena, s...)
	e.ends = append(e.ends, len(e.arena))
	return nil
}

func (e *strEnc) appendZero() { e.ends = append(e.ends, len(e.arena)) }

func (e *strEnc) writeTo(c *Conn) {
	start := 0
	for _, end := range e.ends {
		c.writeBytes(e.arena[start:end])
		start = end
	}
}

func (e *strEnc) reset() { e.arena, e.ends = e.arena[:0], e.ends[:0] }

type fixedStrEnc struct {
	n   int
	buf []byte
}

func (e *fixedStrEnc) append(v any) error {
	s, ok := asString(v)
	if !ok {
		return fmt.Errorf("cannot bind %T as a fixed string", v)
	}
	if len(s) > e.n {
		return fmt.Errorf("value of %d bytes exceeds FixedString(%d)", len(s), e.n)
	}
	e.buf = append(e.buf, s...)
	for i := len(s); i < e.n; i++ {
		e.buf = append(e.buf, 0)
	}
	return nil
}

func (e *fixedStrEnc) appendZero() {
	for range e.n {
		e.buf = append(e.buf, 0)
	}
}

func (e *fixedStrEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *fixedStrEnc) reset()          { e.buf = e.buf[:0] }

type enumEnc struct {
	size int
	vals map[string]int64
	buf  []byte
}

func enumValues(spec string) (map[string]int64, error) {
	names, err := parseEnum(spec)
	if err != nil {
		return nil, err
	}
	vals := make(map[string]int64, len(names))
	for v, name := range names {
		vals[string(name)] = v
	}
	return vals, nil
}

func (e *enumEnc) append(v any) error {
	var n int64
	if s, ok := asString(v); ok {
		n, ok = e.vals[s]
		if !ok {
			return fmt.Errorf("enum has no member %q", s)
		}
	} else if i, ok := asInt64(v); ok {
		n = i
	} else {
		return fmt.Errorf("cannot bind %T as an enum", v)
	}
	if e.size == 1 {
		e.buf = append(e.buf, byte(n))
	} else {
		e.buf = binary.LittleEndian.AppendUint16(e.buf, uint16(n))
	}
	return nil
}

func (e *enumEnc) appendZero() {
	var zero [2]byte
	e.buf = append(e.buf, zero[:e.size]...)
}

func (e *enumEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *enumEnc) reset()          { e.buf = e.buf[:0] }

type uuidEnc struct {
	buf []byte
}

func (e *uuidEnc) append(v any) error {
	s, ok := asString(v)
	if !ok || len(s) != 36 {
		return fmt.Errorf("cannot bind %T as a UUID", v)
	}
	var b [16]byte
	pos := 0
	for i := 0; i < 36; {
		if s[i] == '-' {
			i++
			continue
		}
		hi, ok1 := unhex(s[i])
		lo, ok2 := unhex(s[i+1])
		if !ok1 || !ok2 {
			return fmt.Errorf("bad UUID %q", s)
		}
		b[pos] = hi<<4 | lo
		pos++
		i += 2
	}
	// wire = two little-endian halves
	for j := 7; j >= 0; j-- {
		e.buf = append(e.buf, b[j])
	}
	for j := 15; j >= 8; j-- {
		e.buf = append(e.buf, b[j])
	}
	return nil
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func (e *uuidEnc) appendZero() {
	var zero [16]byte
	e.buf = append(e.buf, zero[:]...)
}

func (e *uuidEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *uuidEnc) reset()          { e.buf = e.buf[:0] }

// timeEnc accepts time.Time values; the adapter layer normalizes rio's text
// form before it reaches the encoder.
type timeEnc struct {
	size int
	conv func(time.Time) int64
	buf  []byte
}

func (e *timeEnc) append(v any) error {
	t, ok := v.(time.Time)
	if !ok {
		return fmt.Errorf("cannot bind %T as a time", v)
	}
	n := e.conv(t)
	switch e.size {
	case 8:
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(n))
	case 4:
		e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(n))
	default:
		e.buf = binary.LittleEndian.AppendUint16(e.buf, uint16(n))
	}
	return nil
}

func (e *timeEnc) appendZero() {
	var zero [8]byte
	e.buf = append(e.buf, zero[:e.size]...)
}

func (e *timeEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *timeEnc) reset()          { e.buf = e.buf[:0] }

type nullEnc struct {
	inner encoder
	mask  []byte
}

func (e *nullEnc) append(v any) error {
	if v == nil {
		e.mask = append(e.mask, 1)
		e.inner.appendZero()
		return nil
	}
	e.mask = append(e.mask, 0)
	return e.inner.append(v)
}

func (e *nullEnc) appendZero() {
	e.mask = append(e.mask, 1)
	e.inner.appendZero()
}

func (e *nullEnc) writeTo(c *Conn) {
	c.w.Write(e.mask)
	e.inner.writeTo(c)
}

func (e *nullEnc) reset() {
	e.mask = e.mask[:0]
	e.inner.reset()
}

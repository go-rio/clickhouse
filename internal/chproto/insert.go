package chproto

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"net/netip"
	"reflect"
	"strings"
	"time"
)

// flushRows is the row threshold at which Append flushes a block.
const flushRows = 65536

// zeros pads null slots and fixed-width zero values.
var zeros [16]byte

// Insert streams native column blocks for one INSERT statement, typed by the
// server's schema sample. A background reader must drain the server's
// response while data streams, or a mid-stream abort blocks both sides.
type Insert struct {
	conn *Conn
	cols []Column
	encs []encoder
	rows int
	stop func() bool // releases the statement's context watch

	readDone chan struct{} // closed when the reader exits; readErr is then final
	readErr  error
}

// BeginInsert sends "INSERT INTO ... VALUES" (no inline data) and consumes
// the response up to the server's schema sample block, which types the
// encoders. A failed transmission returns a SendError; a rejected statement
// returns the server's Exception.
func (c *Conn) BeginInsert(ctx context.Context, query string) (*Insert, error) {
	stop := c.watch(ctx)
	in, err := c.beginInsert(query)
	if err != nil {
		stop()
		return nil, err
	}
	in.stop = stop
	go in.readLoop()
	return in, nil
}

// beginInsert runs the statement up to the schema sample block.
func (c *Conn) beginInsert(query string) (*Insert, error) {
	if err := c.sendQuery(query); err != nil {
		return nil, c.fail(&SendError{Err: err})
	}
	for {
		pt, err := c.nextPacket()
		if err != nil {
			return nil, err
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
			in := &Insert{conn: c, cols: cols, encs: make([]encoder, len(cols)), readDone: make(chan struct{})}
			for i, col := range cols {
				enc, err := newEncoder(col.Type)
				if err != nil {
					return nil, c.fail(err)
				}
				in.encs[i] = enc
			}
			return in, nil
		case serverException:
			return nil, c.readException()
		default:
			return nil, c.fail(fmt.Errorf("chproto: insert: unexpected packet %d", pt))
		}
	}
}

// readLoop drains server packets during the data stream and records the
// terminal outcome: nil on EndOfStream, the Exception on abort.
func (in *Insert) readLoop() {
	defer close(in.readDone)
	c := in.conn
	for {
		pt, err := c.nextPacket()
		if err != nil {
			in.readErr = err
			return
		}
		switch pt {
		case serverEndOfStream:
			return
		case serverException:
			in.readErr = c.readException()
			c.abort() // unblock a writer stuck on a full send buffer
			return
		case serverData: // a stray sample echo carries no rows
			if err := c.skipMetaBlock(); err != nil {
				in.readErr = c.fail(err)
				return
			}
		default:
			in.readErr = c.fail(fmt.Errorf("chproto: insert: unexpected packet %d", pt))
			return
		}
	}
}

// Abort abandons the insert and poisons the connection — it cannot be
// reused mid-stream.
func (in *Insert) Abort() {
	defer in.stop()
	in.conn.abort()
	<-in.readDone
}

// abortErr prefers the reader's error, when one arrives, over the writer's.
func (in *Insert) abortErr(err error) error {
	select {
	case <-in.readDone:
		if in.readErr != nil {
			return in.readErr
		}
	case <-time.After(time.Second):
	}
	return err
}

// Columns returns the target column descriptors in insertion order.
func (in *Insert) Columns() []Column { return in.cols }

// Append adds one row; vals is only read during the call. A full buffer
// flushes a block to the server. A value that does not fit its column fails
// the insert and poisons the connection.
func (in *Insert) Append(vals []any) error {
	if len(vals) != len(in.encs) {
		return fmt.Errorf("chproto: insert row has %d values for %d columns", len(vals), len(in.encs))
	}
	for i, v := range vals {
		if err := in.encs[i].append(v); err != nil {
			return in.conn.fail(fmt.Errorf("column %q (%s): %w", in.cols[i].Name, in.cols[i].Type, err))
		}
	}
	in.rows++
	if in.rows >= flushRows {
		return in.flush()
	}
	return nil
}

// flush writes the buffered block.
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
	if err := c.flush(); err != nil {
		return in.abortErr(c.fail(err))
	}
	return nil
}

// Commit sends buffered rows plus the end-of-data block and waits for the
// reader's result: nil, or the server's Exception. ClickHouse reports no row
// count.
func (in *Insert) Commit() error {
	defer in.stop()
	c := in.conn
	if in.rows > 0 {
		if err := in.flush(); err != nil {
			return err
		}
	}
	c.writeEmptyBlock()
	if err := c.flush(); err != nil {
		return in.abortErr(c.fail(err))
	}
	<-in.readDone
	return in.readErr
}

// --- column encoders ---

// encoder buffers one column's values and writes the block payload.
type encoder interface {
	append(v any) error
	appendZero()
	writeTo(c *Conn)
	reset()
}

// newEncoder builds the encoder for a wire type string.
func newEncoder(typ string) (encoder, error) {
	switch typ {
	case "UInt8", "Bool":
		return &intEnc{size: 1}, nil
	case "UInt16":
		return &intEnc{size: 2}, nil
	case "UInt32":
		return &intEnc{size: 4}, nil
	case "UInt64":
		return &intEnc{size: 8}, nil
	case "Int8":
		return &intEnc{size: 1, signed: true}, nil
	case "Int16":
		return &intEnc{size: 2, signed: true}, nil
	case "Int32":
		return &intEnc{size: 4, signed: true}, nil
	case "Int64":
		return &intEnc{size: 8, signed: true}, nil
	case "Float32":
		return &floatEnc{}, nil
	case "Float64":
		return &floatEnc{wide: true}, nil
	case "String":
		return &strEnc{}, nil
	case "UUID":
		return &uuidEnc{}, nil
	case "Int128", "UInt128":
		return &int128Enc{}, nil
	case "IPv4":
		return &ipEnc{}, nil
	case "IPv6":
		return &ipEnc{v6: true}, nil
	case "Date":
		return &timeEnc{size: 2, dayBased: true}, nil
	case "Date32":
		return &timeEnc{size: 4, dayBased: true}, nil
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
		return &timeEnc{size: 8, unit: pow10Units(precision)}, nil
	case typ == "DateTime" || strings.HasPrefix(typ, "DateTime("):
		return &timeEnc{size: 4, unit: int64(time.Second)}, nil
	case strings.HasPrefix(typ, "FixedString("):
		n, err := parseFixedStringN(typ)
		if err != nil {
			return nil, err
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
	case strings.HasPrefix(typ, "Decimal"):
		size, scale, err := parseDecimal(typ)
		if err != nil {
			return nil, err
		}
		return &decimalEnc{size: size, scale: scale}, nil
	case strings.HasPrefix(typ, "SimpleAggregateFunction("):
		return newEncoder(simpleAggInner(typ))
	}
	return nil, fmt.Errorf("chproto: unsupported insert column type %q", typ)
}

// bufEnc owns the byte buffer every fixed-payload encoder shares.
type bufEnc struct {
	buf []byte
}

func (e *bufEnc) writeTo(c *Conn) { c.w.Write(e.buf) }
func (e *bufEnc) reset()          { e.buf = e.buf[:0] }
func (e *bufEnc) pad(n int)       { e.buf = append(e.buf, zeros[:n]...) }

// intEnc covers every fixed-width integer column and Bool.
type intEnc struct {
	bufEnc
	size   int
	signed bool
}

func (e *intEnc) append(v any) error {
	n, unsigned, ok := asInt64(v)
	if !ok {
		b, isBool := v.(bool)
		if !isBool {
			return fmt.Errorf("cannot bind %T as an integer", v)
		}
		if b { // Bool columns take true/false as well as 0/1
			n = 1
		}
	}
	if !e.fits(n, unsigned) {
		return fmt.Errorf("%v is out of range", v)
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

// fits reports whether n, an unsigned bit pattern when unsigned, is
// representable in the column.
func (e *intEnc) fits(n int64, unsigned bool) bool {
	if e.size == 8 {
		return e.signed != unsigned || n >= 0
	}
	if e.signed {
		lim := int64(1) << (e.size*8 - 1)
		if unsigned {
			return uint64(n) < uint64(lim)
		}
		return -lim <= n && n < lim
	}
	nonNegative := unsigned || n >= 0
	return nonNegative && uint64(n)>>(e.size*8) == 0
}

func (e *intEnc) appendZero() { e.pad(e.size) }

// floatEnc covers Float32/Float64; integer bindings convert.
type floatEnc struct {
	bufEnc
	wide bool
}

func (e *floatEnc) append(v any) error {
	f, ok := asFloat64(v)
	if !ok {
		i, unsigned, iok := asInt64(v)
		switch {
		case !iok:
			return fmt.Errorf("cannot bind %T as a float", v)
		case unsigned:
			f = float64(uint64(i))
		default:
			f = float64(i)
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
	if e.wide {
		e.pad(8)
	} else {
		e.pad(4)
	}
}

// strEnc stores values wire-ready: varint length then bytes.
type strEnc struct {
	bufEnc
}

func (e *strEnc) append(v any) error {
	s, ok := asString(v)
	if !ok {
		return fmt.Errorf("cannot bind %T as a string", v)
	}
	e.buf = binary.AppendUvarint(e.buf, uint64(len(s)))
	e.buf = append(e.buf, s...)
	return nil
}

func (e *strEnc) appendZero() { e.buf = append(e.buf, 0) }

// fixedStrEnc zero-pads values to n bytes and rejects longer ones.
type fixedStrEnc struct {
	bufEnc
	n int
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
	for range e.n - len(s) {
		e.buf = append(e.buf, 0)
	}
	return nil
}

func (e *fixedStrEnc) appendZero() {
	for n := e.n; n > 0; n -= len(zeros) {
		e.buf = append(e.buf, zeros[:min(n, len(zeros))]...)
	}
}

// enumEnc accepts member names or raw Enum8/Enum16 values.
type enumEnc struct {
	bufEnc
	size int
	vals map[string]int64
}

func (e *enumEnc) append(v any) error {
	var n int64
	if s, ok := asString(v); ok {
		n, ok = e.vals[s]
		if !ok {
			return fmt.Errorf("enum has no member %q", s)
		}
	} else if i, _, ok := asInt64(v); ok {
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

func (e *enumEnc) appendZero() { e.pad(e.size) }

// uuidEnc parses canonical 36-character text; the wire carries two
// little-endian halves.
type uuidEnc struct {
	bufEnc
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

func (e *uuidEnc) appendZero() { e.pad(16) }

// timeEnc covers Date, Date32, DateTime, and DateTime64. dayBased columns
// carry days since the epoch; the rest carry unit-nanosecond ticks.
type timeEnc struct {
	bufEnc
	size     int
	unit     int64 // nanoseconds per tick
	dayBased bool
}

func (e *timeEnc) append(v any) error {
	t, ok := v.(time.Time)
	if !ok {
		return fmt.Errorf("cannot bind %T as a time", v)
	}
	var n int64
	if e.dayBased {
		sec := t.Unix()
		n = sec / 86400
		if sec%86400 < 0 {
			n--
		}
	} else {
		tps := int64(time.Second) / e.unit
		n = t.Unix()*tps + int64(t.Nanosecond())/e.unit
	}
	// Date (UInt16 days) and DateTime (UInt32 seconds) are unsigned and
	// narrow; out-of-range instants would otherwise wrap silently.
	isDate := e.size == 2
	isDateTime := e.size == 4 && !e.dayBased
	switch {
	case isDate && (n < 0 || n > math.MaxUint16):
		return fmt.Errorf("chproto: %v is outside the Date range [1970-01-01, 2149-06-06]", t)
	case isDateTime && (n < 0 || n > math.MaxUint32):
		return fmt.Errorf("chproto: %v is outside the DateTime range [1970-01-01, 2106-02-07]", t)
	}
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

func (e *timeEnc) appendZero() { e.pad(e.size) }

// decimalEnc encodes Decimal columns from decimal text or numeric bindings
// into scaled little-endian integers.
type decimalEnc struct {
	bufEnc
	size  int
	scale int
}

func (e *decimalEnc) append(v any) error {
	var hi, lo uint64
	var neg bool
	switch {
	case isStringLike(v):
		s, _ := asString(v)
		var err error
		if hi, lo, neg, err = parseDecimalText(s, e.scale); err != nil {
			return err
		}
	default:
		if f, ok := asFloat64(v); ok {
			scaled := math.Round(f * math.Pow10(e.scale))
			if math.Abs(scaled) >= 9.2e18 {
				return fmt.Errorf("decimal value %v overflows the float64-bound path; bind it as a string", v)
			}
			// int64(scaled) is already the two's-complement magnitude; the
			// sign extension fills hi.
			n := int64(scaled)
			lo = uint64(n)
			hi = uint64(n >> 63)
		} else if n, _, ok := asInt64(v); ok {
			for range e.scale {
				n *= 10
			}
			lo = uint64(n)
			hi = uint64(n >> 63)
		} else {
			return fmt.Errorf("cannot bind %T as a decimal", v)
		}
	}
	if neg { // two's complement of the parsed magnitude
		lo = ^lo + 1
		hi = ^hi
		if lo == 0 {
			hi++
		}
	}
	switch e.size {
	case 4:
		e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(lo))
	case 8:
		e.buf = binary.LittleEndian.AppendUint64(e.buf, lo)
	default:
		e.buf = binary.LittleEndian.AppendUint64(e.buf, lo)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, hi)
	}
	return nil
}

func (e *decimalEnc) appendZero() { e.pad(e.size) }

// int128Enc encodes Int128/UInt128 from decimal text or integer bindings.
type int128Enc struct {
	bufEnc
}

func (e *int128Enc) append(v any) error {
	var hi, lo uint64
	if s, ok := asString(v); ok {
		var neg bool
		var err error
		if hi, lo, neg, err = parseDecimalText(s, 0); err != nil {
			return err
		}
		if neg {
			lo = ^lo + 1
			hi = ^hi
			if lo == 0 {
				hi++
			}
		}
	} else if n, unsigned, ok := asInt64(v); ok {
		lo = uint64(n)
		if !unsigned {
			hi = uint64(n >> 63)
		}
	} else {
		return fmt.Errorf("cannot bind %T as a 128-bit integer", v)
	}
	e.buf = binary.LittleEndian.AppendUint64(e.buf, lo)
	e.buf = binary.LittleEndian.AppendUint64(e.buf, hi)
	return nil
}

func (e *int128Enc) appendZero() { e.pad(16) }

// ipEnc encodes IPv4/IPv6 from address text.
type ipEnc struct {
	bufEnc
	v6 bool
}

func (e *ipEnc) append(v any) error {
	s, ok := asString(v)
	if !ok {
		return fmt.Errorf("cannot bind %T as an IP address", v)
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return err
	}
	if e.v6 {
		b := addr.As16()
		e.buf = append(e.buf, b[:]...)
		return nil
	}
	if !addr.Is4() {
		return fmt.Errorf("cannot bind %q into an IPv4 column", s)
	}
	b := addr.As4()
	e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]))
	return nil
}

func (e *ipEnc) appendZero() {
	if e.v6 {
		e.pad(16)
	} else {
		e.pad(4)
	}
}

// nullEnc prefixes any encoder with a null mask; nil bindings append a zero
// placeholder to the inner column.
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

// --- binding helpers ---

// asInt64 converts any integer-shaped binding, including named types;
// unsigned reports that n carries an unsigned bit pattern.
func asInt64(v any) (n int64, unsigned, ok bool) {
	switch n := v.(type) {
	case int64:
		return n, false, true
	case int:
		return int64(n), false, true
	case int32:
		return int64(n), false, true
	case int16:
		return int64(n), false, true
	case int8:
		return int64(n), false, true
	case uint64:
		return int64(n), true, true
	case uint32:
		return int64(n), true, true
	case uint16:
		return int64(n), true, true
	case uint8:
		return int64(n), true, true
	case uint:
		return int64(n), true, true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), false, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), true, true
	}
	return 0, false, false
}

func asFloat64(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		return f, true
	case float32:
		return float64(f), true
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Float32 || rv.Kind() == reflect.Float64 {
		return rv.Float(), true
	}
	return 0, false
}

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

func isStringLike(v any) bool {
	switch v.(type) {
	case string, []byte:
		return true
	}
	return reflect.ValueOf(v).Kind() == reflect.String
}

// enumValues inverts an enum spec into name → value.
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

// parseDecimalText converts "-123.45" into a 128-bit magnitude scaled to
// exactly scale fractional digits.
func parseDecimalText(s string, scale int) (hi, lo uint64, neg bool, err error) {
	bad := func() (uint64, uint64, bool, error) {
		return 0, 0, false, fmt.Errorf("cannot bind %q as a decimal", s)
	}
	rest := s
	hasSign := len(rest) > 0 && (rest[0] == '-' || rest[0] == '+')
	if hasSign {
		neg = rest[0] == '-'
		rest = rest[1:]
	}
	intPart, fracPart, hasDot := strings.Cut(rest, ".")
	if intPart == "" && fracPart == "" {
		return bad()
	}
	if hasDot && len(fracPart) > scale {
		return 0, 0, false, fmt.Errorf("%q has more than %d fractional digits", s, scale)
	}
	mulAdd := func(d byte) bool { // ×10 + d, reporting overflow
		var carry uint64
		hh, hl := bits.Mul64(hi, 10)
		lh, ll := bits.Mul64(lo, 10)
		if hh != 0 {
			return false
		}
		lo, carry = bits.Add64(ll, uint64(d), 0)
		hi, carry = bits.Add64(hl+lh, 0, carry)
		return carry == 0 && hl+lh >= hl
	}
	digits := 0
	for i := range len(intPart) {
		d := intPart[i] - '0'
		if d > 9 {
			return bad()
		}
		if !mulAdd(d) {
			return bad()
		}
		digits++
	}
	for i := range scale {
		var d byte
		if i < len(fracPart) {
			d = fracPart[i] - '0'
			if d > 9 {
				return bad()
			}
		}
		if !mulAdd(d) {
			return bad()
		}
	}
	if digits == 0 && !hasDot {
		return bad()
	}
	return hi, lo, neg, nil
}

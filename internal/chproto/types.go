package chproto

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// grow resizes buf to n elements, reallocating only when capacity lacks.
func grow[T any](buf []T, n int) []T {
	if cap(buf) < n {
		buf = make([]T, n)
	}
	return buf[:n]
}

// pow10Units returns the tick size in nanoseconds for a DateTime64 precision.
func pow10Units(precision int) int64 {
	unit := int64(1)
	for i := precision; i < 9; i++ {
		unit *= 10
	}
	return unit
}

// parseDecimal parses Decimal type strings into wire width and scale.
// "Decimal(P, S)" sizes by precision; Decimal32/64/128(S) are the aliases.
func parseDecimal(typ string) (size, scale int, err error) {
	bad := func() (int, int, error) { return 0, 0, fmt.Errorf("chproto: bad type %q", typ) }
	switch {
	case strings.HasPrefix(typ, "Decimal("):
		inner := strings.TrimSuffix(strings.TrimPrefix(typ, "Decimal("), ")")
		p, sStr, ok := strings.Cut(inner, ",")
		if !ok {
			return bad()
		}
		precision, err1 := strconv.Atoi(strings.TrimSpace(p))
		scale, err2 := strconv.Atoi(strings.TrimSpace(sStr))
		if err1 != nil || err2 != nil || scale < 0 || scale > precision {
			return bad()
		}
		switch {
		case precision <= 9:
			return 4, scale, nil
		case precision <= 18:
			return 8, scale, nil
		case precision <= 38:
			return 16, scale, nil
		}
		return 0, 0, fmt.Errorf("chproto: %s exceeds 38 digits (Decimal256 is not supported)", typ)
	case strings.HasPrefix(typ, "Decimal32("):
		size = 4
	case strings.HasPrefix(typ, "Decimal64("):
		size = 8
	case strings.HasPrefix(typ, "Decimal128("):
		size = 16
	default:
		return bad()
	}
	open := strings.IndexByte(typ, '(')
	scale, err = strconv.Atoi(strings.TrimSpace(typ[open+1 : len(typ)-1]))
	if err != nil || scale < 0 {
		return bad()
	}
	return size, scale, nil
}

// parseFixedStringN parses "FixedString(n)".
func parseFixedStringN(typ string) (int, error) {
	n, err := strconv.Atoi(typ[len("FixedString(") : len(typ)-1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("chproto: bad type %q", typ)
	}
	return n, nil
}

// fixedWidth reports a type's per-value wire width, or -1 for String.
func fixedWidth(typ string) (int, error) {
	switch typ {
	case "UInt8", "Int8", "Bool":
		return 1, nil
	case "UInt16", "Int16", "Date":
		return 2, nil
	case "UInt32", "Int32", "Float32", "DateTime", "Date32":
		return 4, nil
	case "UInt64", "Int64", "Float64":
		return 8, nil
	case "UUID", "Int128", "UInt128":
		return 16, nil
	case "IPv6":
		return 16, nil
	case "IPv4":
		return 4, nil
	case "Nothing":
		return 1, nil
	case "String":
		return -1, nil
	}
	switch {
	case strings.HasPrefix(typ, "DateTime64("):
		return 8, nil
	case strings.HasPrefix(typ, "DateTime("):
		return 4, nil
	case strings.HasPrefix(typ, "Enum8("):
		return 1, nil
	case strings.HasPrefix(typ, "Enum16("):
		return 2, nil
	case strings.HasPrefix(typ, "FixedString("):
		return parseFixedStringN(typ)
	case strings.HasPrefix(typ, "Decimal"):
		size, _, err := parseDecimal(typ)
		return size, err
	case strings.HasPrefix(typ, "SimpleAggregateFunction("):
		return fixedWidth(simpleAggInner(typ))
	}
	return 0, fmt.Errorf("chproto: unsupported column type %q", typ)
}

// skipColumnData discards one column's payload without decoding it.
func skipColumnData(c *Conn, typ string, rows int) error {
	if inner, ok := strings.CutPrefix(typ, "Nullable("); ok {
		if err := c.skipN(rows); err != nil { // null mask
			return err
		}
		return skipColumnData(c, inner[:len(inner)-1], rows)
	}
	width, err := fixedWidth(typ)
	if err != nil {
		return err
	}
	if width >= 0 {
		return c.skipN(rows * width)
	}
	for range rows {
		if err := c.skipString(); err != nil {
			return err
		}
	}
	return nil
}

// Kind is the access class a decoder serves; the SPI layer picks the
// matching typed sink once per column.
type Kind uint8

const (
	KindInt Kind = iota
	KindUint
	KindFloat
	KindBool
	KindBytes // String/FixedString/Enum/UUID — BytesAt borrows until the next read
	KindTime
)

// decoder loads one column per block and serves typed per-row access.
// Accessors must only be called with the row index of the current block, and
// borrowed bytes die at the next read.
type Decoder interface {
	read(c *Conn, rows int) error
	Kind() Kind
	Null(i int) bool
	Int64At(i int) int64
	Uint64At(i int) uint64
	Float64At(i int) float64
	BoolAt(i int) bool
	BytesAt(i int) []byte
	TimeAt(i int) time.Time
}

// base provides accessor stubs; concrete decoders override their class.
type base struct{}

func (base) Null(int) bool         { return false }
func (base) Int64At(int) int64     { panic("chproto: not an integer column") }
func (base) Uint64At(int) uint64   { panic("chproto: not an unsigned column") }
func (base) Float64At(int) float64 { panic("chproto: not a float column") }
func (base) BoolAt(int) bool       { panic("chproto: not a bool column") }
func (base) BytesAt(int) []byte    { panic("chproto: not a bytes column") }
func (base) TimeAt(int) time.Time  { panic("chproto: not a time column") }

// fixedInt covers every fixed-width integer column.
type fixedInt struct {
	base
	size   int // 1, 2, 4, 8
	signed bool
	isBool bool
	buf    []byte
}

func (d *fixedInt) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*d.size)
	return c.readFull(d.buf)
}

func (d *fixedInt) Kind() Kind {
	switch {
	case d.isBool:
		return KindBool
	case d.signed:
		return KindInt
	default:
		return KindUint
	}
}

func (d *fixedInt) Uint64At(i int) uint64 {
	switch d.size {
	case 8:
		return binary.LittleEndian.Uint64(d.buf[i*8:])
	case 4:
		return uint64(binary.LittleEndian.Uint32(d.buf[i*4:]))
	case 2:
		return uint64(binary.LittleEndian.Uint16(d.buf[i*2:]))
	default:
		return uint64(d.buf[i])
	}
}

func (d *fixedInt) Int64At(i int) int64 {
	switch d.size {
	case 8:
		return int64(binary.LittleEndian.Uint64(d.buf[i*8:]))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(d.buf[i*4:])))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(d.buf[i*2:])))
	default:
		return int64(int8(d.buf[i]))
	}
}

func (d *fixedInt) BoolAt(i int) bool { return d.buf[i] != 0 }

// fixedFloat covers Float32/Float64.
type fixedFloat struct {
	base
	wide bool // Float64
	buf  []byte
}

func (d *fixedFloat) read(c *Conn, rows int) error {
	size := 4
	if d.wide {
		size = 8
	}
	d.buf = grow(d.buf, rows*size)
	return c.readFull(d.buf)
}

func (d *fixedFloat) Kind() Kind { return KindFloat }

func (d *fixedFloat) Float64At(i int) float64 {
	if d.wide {
		return math.Float64frombits(binary.LittleEndian.Uint64(d.buf[i*8:]))
	}
	return float64(math.Float32frombits(binary.LittleEndian.Uint32(d.buf[i*4:])))
}

// strCol decodes String columns into one arena plus end offsets.
type strCol struct {
	base
	arena []byte
	ends  []int
}

func (d *strCol) read(c *Conn, rows int) error {
	d.arena = d.arena[:0]
	d.ends = grow(d.ends, rows)
	var err error
	for i := range rows {
		if d.arena, err = c.readStringInto(d.arena); err != nil {
			return err
		}
		d.ends[i] = len(d.arena)
	}
	return nil
}

func (d *strCol) Kind() Kind { return KindBytes }

func (d *strCol) BytesAt(i int) []byte {
	start := 0
	if i > 0 {
		start = d.ends[i-1]
	}
	return d.arena[start:d.ends[i]]
}

// fixedStr decodes FixedString(n) columns.
type fixedStr struct {
	base
	n   int
	buf []byte
}

func (d *fixedStr) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*d.n)
	return c.readFull(d.buf)
}

func (d *fixedStr) Kind() Kind           { return KindBytes }
func (d *fixedStr) BytesAt(i int) []byte { return d.buf[i*d.n : (i+1)*d.n] }

// enumCol maps Enum8/Enum16 wire values to their names.
type enumCol struct {
	base
	size  int // 1 or 2
	names map[int64][]byte
	buf   []byte
}

func (d *enumCol) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*d.size)
	return c.readFull(d.buf)
}

func (d *enumCol) Kind() Kind { return KindBytes }

func (d *enumCol) BytesAt(i int) []byte {
	var v int64
	if d.size == 1 {
		v = int64(int8(d.buf[i]))
	} else {
		v = int64(int16(binary.LittleEndian.Uint16(d.buf[i*2:])))
	}
	return d.names[v]
}

// uuidCol renders UUID columns as canonical text. The wire format is two
// little-endian 64-bit halves of the big-endian UUID.
type uuidCol struct {
	base
	buf []byte
	txt []byte
}

func (d *uuidCol) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*16)
	d.txt = grow(d.txt, 36)
	return c.readFull(d.buf)
}

func (d *uuidCol) Kind() Kind { return KindBytes }

func (d *uuidCol) BytesAt(i int) []byte {
	src := d.buf[i*16 : i*16+16]
	var b [16]byte
	for j := range 8 { // un-swap the two LE halves
		b[j] = src[7-j]
		b[8+j] = src[15-j]
	}
	txt := d.txt[:36]
	const hex = "0123456789abcdef"
	pos := 0
	for j, v := range b {
		switch j {
		case 4, 6, 8, 10:
			txt[pos] = '-'
			pos++
		}
		txt[pos] = hex[v>>4]
		txt[pos+1] = hex[v&0x0F]
		pos += 2
	}
	return txt
}

// timeCol covers Date, Date32, DateTime, and DateTime64. dayBased columns
// carry days since the epoch; the rest carry unit-nanosecond ticks.
type timeCol struct {
	base
	size     int   // wire width
	unit     int64 // nanoseconds per tick
	dayBased bool
	loc      *time.Location
	buf      []byte
}

func (d *timeCol) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*d.size)
	return c.readFull(d.buf)
}

func (d *timeCol) Kind() Kind { return KindTime }

func (d *timeCol) TimeAt(i int) time.Time {
	var v int64
	switch d.size {
	case 8:
		v = int64(binary.LittleEndian.Uint64(d.buf[i*8:]))
	case 4:
		v = int64(binary.LittleEndian.Uint32(d.buf[i*4:]))
	default:
		v = int64(binary.LittleEndian.Uint16(d.buf[i*2:]))
	}
	if d.dayBased {
		if d.size == 4 {
			v = int64(int32(v)) // Date32 is signed
		}
		return time.Unix(v*86400, 0).UTC()
	}
	return time.Unix(0, v*d.unit).In(d.loc)
}

// decimalCol decodes Decimal columns — scaled little-endian integers of 4,
// 8, or 16 bytes — into fixed-scale decimal text.
type decimalCol struct {
	base
	size  int
	scale int
	buf   []byte
	txt   []byte
}

func (d *decimalCol) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*d.size)
	return c.readFull(d.buf)
}

func (d *decimalCol) Kind() Kind { return KindBytes }

func (d *decimalCol) BytesAt(i int) []byte {
	var hi, lo uint64
	switch d.size {
	case 4:
		v := int64(int32(binary.LittleEndian.Uint32(d.buf[i*4:])))
		lo = uint64(v)
		hi = uint64(v >> 63)
	case 8:
		v := int64(binary.LittleEndian.Uint64(d.buf[i*8:]))
		lo = uint64(v)
		hi = uint64(v >> 63)
	default:
		lo = binary.LittleEndian.Uint64(d.buf[i*16:])
		hi = binary.LittleEndian.Uint64(d.buf[i*16+8:])
	}
	neg := int64(hi) < 0
	if neg {
		lo = ^lo + 1
		hi = ^hi
		if lo == 0 {
			hi++
		}
	}
	// format the unsigned 128-bit value in reverse, then point at the front
	d.txt = d.txt[:0]
	digits := 0
	for hi != 0 || lo != 0 || digits <= d.scale {
		var rem uint64
		hi, rem = hi/10, hi%10
		lo, rem = bits.Div64(rem, lo, 10)
		d.txt = append(d.txt, byte('0'+rem))
		digits++
		if digits == d.scale && d.scale > 0 {
			d.txt = append(d.txt, '.')
		}
	}
	if neg {
		d.txt = append(d.txt, '-')
	}
	slices.Reverse(d.txt)
	return d.txt
}

// int128Col decodes Int128/UInt128 into decimal text (Go has no native
// 128-bit integer; string fields carry them exactly).
type int128Col struct {
	base
	signed bool
	buf    []byte
	txt    []byte
}

func (d *int128Col) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows*16)
	return c.readFull(d.buf)
}

func (d *int128Col) Kind() Kind { return KindBytes }

func (d *int128Col) BytesAt(i int) []byte {
	lo := binary.LittleEndian.Uint64(d.buf[i*16:])
	hi := binary.LittleEndian.Uint64(d.buf[i*16+8:])
	neg := d.signed && int64(hi) < 0
	if neg {
		lo = ^lo + 1
		hi = ^hi
		if lo == 0 {
			hi++
		}
	}
	d.txt = d.txt[:0]
	for hi != 0 || lo != 0 || len(d.txt) == 0 {
		var rem uint64
		hi, rem = hi/10, hi%10
		lo, rem = bits.Div64(rem, lo, 10)
		d.txt = append(d.txt, byte('0'+rem))
	}
	if neg {
		d.txt = append(d.txt, '-')
	}
	slices.Reverse(d.txt)
	return d.txt
}

// ipCol decodes IPv4 (wire UInt32) and IPv6 (16 raw bytes) into text.
type ipCol struct {
	base
	v6  bool
	buf []byte
	txt []byte
}

func (d *ipCol) read(c *Conn, rows int) error {
	size := 4
	if d.v6 {
		size = 16
	}
	d.buf = grow(d.buf, rows*size)
	return c.readFull(d.buf)
}

func (d *ipCol) Kind() Kind { return KindBytes }

func (d *ipCol) BytesAt(i int) []byte {
	var addr netip.Addr
	if d.v6 {
		addr = netip.AddrFrom16([16]byte(d.buf[i*16 : i*16+16]))
	} else {
		v := binary.LittleEndian.Uint32(d.buf[i*4:])
		addr = netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	}
	d.txt = addr.AppendTo(d.txt[:0])
	return d.txt
}

// nothingCol carries the value-free column of NULL literals; the wrapping
// Nullable mask says everything, the payload is one placeholder byte a row.
type nothingCol struct {
	base
	buf []byte
}

func (d *nothingCol) read(c *Conn, rows int) error {
	d.buf = grow(d.buf, rows)
	return c.readFull(d.buf)
}

func (d *nothingCol) Kind() Kind         { return KindBytes }
func (d *nothingCol) BytesAt(int) []byte { return nil }

// nullCol wraps any decoder with a null mask.
type nullCol struct {
	inner Decoder
	mask  []byte
}

func (d *nullCol) read(c *Conn, rows int) error {
	d.mask = grow(d.mask, rows)
	if err := c.readFull(d.mask); err != nil {
		return err
	}
	return d.inner.read(c, rows)
}

func (d *nullCol) Kind() Kind            { return d.inner.Kind() }
func (d *nullCol) Null(i int) bool       { return d.mask[i] != 0 }
func (d *nullCol) Int64At(i int) int64   { return d.inner.Int64At(i) }
func (d *nullCol) Uint64At(i int) uint64 { return d.inner.Uint64At(i) }
func (d *nullCol) Float64At(i int) float64 {
	return d.inner.Float64At(i)
}
func (d *nullCol) BoolAt(i int) bool      { return d.inner.BoolAt(i) }
func (d *nullCol) BytesAt(i int) []byte   { return d.inner.BytesAt(i) }
func (d *nullCol) TimeAt(i int) time.Time { return d.inner.TimeAt(i) }

// newDecoder builds the decoder for a wire type string.
func newDecoder(typ string, tz *time.Location) (Decoder, error) {
	switch typ {
	case "UInt8":
		return &fixedInt{size: 1}, nil
	case "UInt16":
		return &fixedInt{size: 2}, nil
	case "UInt32":
		return &fixedInt{size: 4}, nil
	case "UInt64":
		return &fixedInt{size: 8}, nil
	case "Int8":
		return &fixedInt{size: 1, signed: true}, nil
	case "Int16":
		return &fixedInt{size: 2, signed: true}, nil
	case "Int32":
		return &fixedInt{size: 4, signed: true}, nil
	case "Int64":
		return &fixedInt{size: 8, signed: true}, nil
	case "Bool":
		return &fixedInt{size: 1, isBool: true}, nil
	case "Float32":
		return &fixedFloat{}, nil
	case "Float64":
		return &fixedFloat{wide: true}, nil
	case "String":
		return &strCol{}, nil
	case "UUID":
		return &uuidCol{}, nil
	case "Int128":
		return &int128Col{signed: true}, nil
	case "UInt128":
		return &int128Col{}, nil
	case "IPv4":
		return &ipCol{}, nil
	case "IPv6":
		return &ipCol{v6: true}, nil
	case "Nothing":
		return &nothingCol{}, nil
	case "Date":
		return &timeCol{size: 2, dayBased: true}, nil
	case "Date32":
		return &timeCol{size: 4, dayBased: true}, nil
	}
	switch {
	case strings.HasPrefix(typ, "Nullable(") && strings.HasSuffix(typ, ")"):
		inner, err := newDecoder(typ[len("Nullable("):len(typ)-1], tz)
		if err != nil {
			return nil, err
		}
		return &nullCol{inner: inner}, nil
	case strings.HasPrefix(typ, "DateTime64("):
		precision, loc, err := parseDateTime64(typ, tz)
		if err != nil {
			return nil, err
		}
		return &timeCol{size: 8, unit: pow10Units(precision), loc: loc}, nil
	case typ == "DateTime" || strings.HasPrefix(typ, "DateTime("):
		loc := tz
		if arg := typeArg(typ, "DateTime"); arg != "" {
			if l, err := time.LoadLocation(arg); err == nil {
				loc = l
			}
		}
		return &timeCol{size: 4, unit: int64(time.Second), loc: loc}, nil
	case strings.HasPrefix(typ, "FixedString("):
		n, err := parseFixedStringN(typ)
		if err != nil {
			return nil, err
		}
		return &fixedStr{n: n}, nil
	case strings.HasPrefix(typ, "Enum8("):
		names, err := parseEnum(typ[len("Enum8(") : len(typ)-1])
		if err != nil {
			return nil, err
		}
		return &enumCol{size: 1, names: names}, nil
	case strings.HasPrefix(typ, "Enum16("):
		names, err := parseEnum(typ[len("Enum16(") : len(typ)-1])
		if err != nil {
			return nil, err
		}
		return &enumCol{size: 2, names: names}, nil
	case strings.HasPrefix(typ, "Decimal"):
		size, scale, err := parseDecimal(typ)
		if err != nil {
			return nil, err
		}
		return &decimalCol{size: size, scale: scale}, nil
	case strings.HasPrefix(typ, "SimpleAggregateFunction("):
		return newDecoder(simpleAggInner(typ), tz)
	}
	return nil, fmt.Errorf("chproto: unsupported column type %q (rio's ClickHouse surface covers scalars, strings, enums, UUID, and date/time)", typ)
}

// simpleAggInner unwraps "SimpleAggregateFunction(fn, T)" to T; the wire
// carries plain T values.
func simpleAggInner(typ string) string {
	inner := strings.TrimSuffix(strings.TrimPrefix(typ, "SimpleAggregateFunction("), ")")
	if _, t, ok := strings.Cut(inner, ","); ok {
		return strings.TrimSpace(t)
	}
	return inner
}

// typeArg extracts X from "Name('X')", or returns "".
func typeArg(typ, name string) string {
	inner := strings.TrimSuffix(strings.TrimPrefix(typ, name+"("), ")")
	return strings.Trim(inner, "'")
}

// parseDateTime64 parses "DateTime64(p[, 'TZ'])".
func parseDateTime64(typ string, def *time.Location) (int, *time.Location, error) {
	inner := strings.TrimSuffix(strings.TrimPrefix(typ, "DateTime64("), ")")
	parts := strings.SplitN(inner, ",", 2)
	precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || precision < 0 || precision > 9 {
		return 0, nil, fmt.Errorf("chproto: bad type %q", typ)
	}
	loc := def
	if len(parts) == 2 {
		name := strings.Trim(strings.TrimSpace(parts[1]), "'")
		if l, err := time.LoadLocation(name); err == nil {
			loc = l
		}
	}
	return precision, loc, nil
}

// parseEnum parses "'a' = 1, 'b' = 2" into value → name.
func parseEnum(spec string) (map[int64][]byte, error) {
	names := make(map[int64][]byte)
	for len(spec) > 0 {
		q := strings.IndexByte(spec, '\'')
		if q < 0 {
			break
		}
		spec = spec[q+1:]
		end := 0
		var name []byte
		for end < len(spec) { // \' escapes inside the name
			if spec[end] == '\\' && end+1 < len(spec) {
				name = append(name, spec[end+1])
				end += 2
				continue
			}
			if spec[end] == '\'' {
				break
			}
			name = append(name, spec[end])
			end++
		}
		spec = spec[end+1:]
		eq := strings.IndexByte(spec, '=')
		if eq < 0 {
			return nil, fmt.Errorf("chproto: bad enum spec")
		}
		spec = spec[eq+1:]
		numEnd := 0
		for numEnd < len(spec) && spec[numEnd] != ',' {
			numEnd++
		}
		v, err := strconv.ParseInt(strings.TrimSpace(spec[:numEnd]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("chproto: bad enum value: %w", err)
		}
		names[v] = name
		spec = spec[numEnd:]
		if len(spec) > 0 {
			spec = spec[1:]
		}
	}
	return names, nil
}

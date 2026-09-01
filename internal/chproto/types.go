package chproto

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

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
	n := rows * d.size
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
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
	n := rows * size
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
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
	if cap(d.ends) < rows {
		d.ends = make([]int, rows)
	}
	d.ends = d.ends[:rows]
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
	n := rows * d.n
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
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
	n := rows * d.size
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
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
	n := rows * 16
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
	if cap(d.txt) < 36 {
		d.txt = make([]byte, 36)
	}
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

// timeCol covers Date, Date32, DateTime, and DateTime64.
type timeCol struct {
	base
	size  int // wire width
	unit  int64
	epoch func(int64) time.Time
	buf   []byte
}

func (d *timeCol) read(c *Conn, rows int) error {
	n := rows * d.size
	if cap(d.buf) < n {
		d.buf = make([]byte, n)
	}
	d.buf = d.buf[:n]
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
	return d.epoch(v)
}

// nullCol wraps any decoder with a null mask.
type nullCol struct {
	inner Decoder
	mask  []byte
}

func (d *nullCol) read(c *Conn, rows int) error {
	if cap(d.mask) < rows {
		d.mask = make([]byte, rows)
	}
	d.mask = d.mask[:rows]
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
	case "Date":
		return &timeCol{size: 2, epoch: func(v int64) time.Time {
			return time.Unix(v*86400, 0).UTC()
		}}, nil
	case "Date32":
		return &timeCol{size: 4, epoch: func(v int64) time.Time {
			return time.Unix(int64(int32(v))*86400, 0).UTC()
		}}, nil
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
		div := int64(1)
		for i := precision; i < 9; i++ {
			div *= 10
		}
		return &timeCol{size: 8, epoch: func(v int64) time.Time {
			return time.Unix(0, v*div).In(loc)
		}}, nil
	case typ == "DateTime" || strings.HasPrefix(typ, "DateTime("):
		loc := tz
		if arg := typeArg(typ, "DateTime"); arg != "" {
			if l, err := time.LoadLocation(arg); err == nil {
				loc = l
			}
		}
		return &timeCol{size: 4, epoch: func(v int64) time.Time {
			return time.Unix(v, 0).In(loc)
		}}, nil
	case strings.HasPrefix(typ, "FixedString("):
		n, err := strconv.Atoi(typ[len("FixedString(") : len(typ)-1])
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("chproto: bad type %q", typ)
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
	}
	return nil, fmt.Errorf("chproto: unsupported column type %q (rio's ClickHouse surface covers scalars, strings, enums, UUID, and date/time)", typ)
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

// discardColumn reads and drops one column payload (meta blocks).
func discardColumn(c *Conn, typ string, rows int) error {
	dec, err := newDecoder(typ, c.timezone)
	if err != nil {
		return err
	}
	return dec.read(c, rows)
}

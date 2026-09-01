package chproto

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

// Integer columns reject bindings outside their width and sign instead of
// wrapping; an unsigned bit pattern still passes into UInt64 and UInt128.
func TestIntEncRangeChecks(t *testing.T) {
	type myUint uint16
	for _, tc := range []struct {
		typ string
		v   any
		ok  bool
	}{
		{"UInt16", int64(70000), false},
		{"UInt16", int64(65535), true},
		{"UInt16", int64(-1), false},
		{"UInt16", myUint(65535), true},
		{"UInt8", uint64(256), false},
		{"UInt8", uint64(255), true},
		{"Int8", int64(-128), true},
		{"Int8", int64(-129), false},
		{"Int8", uint8(128), false},
		{"Int8", 127, true},
		{"Int32", int64(1 << 31), false},
		{"Int32", int64(-1 << 31), true},
		{"UInt32", uint64(1 << 32), false},
		{"UInt32", uint64(1<<32 - 1), true},
		{"UInt64", uint64(1 << 63), true},
		{"UInt64", int64(-1), false},
		{"Int64", uint64(1 << 63), false},
		{"Int64", uint64(1<<63 - 1), true},
		{"Int64", int64(-1), true},
		{"Nullable(UInt16)", int64(70000), false},
		{"Bool", true, true},
	} {
		enc, err := newEncoder(tc.typ)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.append(tc.v); (err == nil) != tc.ok {
			t.Fatalf("%s <- %v (%T): err = %v", tc.typ, tc.v, tc.v, err)
		}
	}
	u64, _ := newEncoder("UInt64")
	if err := u64.append(uint64(1 << 63)); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(u64.(*intEnc).buf); got != 1<<63 {
		t.Fatalf("UInt64 bit pattern = %#x", got)
	}
	u128, _ := newEncoder("UInt128")
	if err := u128.append(uint64(1 << 63)); err != nil {
		t.Fatal(err)
	}
	if b := u128.(*int128Enc).buf; binary.LittleEndian.Uint64(b[8:]) != 0 {
		t.Fatalf("UInt128 sign-extended an unsigned binding: %x", b)
	}
}

// DateTime64 encodes the server's whole range at precisions below 9, and
// day-based columns floor pre-1970 instants to the previous day.
func TestTimeEncWideRanges(t *testing.T) {
	for _, tc := range []struct {
		precision int
		t         time.Time
		want      int64
	}{
		{0, time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC), 10413791999},
		{3, time.Date(2299, 12, 31, 23, 59, 59, 999e6, time.UTC), 10413791999999},
		{8, time.Date(2299, 12, 31, 23, 59, 59, 999999990, time.UTC), 1041379199999999999},
		{6, time.Date(1900, 1, 1, 0, 0, 0, 1000, time.UTC), -2208988799999999},
		{3, time.Date(1969, 12, 31, 23, 59, 59, 500e6, time.UTC), -500},
		{0, time.Date(1969, 12, 31, 23, 59, 59, 500e6, time.UTC), -1},
		{9, time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), -1},
	} {
		enc := &timeEnc{size: 8, unit: pow10Units(tc.precision)}
		if err := enc.append(tc.t); err != nil {
			t.Fatal(err)
		}
		if got := int64(binary.LittleEndian.Uint64(enc.buf)); got != tc.want {
			t.Fatalf("precision %d, %v: got %d, want %d", tc.precision, tc.t, got, tc.want)
		}
	}
	date32 := &timeEnc{size: 4, dayBased: true}
	for _, tc := range []struct {
		t    time.Time
		want int32
	}{
		{time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC), -1},
		{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), -1},
		{time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC), 0},
		{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), -25567},
	} {
		date32.reset()
		if err := date32.append(tc.t); err != nil {
			t.Fatal(err)
		}
		if got := int32(binary.LittleEndian.Uint32(date32.buf)); got != tc.want {
			t.Fatalf("Date32 %v: got %d, want %d", tc.t, got, tc.want)
		}
	}
	date := &timeEnc{size: 2, dayBased: true}
	if err := date.append(time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("Date must reject the day before the epoch")
	}
}

// Date (UInt16 days) and DateTime (UInt32 seconds) reject instants that
// would wrap; the zero time.Time is the usual offender.
func TestTimeEncRejectsNarrowRanges(t *testing.T) {
	date := &timeEnc{size: 2, dayBased: true}
	dateTime := &timeEnc{size: 4, unit: int64(time.Second)}
	for _, tc := range []struct {
		enc  *timeEnc
		v    time.Time
		want string
	}{
		{date, time.Time{}, "Date range"},
		{date, time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), "Date range"},
		{dateTime, time.Time{}, "DateTime range"},
		{dateTime, time.Date(2110, 1, 1, 0, 0, 0, 0, time.UTC), "DateTime range"},
	} {
		if err := tc.enc.append(tc.v); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("append(%v): want %q error, got %v", tc.v, tc.want, err)
		}
	}
	if err := date.append(time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("valid Date rejected: %v", err)
	}
	if err := dateTime.append(time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("valid DateTime rejected: %v", err)
	}
	date32 := &timeEnc{size: 4, dayBased: true}
	if err := date32.append(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Date32 pre-epoch rejected: %v", err)
	}
}

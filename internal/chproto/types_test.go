package chproto

import (
	"encoding/binary"
	"testing"
	"time"
)

// DateTime64 decodes the server's whole 1900–2299 range at every precision
// below 9, and pre-1970 sub-second ticks stay exact.
func TestTimeColDateTime64Range(t *testing.T) {
	for _, tc := range []struct {
		precision int
		ticks     int64
		want      time.Time
	}{
		{0, 10413791999, time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC)},
		{3, 10413791999999, time.Date(2299, 12, 31, 23, 59, 59, 999e6, time.UTC)},
		{8, 1041379199999999999, time.Date(2299, 12, 31, 23, 59, 59, 999999990, time.UTC)},
		{6, -2208988799999999, time.Date(1900, 1, 1, 0, 0, 0, 1000, time.UTC)},
		{3, -500, time.Date(1969, 12, 31, 23, 59, 59, 500e6, time.UTC)},
		{9, -1, time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC)},
	} {
		d := &timeCol{size: 8, unit: pow10Units(tc.precision), loc: time.UTC}
		d.buf = binary.LittleEndian.AppendUint64(nil, uint64(tc.ticks))
		if got := d.TimeAt(0); !got.Equal(tc.want) {
			t.Fatalf("precision %d, ticks %d: got %v, want %v", tc.precision, tc.ticks, got, tc.want)
		}
	}
}

func TestParseEnum(t *testing.T) {
	names, err := parseEnum(`'increment' = 1, 'gauge' = 2, 'it\'s' = -3`)
	if err != nil {
		t.Fatal(err)
	}
	if string(names[1]) != "increment" || string(names[2]) != "gauge" || string(names[-3]) != "it's" {
		t.Fatalf("names = %v", names)
	}
}

func TestParseDateTime64(t *testing.T) {
	p, loc, err := parseDateTime64("DateTime64(6, 'UTC')", time.Local)
	if err != nil || p != 6 || loc != time.UTC {
		t.Fatalf("p=%d loc=%v err=%v", p, loc, err)
	}
	p, loc, err = parseDateTime64("DateTime64(3)", time.UTC)
	if err != nil || p != 3 || loc != time.UTC {
		t.Fatalf("p=%d loc=%v err=%v", p, loc, err)
	}
}

func TestNewDecoderRejects(t *testing.T) {
	for _, typ := range []string{"Array(String)", "Map(String, UInt64)", "Tuple(UInt8)", "LowCardinality(String)"} {
		if _, err := newDecoder(typ, time.UTC); err == nil {
			t.Fatalf("%s must be rejected", typ)
		}
	}
}

package chproto

import (
	"testing"
	"time"
)

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

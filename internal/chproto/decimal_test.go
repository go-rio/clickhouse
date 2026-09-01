package chproto

import (
	"encoding/binary"
	"testing"
)

func decRoundTrip(t *testing.T, size, scale int, in string, want string) {
	t.Helper()
	enc := &decimalEnc{size: size, scale: scale}
	if err := enc.append(in); err != nil {
		t.Fatalf("%s: %v", in, err)
	}
	dec := &decimalCol{size: size, scale: scale, buf: enc.buf}
	if got := string(dec.BytesAt(0)); got != want {
		t.Fatalf("%s: got %s, want %s", in, got, want)
	}
}

func TestDecimalRoundTrip(t *testing.T) {
	decRoundTrip(t, 8, 4, "1234.5678", "1234.5678")
	decRoundTrip(t, 8, 4, "-0.0001", "-0.0001")
	decRoundTrip(t, 8, 4, "42", "42.0000")
	decRoundTrip(t, 8, 4, "-999999999999.99", "-999999999999.9900")
	decRoundTrip(t, 4, 2, "12.34", "12.34")
	decRoundTrip(t, 4, 0, "-7", "-7")
	decRoundTrip(t, 16, 10, "12345678901234567890.0123456789", "12345678901234567890.0123456789")
	decRoundTrip(t, 16, 4, "-99999999999999999999999999999999.1234", "-99999999999999999999999999999999.1234")
	decRoundTrip(t, 8, 4, "0", "0.0000")
}

func TestDecimalFloatAndIntBindings(t *testing.T) {
	enc := &decimalEnc{size: 8, scale: 2}
	if err := enc.append(12.345); err != nil { // rounds
		t.Fatal(err)
	}
	if err := enc.append(int64(-3)); err != nil {
		t.Fatal(err)
	}
	if got := int64(binary.LittleEndian.Uint64(enc.buf[:8])); got != 1235 {
		t.Fatalf("float scaled = %d", got)
	}
	if got := int64(binary.LittleEndian.Uint64(enc.buf[8:])); got != -300 {
		t.Fatalf("int scaled = %d", got)
	}
}

func TestDecimalRejects(t *testing.T) {
	enc := &decimalEnc{size: 8, scale: 2}
	for _, bad := range []string{"", "-", "1.234", "12a", "."} {
		if err := enc.append(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	if _, _, err := parseDecimal("Decimal(40, 2)"); err == nil {
		t.Fatal("Decimal256 range must be rejected")
	}
}

func TestParseDecimalForms(t *testing.T) {
	for typ, want := range map[string][2]int{
		"Decimal(9, 2)":   {4, 2},
		"Decimal(18, 4)":  {8, 4},
		"Decimal(38, 10)": {16, 10},
		"Decimal32(2)":    {4, 2},
		"Decimal64(4)":    {8, 4},
		"Decimal128(6)":   {16, 6},
	} {
		size, scale, err := parseDecimal(typ)
		if err != nil || size != want[0] || scale != want[1] {
			t.Fatalf("%s: size=%d scale=%d err=%v", typ, size, scale, err)
		}
	}
}

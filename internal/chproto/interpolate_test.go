package chproto

import (
	"testing"
	"time"
)

func TestInterpolate(t *testing.T) {
	for _, tc := range []struct {
		query string
		args  []any
		want  string
	}{
		{"SELECT 1", nil, "SELECT 1"},
		{"a = ?", []any{int64(5)}, "a = 5"},
		{"a = ? AND b = ?", []any{int32(-3), uint64(18446744073709551615)}, "a = -3 AND b = 18446744073709551615"},
		{"s = ?", []any{"it's"}, `s = 'it\'s'`},
		{"s = ?", []any{`a\b`}, `s = 'a\\b'`},
		{"b = ?", []any{[]byte{0x68, 0x69}}, "b = 'hi'"},
		{"f = ?", []any{2.5}, "f = 2.5"},
		{"x = ? AND y = ?", []any{true, nil}, "x = true AND y = NULL"},
		{"t = ?", []any{time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)}, "t = fromUnixTimestamp64Micro(1788256800000000, 'UTC')"},
		// quoting contexts keep their question marks
		{"s = '?' AND a = ?", []any{int64(1)}, "s = '?' AND a = 1"},
		{"s = 'esc \\' ?' AND a = ?", []any{int64(1)}, "s = 'esc \\' ?' AND a = 1"},
		{"`col?` = ?", []any{int64(2)}, "`col?` = 2"},
		{`"col?" = ?`, []any{int64(2)}, `"col?" = 2`},
		{"-- is it? \na = ?", []any{int64(3)}, "-- is it? \na = 3"},
		{"/* ? /* nested ? */ */ a = ?", []any{int64(4)}, "/* ? /* nested ? */ */ a = 4"},
		// the rendered literal-question escape unwraps
		{`SELECT 'a' \? 'b'`, nil, `SELECT 'a' ? 'b'`},
	} {
		got, err := Interpolate(tc.query, tc.args)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		if got != tc.want {
			t.Fatalf("%q: got %q, want %q", tc.query, got, tc.want)
		}
	}
}

func TestInterpolateArity(t *testing.T) {
	if _, err := Interpolate("a = ?", nil); err == nil {
		t.Fatal("missing argument must fail")
	}
	if _, err := Interpolate("a = 1", []any{int64(1)}); err == nil {
		t.Fatal("excess argument must fail")
	}
	if _, err := Interpolate("s = 'open AND a = ?", []any{int64(1)}); err == nil {
		t.Fatal("unterminated literal must fail")
	}
}

func TestInterpolateNamedTypes(t *testing.T) {
	type myInt int32
	type myStr string
	got, err := Interpolate("a = ? AND s = ?", []any{myInt(7), myStr("x")})
	if err != nil || got != "a = 7 AND s = 'x'" {
		t.Fatalf("got %q err %v", got, err)
	}
}

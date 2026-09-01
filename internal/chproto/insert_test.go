package chproto

import (
	"strings"
	"testing"
	"time"
)

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

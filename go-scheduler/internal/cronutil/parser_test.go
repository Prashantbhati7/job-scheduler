package cronutil_test

import (
	"testing"
	"time"

	"github.com/Prashantbhati7/job-scheduler/go-scheduler/internal/cronutil"
)

func TestParseExpr_Valid(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"every minute", "* * * * *"},
		{"every 5 minutes", "*/5 * * * *"},
		{"daily at noon", "0 12 * * *"},
		{"weekdays at 9am", "0 9 * * 1-5"},
		{"first of month midnight", "0 0 1 * *"},
		{"descriptor @hourly", "@hourly"},
		{"descriptor @daily", "@daily"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cronutil.ParseExpr(tc.expr)
			if err != nil {
				t.Errorf("expected valid, got error: %v", err)
			}
		})
	}
}

func TestParseExpr_Invalid(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"too few fields", "* * * *"},
		{"too many fields", "* * * * * *"},
		{"bad dow value", "* * * * 8"},
		{"junk", "not-a-cron"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cronutil.ParseExpr(tc.expr)
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.expr)
			}
		})
	}
}

func TestNextRun_UTC(t *testing.T) {
	from := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
	next, err := cronutil.NextRun("0 12 * * *", "UTC", from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, next)
	}
}

func TestNextRun_NamedTimezone(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	from := time.Date(2024, 1, 1, 8, 0, 0, 0, loc)
	next, err := cronutil.NextRun("0 9 * * *", "America/New_York", from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedUTC := time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC)
	if !next.Equal(expectedUTC) {
		t.Errorf("expected %v UTC, got %v UTC", expectedUTC, next.UTC())
	}
}

func TestNextRun_UnknownTimezone(t *testing.T) {
	_, err := cronutil.NextRun("* * * * *", "Mars/Olympus_Mons", time.Now())
	if err == nil {
		t.Error("expected error for unknown timezone, got nil")
	}
}

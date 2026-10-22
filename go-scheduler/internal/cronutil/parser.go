package cronutil

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Parser wraps robfig/cron with standard 5-field (no seconds) expressions.

var parser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// parses 5-field cron expression and returns error on invalid input so the caller can reject bad job configs
// at insertion time rather than at run time.
func ParseExpr(expr string) (cron.Schedule, error) {
	sched, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return sched, nil
}


// Why use the job's timezone?
// The hard rule is: all DB timestamps are UTC. But the schedule is defined in the
// job's local time.convert the cron tick to UTC before writing it to `next_run_at`.
func NextRun(expr, timezone string, from time.Time) (time.Time, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("unknown timezone %q: %w", timezone, err)
	}

	sched, err := ParseExpr(expr)
	if err != nil {
		return time.Time{}, err
	}

	// Compute next tick in the job's local timezone, then convert to UTC.
	next := sched.Next(from.In(loc))
	return next.UTC(), nil
}

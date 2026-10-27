package retry

import (
	"math"
	"math/rand"
	"time"
)

const (
	DefaultBase   = 1 * time.Second
	DefaultMaxCap = 60 * time.Second
)

func CalculateBackoff(attempt int32, base, maxCap time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	exp := math.Min(float64(attempt), 30)
	ceiling := math.Min(float64(maxCap), float64(base)*math.Pow(2, exp))

	jitter := rand.Float64() * ceiling

	return time.Duration(jitter)
}

func NextRetryAt(attempt int32, base, maxCap time.Duration) time.Time {
	return time.Now().UTC().Add(CalculateBackoff(attempt, base, maxCap))
}

package clock

import "time"

// Clock abstracts time so producers can stamp telemetry deterministically in tests.
type Clock interface {
	Now() time.Time
}

// SystemClock returns UTC wall-clock time.
type SystemClock struct{}

func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

// FixedClock returns a constant instant.
type FixedClock struct {
	T time.Time
}

func (c FixedClock) Now() time.Time {
	return c.T.UTC()
}

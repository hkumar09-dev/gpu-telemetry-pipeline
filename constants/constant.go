package constants

import "time"

const (
	MAX_RETRY_ATTEMPTS = 10
	INITIAL_BACKOFF    = 200 * time.Millisecond
	MAX_BACKOFF        = 2 * time.Second
	TOPIC              = "gpu-telemetry"
)

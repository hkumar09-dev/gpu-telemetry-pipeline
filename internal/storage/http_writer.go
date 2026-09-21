package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

// HTTPWriter posts telemetry to the gateway ingest endpoint.
type HTTPWriter struct {
	BaseURL string
	Client  *http.Client
	Path    string
}

// NewHTTPWriter creates a new HTTPWriter.
func NewHTTPWriter(baseURL string) *HTTPWriter {
	return &HTTPWriter{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Path:    "/internal/v1/telemetry",
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *HTTPWriter) Write(ctx context.Context, t domain.Telemetry) error {
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}

	backoff := constants.INITIAL_BACKOFF
	var last error
	for attempt := 0; attempt < constants.MAX_RETRY_ATTEMPTS; attempt++ {
		// Don't start a new request if the caller has already cancelled.
		if err := ctx.Err(); err != nil {
			return err
		}

		last = w.post(ctx, body)
		if last == nil {
			return nil
		}

		if !retryable(last) {
			return err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
		// Exponential backoff:
		// 200ms → 400ms → 800ms → 1.6s → 2s → 2s...
		backoff *= 2
		if backoff > constants.MAX_BACKOFF {
			backoff = constants.MAX_BACKOFF
		}
	}

	return last
}

// can also use backoffWithJitter
//func backoffWithJitter(base time.Duration) time.Duration {
//	jitter := time.Duration(rand.Int63n(int64(base / 2)))
//	return base + jitter
//}

// post posts the telemetry to the gateway ingest endpoint.
func (w *HTTPWriter) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.BaseURL+w.Path, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ingest status %d", resp.StatusCode)
	}

	return nil
}

// retryable returns true if the error is retryable or transient.
// Returns false if the error is permanent or transient.
func retryable(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Non transient errors
	if errors.Is(err, context.Canceled) {
		return false
	}
	//msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "no such host"),
		strings.Contains(msg, "ingest status 502"),
		strings.Contains(msg, "ingest status 503"):
		return true
	default:
		// non-retryable error or not transient
		return false
	}
}

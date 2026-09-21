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
	backoff := 200 * time.Millisecond
	var last error
	for attempt := 0; attempt < 10; attempt++ {
		last = w.post(ctx, body)
		if last == nil || !retryable(last) {
			return last
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
	return last
}

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

func retryable(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return true
	case strings.Contains(msg, "connection reset"):
		return true
	case strings.Contains(msg, "no such host"):
		return true
	case strings.Contains(msg, "ingest status 502"):
		return true
	case strings.Contains(msg, "ingest status 503"):
		return true
	default:
		return false
	}
}

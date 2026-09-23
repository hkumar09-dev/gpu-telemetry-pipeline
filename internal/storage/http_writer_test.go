package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

func TestHTTPWriter(t *testing.T) {
	var got domain.Telemetry
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/telemetry" {
			t.Fatalf("path %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	err := w.Write(context.Background(), domain.Telemetry{
		UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC(), Value: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.UUID != "g" {
		t.Fatalf("%+v", got)
	}
}

func TestRetryable(t *testing.T) {
	if retryable(nil) {
		t.Fatal("nil")
	}
	if !retryable(fmt.Errorf("connection refused")) {
		t.Fatal("refused")
	}
	if !retryable(fmt.Errorf("ingest status 503")) {
		t.Fatal("503")
	}
	if !retryable(fmt.Errorf("ingest status 504")) {
		t.Fatal("504")
	}
	if retryable(fmt.Errorf("ingest status 400")) {
		t.Fatal("400 should not retry")
	}
	if retryable(context.Canceled) {
		t.Fatal("canceled")
	}
	if !retryable(fmt.Errorf("connection reset")) || !retryable(fmt.Errorf("no such host")) || !retryable(fmt.Errorf("ingest status 502")) {
		t.Fatal("retry strings")
	}
	if !retryable(context.DeadlineExceeded) {
		t.Fatal("deadline")
	}
	if retryable(fmt.Errorf("other")) {
		t.Fatal("other")
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestRetryableNetTimeout(t *testing.T) {
	if !retryable(timeoutErr{}) {
		t.Fatal("net timeout")
	}
	if !retryable(&net.DNSError{Err: "i/o timeout", IsTimeout: true}) {
		t.Fatal("dns timeout")
	}
}

func TestHTTPWriterRetryThenOK(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	err := w.Write(context.Background(), domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHTTPWriterNonRetryableStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	_ = w.Write(context.Background(), domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC()})
}

func TestHTTPWriterCancelDuringRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = w.Write(ctx, domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC()})
}

func TestHTTPWriterExhaustsRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	w.maxAttempts = 2
	w.initialBackoff = time.Millisecond
	if err := w.Write(context.Background(), domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC()}); err == nil {
		t.Fatal("expected last error")
	}
}

func TestHTTPWriterBackoffCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL)
	w.maxAttempts = 2
	w.initialBackoff = constants.MAX_BACKOFF
	_ = w.Write(context.Background(), domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: time.Now().UTC()})
}

func TestHTTPWriterPostDialError(t *testing.T) {
	w := NewHTTPWriter("http://127.0.0.1:1")
	w.Client = &http.Client{Timeout: 50 * time.Millisecond}
	_ = w.post(context.Background(), []byte(`{}`))
}

func TestHTTPWriterBadURL(t *testing.T) {
	w := NewHTTPWriter("http://[::1")
	_ = w.post(context.Background(), []byte(`{}`))
}

func TestHTTPWriterPostBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	w := NewHTTPWriter(srv.URL + "/")
	err := w.post(context.Background(), []byte(`{}`))
	if err == nil || err.Error() != "ingest status 400" {
		t.Fatalf("got %v", err)
	}
}

func TestHTTPWriterEnvTimeoutAndRetries(t *testing.T) {
	t.Setenv("HTTP_TIMEOUT", "50ms")
	t.Setenv("HTTP_MAX_RETRIES", "2")
	t.Setenv("HTTP_INITIAL_BACKOFF", "1ms")
	w := NewHTTPWriter("http://127.0.0.1:1")
	if w.Client.Timeout != 50*time.Millisecond {
		t.Fatalf("timeout %s", w.Client.Timeout)
	}
	if w.maxAttempts != 2 || w.initialBackoff != time.Millisecond {
		t.Fatalf("retries %d %s", w.maxAttempts, w.initialBackoff)
	}
}

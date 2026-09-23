package observe

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlerHealthReadyMetrics(t *testing.T) {
	st := &Status{}
	h := Handler(st.Ready)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != 200 {
		t.Fatalf("health %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready before %d", rec.Code)
	}
	st.SetReady(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != 200 {
		t.Fatalf("ready after %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("metrics %d", rec.Code)
	}
}

func TestListenNoopAndPort(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	stop, err := Listen(context.Background(), "-", nil, log)
	if err != nil || stop(context.Background()) != nil {
		t.Fatal("noop")
	}
	stop, err = Listen(context.Background(), "127.0.0.1:0", func() bool { return true }, log)
	if err != nil {
		t.Fatal(err)
	}
	shctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := stop(shctx); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPShutdownNil(t *testing.T) {
	if err := HTTPShutdown(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestListenBadAddr(t *testing.T) {
	_, err := Listen(context.Background(), "127.0.0.1:999999", nil, nil)
	if err == nil {
		t.Fatal("expected listen error")
	}
}

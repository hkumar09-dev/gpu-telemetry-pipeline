package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gpu-telemetry-pipeline/internal/storage"
)

func TestHandlerLivezAndIngestErrors(t *testing.T) {
	h := NewHandler(NewService(storage.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil))))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("livez %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/openapi.yaml", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Fatal("openapi")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/v1/telemetry", bytes.NewReader([]byte("{"))))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/v1/telemetry", bytes.NewReader([]byte(`{"uuid":"x"}`))))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid telemetry %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("404 got %d", rec.Code)
	}
}

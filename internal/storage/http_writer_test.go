package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

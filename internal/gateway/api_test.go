package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/domain"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/storage"
)

func TestListAndQuery(t *testing.T) {
	repo := storage.NewMemory()
	api := New(repo, nil)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)

	ts := time.Date(2026, 9, 17, 4, 0, 0, 0, time.UTC)
	body, _ := json.Marshal(domain.Telemetry{
		ProcessedAt: ts,
		MetricName:  "DCGM_FI_DEV_GPU_UTIL",
		UUID:        "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
		GPUIndex:    "0",
		Device:      "nvidia0",
		ModelName:   "NVIDIA H100 80GB HBM3",
		Hostname:    "mtv5-dgx1-hgpu-031",
		Value:       0,
	})
	resp, err := http.Post(srv.URL+"/internal/v1/telemetry", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest %d", resp.StatusCode)
	}

	gresp, err := http.Get(srv.URL + "/api/v1/gpus")
	if err != nil {
		t.Fatal(err)
	}
	defer gresp.Body.Close()
	var gpus []domain.GPU
	if err := json.NewDecoder(gresp.Body).Decode(&gpus); err != nil {
		t.Fatal(err)
	}
	if len(gpus) != 1 {
		t.Fatalf("gpus %+v", gpus)
	}

	q := srv.URL + "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2026-09-17T03:00:00Z&end_time=2026-09-17T05:00:00Z"
	tresp, err := http.Get(q)
	if err != nil {
		t.Fatal(err)
	}
	defer tresp.Body.Close()
	var rows []domain.Telemetry
	if err := json.NewDecoder(tresp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("telemetry %+v", rows)
	}
}

func TestBadTimeFilter(t *testing.T) {
	api := New(storage.NewMemory(), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/gpus/x/telemetry?start_time=not-a-date", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code %d", rec.Code)
	}
}

func TestHealthAndOpenAPI(t *testing.T) {
	api := New(storage.NewMemory(), nil)
	h := api.Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 {
		t.Fatal(rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if rec.Code != 200 || rec.Body.Len() == 0 {
		t.Fatal("expected openapi body")
	}
}

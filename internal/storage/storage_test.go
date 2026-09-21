package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

func TestMemoryQueryWindow(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t1.Add(2 * time.Hour)
	_ = m.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "u", ProcessedAt: t1, Value: 1, Hostname: "h", GPUIndex: "0"})
	_ = m.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "u", ProcessedAt: t2, Value: 2, Hostname: "h", GPUIndex: "0"})
	_ = m.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "u", ProcessedAt: t3, Value: 3, Hostname: "h", GPUIndex: "0"})
	_ = m.Save(ctx, domain.Telemetry{UUID: "g2", MetricName: "u", ProcessedAt: t2, Value: 9, Hostname: "h", GPUIndex: "1"})

	start, end := t2, t2
	rows, err := m.QueryByGPU(ctx, "g1", domain.TimeWindow{Start: &start, End: &end})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Value != 2 {
		t.Fatalf("window %+v", rows)
	}
	gpus, err := m.ListGPUs(ctx)
	if err != nil || len(gpus) != 2 {
		t.Fatalf("gpus %v %v", gpus, err)
	}
}

func TestMemoryCapsRecords(t *testing.T) {
	m := NewMemory()
	m.maxRecords = 3
	ctx := context.Background()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_ = m.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "u", ProcessedAt: ts.Add(time.Duration(i) * time.Second), Value: float64(i), Hostname: "h", GPUIndex: "0"})
	}
	rows, err := m.QueryByGPU(ctx, "g1", domain.TimeWindow{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Value != 2 || rows[2].Value != 4 {
		t.Fatalf("capped %+v", rows)
	}
}

func TestFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.json")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	rec := domain.Telemetry{
		ProcessedAt: ts,
		MetricName:  "DCGM_FI_DEV_GPU_TEMP",
		GPUIndex:    "0",
		Device:      "nvidia0",
		UUID:        "GPU-xyz",
		ModelName:   "H100",
		Hostname:    "node-1",
		Value:       71,
	}
	if err := db.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	gpus, err := db.ListGPUs(ctx)
	if err != nil || len(gpus) != 1 || gpus[0].ID != "GPU-xyz" {
		t.Fatalf("gpus %+v err %v", gpus, err)
	}
	start := ts.Add(-time.Second)
	end := ts.Add(time.Second)
	rows, err := db.QueryByGPU(ctx, "GPU-xyz", domain.TimeWindow{Start: &start, End: &end})
	if err != nil || len(rows) != 1 || rows[0].Value != 71 {
		t.Fatalf("rows %+v err %v", rows, err)
	}
}

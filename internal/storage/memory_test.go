package storage

import (
	"context"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

func TestMemoryWriteAndSort(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = m.Write(ctx, domain.Telemetry{UUID: "b", MetricName: "m", ProcessedAt: ts, Hostname: "z", GPUIndex: "1"})
	_ = m.Write(ctx, domain.Telemetry{UUID: "a", MetricName: "m", ProcessedAt: ts, Hostname: "a", GPUIndex: "0"})
	gpus, err := m.ListGPUs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gpus) != 2 || gpus[0].ID != "a" || gpus[1].ID != "b" {
		t.Fatalf("sort %+v", gpus)
	}
	_ = m.Write(ctx, domain.Telemetry{UUID: "c", MetricName: "m", ProcessedAt: ts, Hostname: "a", GPUIndex: "1"})
	gpus, err = m.ListGPUs(ctx)
	if err != nil || len(gpus) != 3 || gpus[0].GPUIndex != "0" {
		t.Fatalf("same host sort %+v %v", gpus, err)
	}
}

func TestNewMemorySize(t *testing.T) {
	m := NewMemorySize(1)
	ctx := context.Background()
	ts := time.Now().UTC()
	_ = m.Save(ctx, domain.Telemetry{UUID: "a", MetricName: "m", ProcessedAt: ts})
	_ = m.Save(ctx, domain.Telemetry{UUID: "b", MetricName: "m", ProcessedAt: ts})
	rows, err := m.QueryByGPU(ctx, "a", domain.TimeWindow{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("expected cap to drop a, got %+v %v", rows, err)
	}
}

func TestMemoryDuplicateAndCancel(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row := domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: ts, Value: 1}
	if err := m.Save(ctx, row); err != nil {
		t.Fatal(err)
	}
	row.Value = 99
	if err := m.Save(ctx, row); err != nil {
		t.Fatal(err)
	}
	rows, err := m.QueryByGPU(ctx, "g", domain.TimeWindow{})
	if err != nil || len(rows) != 1 || rows[0].Value != 1 {
		t.Fatalf("duplicate should be idempotent, got %+v %v", rows, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.Save(canceled, row); err == nil {
		t.Fatal("expected cancel")
	}
	if _, err := m.ListGPUs(canceled); err == nil {
		t.Fatal("expected list cancel")
	}
	if _, err := m.QueryByGPU(canceled, "g", domain.TimeWindow{}); err == nil {
		t.Fatal("expected query cancel")
	}
}

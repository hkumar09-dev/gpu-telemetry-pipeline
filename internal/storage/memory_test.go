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

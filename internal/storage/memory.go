package storage

import (
	"context"
	"sort"
	"sync"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

const defaultMaxTelemetry = 20000

// Memory is an in-memory repository used by unit tests.
type Memory struct {
	mu         sync.RWMutex
	gpus       map[string]domain.GPU
	telemetry  []domain.Telemetry
	maxRecords int
}

// NewMemory creates a new in-memory storage.
func NewMemory() *Memory {
	return &Memory{gpus: make(map[string]domain.GPU), maxRecords: defaultMaxTelemetry}
}

// Save stores a telemetry record.
func (m *Memory) Save(ctx context.Context, t domain.Telemetry) error {
	gpu := t.GPU()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gpus[t.UUID] = gpu
	m.telemetry = append(m.telemetry, t)
	if m.maxRecords > 0 && len(m.telemetry) > m.maxRecords {
		m.telemetry = append([]domain.Telemetry(nil), m.telemetry[len(m.telemetry)-m.maxRecords:]...)
	}
	return nil
}

// Write stores a telemetry record.
func (m *Memory) Write(ctx context.Context, t domain.Telemetry) error {
	return m.Save(ctx, t)
}

// ListGPUs lists all GPUs.
func (m *Memory) ListGPUs(_ context.Context) ([]domain.GPU, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.GPU, 0, len(m.gpus))
	for _, g := range m.gpus {
		out = append(out, g)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname == out[j].Hostname {
			return out[i].GPUIndex < out[j].GPUIndex
		}
		return out[i].Hostname < out[j].Hostname
	})

	return out, nil
}

// QueryByGPU queries telemetry records for a specific GPU.
func (m *Memory) QueryByGPU(_ context.Context, gpuID string, window domain.TimeWindow) ([]domain.Telemetry, error) {
	var out []domain.Telemetry
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, t := range m.telemetry {
		if t.UUID != gpuID {
			continue
		}
		if !window.Contains(t.ProcessedAt) {
			continue
		}
		out = append(out, t)
	}

	// Sort by processed time
	sort.SliceStable(out, func(i, j int) bool { return out[i].ProcessedAt.Before(out[j].ProcessedAt) })
	return out, nil
}

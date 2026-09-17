package storage

import (
	"context"
	"sort"
	"sync"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/domain"
)

// Memory is an in-memory repository used by unit tests.
type Memory struct {
	mu         sync.RWMutex
	gpus       map[string]domain.GPU
	telemetry  []domain.Telemetry
}

func NewMemory() *Memory {
	return &Memory{gpus: make(map[string]domain.GPU)}
}

func (m *Memory) Save(_ context.Context, t domain.Telemetry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gpus[t.UUID] = t.GPU()
	cp := t
	m.telemetry = append(m.telemetry, cp)
	return nil
}

func (m *Memory) Write(ctx context.Context, t domain.Telemetry) error {
	return m.Save(ctx, t)
}

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

func (m *Memory) QueryByGPU(_ context.Context, gpuID string, window domain.TimeWindow) ([]domain.Telemetry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []domain.Telemetry
	for _, t := range m.telemetry {
		if t.UUID != gpuID {
			continue
		}
		if !window.Contains(t.ProcessedAt) {
			continue
		}
		out = append(out, t)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ProcessedAt.Before(out[j].ProcessedAt) })
	return out, nil
}

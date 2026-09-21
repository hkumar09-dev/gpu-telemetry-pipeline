package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

type snapshot struct {
	GPUs      map[string]domain.GPU `json:"gpus"`
	Telemetry []domain.Telemetry    `json:"telemetry"`
}

// File is a durable JSON-backed repository. Suitable for a single gateway replica + PVC.
type File struct {
	path string
	mem  *Memory
	mu   sync.Mutex
}

func Open(path string) (*File, error) {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	s := &File{path: path, mem: NewMemory()}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *File) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("decode store: %w", err)
	}
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()
	if snap.GPUs != nil {
		s.mem.gpus = snap.GPUs
	}
	s.mem.telemetry = snap.Telemetry
	return nil
}

func (s *File) persistLocked() error {
	s.mem.mu.RLock()
	snap := snapshot{GPUs: s.mem.gpus, Telemetry: s.mem.telemetry}
	s.mem.mu.RUnlock()
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *File) Save(ctx context.Context, t domain.Telemetry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Save(ctx, t); err != nil {
		return err
	}
	return s.persistLocked()
}

func (s *File) Write(ctx context.Context, t domain.Telemetry) error {
	return s.Save(ctx, t)
}

func (s *File) ListGPUs(ctx context.Context) ([]domain.GPU, error) {
	return s.mem.ListGPUs(ctx)
}

func (s *File) QueryByGPU(ctx context.Context, gpuID string, window domain.TimeWindow) ([]domain.Telemetry, error) {
	return s.mem.QueryByGPU(ctx, gpuID, window)
}

func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}

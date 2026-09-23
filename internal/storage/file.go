package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

// snapshot is a snapshot of the repository.
type snapshot struct {
	GPUs      map[string]domain.GPU `json:"gpus"`
	Telemetry []domain.Telemetry    `json:"telemetry"`
}

// File is a durable JSON-backed repository. Suitable for a single gateway replica + PVC.
type File struct {
	path         string
	mem          *Memory
	mu           sync.Mutex
	writes       chan struct{}
	maxOpenConns int
	maxIdleConns int
	log          *slog.Logger
	dirty        bool
	lastFlush    time.Time
}

// Config is gateway store configuration from the environment.
type Config struct {
	Path         string
	MaxRecords   int
	MaxOpenConns int
	MaxIdleConns int
}

// Open opens a file repository at the given path.
func Open(path string) (*File, error) {
	return OpenConfig(Config{Path: path})
}

// OpenConfig opens a file repository with capacity and write-concurrency limits.
func OpenConfig(cfg Config) (*File, error) {
	path := cfg.Path
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create store dir %s: %w", dir, err)
		}
	}
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 4
	}
	writes := make(chan struct{}, maxOpen)
	s := &File{
		path:         path,
		mem:          NewMemorySize(cfg.MaxRecords),
		writes:       writes,
		maxOpenConns: maxOpen,
		maxIdleConns: cfg.MaxIdleConns,
		log:          slog.Default(),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	if err := s.persistLocked(); err != nil {
		return nil, fmt.Errorf("store %s is not writable: %w", path, err)
	}
	return s, nil
}

// load loads the repository from the file system.
func (s *File) load() error {
	info, err := os.Stat(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat store: %w", err)
	}
	if info.Size() > constants.MaxStoreBytes {
		s.log.Warn("store too large, resetting", "path", s.path, "bytes", info.Size())
		return os.Remove(s.path)
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read store: %w", err)
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
	if s.mem.maxRecords > 0 && len(s.mem.telemetry) > s.mem.maxRecords {
		s.mem.telemetry = append([]domain.Telemetry(nil), s.mem.telemetry[len(s.mem.telemetry)-s.mem.maxRecords:]...)
	}
	return nil
}

func (s *File) persistLocked() error {
	s.mem.mu.RLock()
	snap := snapshot{GPUs: s.mem.gpus, Telemetry: s.mem.telemetry}
	s.mem.mu.RUnlock()
	b, _ := json.Marshal(snap)
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("persist telemetry: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("persist telemetry: %w", err)
	}
	s.dirty = false
	s.lastFlush = time.Now()
	return nil
}

func (s *File) acquireWrite(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("persist telemetry: %w", err)
	}
	select {
	case s.writes <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("persist telemetry: %w", ctx.Err())
	}
}

func (s *File) releaseWrite() {
	select {
	case <-s.writes:
	default:
	}
}

// Save saves the telemetry to the repository.
func (s *File) Save(ctx context.Context, t domain.Telemetry) error {
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Save(ctx, t); err != nil {
		return err
	}
	s.dirty = true
	if time.Since(s.lastFlush) < constants.PersistInterval {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		return err
	}
	return nil
}

// Write writes the telemetry to the repository.
func (s *File) Write(ctx context.Context, t domain.Telemetry) error {
	return s.Save(ctx, t)
}

// ListGPUs lists the GPUs in the repository.
func (s *File) ListGPUs(ctx context.Context) ([]domain.GPU, error) {
	return s.mem.ListGPUs(ctx)
}

// QueryByGPU queries the telemetry for a given GPU and time window.
func (s *File) QueryByGPU(ctx context.Context, gpuID string, window domain.TimeWindow) ([]domain.Telemetry, error) {
	return s.mem.QueryByGPU(ctx, gpuID, window)
}

// Close closes the repository.
func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	return s.persistLocked()
}

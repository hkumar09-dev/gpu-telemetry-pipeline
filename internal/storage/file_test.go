package storage

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

func TestFileWriteAndEmptyQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	ts := time.Now().UTC()
	if err := db.Write(ctx, domain.Telemetry{
		UUID: "g1", MetricName: "m", ProcessedAt: ts, Hostname: "h", GPUIndex: "0", Value: 1,
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryByGPU(ctx, "missing", domain.TimeWindow{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("got %+v", rows)
	}
}

func TestFileResetsOversizedStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.json")
	if err := os.WriteFile(path, make([]byte, 33<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	gpus, err := db.ListGPUs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(gpus) != 0 {
		t.Fatalf("expected empty after reset, got %+v", gpus)
	}
}

func TestFileCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestFilePersistDebounceAndCloseClean(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ts := time.Now().UTC()
	if err := db.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "m", ProcessedAt: ts, Hostname: "h", GPUIndex: "0", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "m", ProcessedAt: ts.Add(time.Second), Hostname: "h", GPUIndex: "0", Value: 2}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFilePersistAfterIntervalAndUnwritable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ts := time.Now().UTC()
	if err := db.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "m", ProcessedAt: ts, Hostname: "h", GPUIndex: "0", Value: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(constants.PersistInterval + 20*time.Millisecond)
	if err := db.Save(ctx, domain.Telemetry{UUID: "g1", MetricName: "m", ProcessedAt: ts.Add(time.Second), Hostname: "h", GPUIndex: "0", Value: 2}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.path = filepath.Join(dir, "nope", "x.json")
	db.dirty = true
	db.lastFlush = time.Time{}
	if err := db.Save(ctx, domain.Telemetry{UUID: "g2", MetricName: "m", ProcessedAt: ts, Hostname: "h", GPUIndex: "1", Value: 3}); err == nil {
		t.Fatal("expected persist error")
	}
	_ = db.Close()
}

func TestFileLoadStatError(t *testing.T) {
	base := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(base, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &File{path: filepath.Join(base, "x.json"), mem: NewMemory(), log: slog.Default()}
	if err := s.load(); err == nil {
		t.Fatal("expected stat error")
	}
}

func TestFileOpenNotWritableAndUnreadable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if _, err := Open(filepath.Join(dir, "store.json")); err == nil {
		t.Fatal("expected not writable")
	}

	dir2 := t.TempDir()
	path := filepath.Join(dir2, "store.json")
	if err := os.WriteFile(path, []byte(`{"gpus":{},"telemetry":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	if _, err := Open(path); err == nil {
		t.Fatal("expected read error")
	}
}

func TestFileRenameConflict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &File{path: path, mem: NewMemory(), log: slog.Default()}
	if err := s.persistLocked(); err == nil {
		t.Fatal("expected rename error")
	}
}

func TestFileOpenParentIsFile(t *testing.T) {
	base := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(base, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(base, "store.json")); err == nil {
		t.Fatal("expected mkdir error")
	}
}

func TestFileLoadCapsTelemetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	snap := snapshot{GPUs: map[string]domain.GPU{"g": {ID: "g"}}, Telemetry: make([]domain.Telemetry, 0, 5)}
	ts := time.Now().UTC()
	for i := 0; i < 5; i++ {
		snap.Telemetry = append(snap.Telemetry, domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: ts, Value: float64(i)})
	}
	b, _ := json.Marshal(snap)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.mem.maxRecords = 2
	if err := db.load(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryByGPU(context.Background(), "g", domain.TimeWindow{})
	if err != nil || len(rows) != 2 {
		t.Fatalf("capped load %d %v", len(rows), err)
	}
}

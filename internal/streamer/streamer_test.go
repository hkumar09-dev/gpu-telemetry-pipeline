package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/clock"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type stubSource struct{ rows []domain.Telemetry }

func (s stubSource) Load(context.Context) ([]domain.Telemetry, error) { return s.rows, nil }

type capturePub struct {
	mu   sync.Mutex
	msgs []published
}

type published struct {
	topic, key string
	payload    []byte
}

func (c *capturePub) Publish(_ context.Context, topic, key string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]byte(nil), payload...)
	c.msgs = append(c.msgs, published{topic, key, cp})
	return nil
}

func TestStreamerShardsAndStamps(t *testing.T) {
	rows := []domain.Telemetry{
		{UUID: "g0", MetricName: "util", GPUIndex: "0"},
		{UUID: "g1", MetricName: "util", GPUIndex: "1"},
		{UUID: "g2", MetricName: "util", GPUIndex: "2"},
		{UUID: "g3", MetricName: "util", GPUIndex: "3"},
	}
	ts := time.Date(2026, 9, 17, 4, 20, 0, 0, time.UTC)
	pub := &capturePub{}
	s := New(stubSource{rows}, pub, clock.FixedClock{T: ts}, Config{
		Topic:    "gpu-telemetry",
		Index:    1,
		Count:    2,
		Interval: time.Millisecond,
		Loop:     false,
	}, quietLog())
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.Ready() {
		t.Fatal("expected ready after load")
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("shard should emit 2 messages, got %d", len(pub.msgs))
	}
	for _, m := range pub.msgs {
		var got domain.Telemetry
		if err := json.Unmarshal(m.payload, &got); err != nil {
			t.Fatal(err)
		}
		if !got.ProcessedAt.Equal(ts) {
			t.Fatalf("processed_at %s", got.ProcessedAt)
		}
		if got.UUID != "g1" && got.UUID != "g3" {
			t.Fatalf("unexpected uuid %s", got.UUID)
		}
		if m.key != got.UUID {
			t.Fatal("publish key must be gpu uuid")
		}
	}
}

func TestStreamerLoadErrorInvalidLoopAndPublish(t *testing.T) {
	src := errSource{err: fmt.Errorf("no csv")}
	s := New(src, &capturePub{}, clock.SystemClock{}, Config{Topic: "t"}, quietLog())
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("expected load error")
	}

	rows := []domain.Telemetry{
		{UUID: "", MetricName: "m", GPUIndex: "0"},
		{UUID: "g1", MetricName: "util", GPUIndex: "0"},
	}
	pub := &failPub{}
	s = New(stubSource{rows}, pub, clock.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, Config{
		Topic: "t", Interval: time.Millisecond, Loop: true, MaxIterations: 1,
	}, quietLog())
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s = New(stubSource{rows: []domain.Telemetry{{UUID: "g", MetricName: "m", GPUIndex: "0"}}}, &capturePub{}, clock.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, Config{Interval: time.Hour}, quietLog())
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

type errSource struct{ err error }

func (e errSource) Load(context.Context) ([]domain.Telemetry, error) { return nil, e.err }

type failPub struct{}

func (failPub) Publish(context.Context, string, string, []byte) error { return fmt.Errorf("pub fail") }

func TestStreamerDefaultsAndCancelDuringInterval(t *testing.T) {
	s := New(stubSource{}, &capturePub{}, nil, Config{Index: -1}, nil)
	s.log = quietLog()
	if s.cfg.Count != 1 || s.cfg.Index != 0 || s.cfg.Topic != "gpu-telemetry" {
		t.Fatalf("%+v", s.cfg)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pub := &cancelPub{cancel: cancel}
	s = New(stubSource{rows: []domain.Telemetry{{UUID: "g", MetricName: "m", GPUIndex: "0"}}}, pub, clock.FixedClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, Config{Interval: time.Hour}, quietLog())
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

type cancelPub struct{ cancel context.CancelFunc }

func (c *cancelPub) Publish(context.Context, string, string, []byte) error {
	c.cancel()
	return nil
}

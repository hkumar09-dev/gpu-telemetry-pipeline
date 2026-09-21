package streamer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/clock"
	"github.com/gpu-telemetry-pipeline/internal/domain"
)

type stubSource struct{ rows []domain.Telemetry }

func (s stubSource) Load() ([]domain.Telemetry, error) { return s.rows, nil }

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
	}, nil)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
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

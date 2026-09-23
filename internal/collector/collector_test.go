package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/ports"
	"github.com/gpu-telemetry-pipeline/internal/storage"
)

func TestCollectorPersistsAndAcks(t *testing.T) {
	engine := mq.NewEngine(mq.Config{Partitions: 2})
	t.Cleanup(engine.Close)
	pub := mq.EnginePublisher{Engine: engine}
	engine.Subscribe("gpu-telemetry", "collectors", "c1")
	cons := mq.EngineConsumer{Engine: engine, Topic: "gpu-telemetry", Group: "collectors", ConsumerID: "c1"}
	repo := storage.NewMemory()
	c := New(cons, repo, Config{ConsumeTimeout: 50 * time.Millisecond}, nil)

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	body, _ := json.Marshal(domain.Telemetry{
		ProcessedAt: ts,
		MetricName:  "DCGM_FI_DEV_GPU_UTIL",
		UUID:        "GPU-1",
		Hostname:    "host",
		Value:       42,
	})
	if err := pub.Publish(context.Background(), "gpu-telemetry", "GPU-1", body); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gpus, _ := repo.ListGPUs(context.Background())
		if len(gpus) == 1 {
			rows, err := repo.QueryByGPU(context.Background(), "GPU-1", domain.TimeWindow{})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Value != 42 {
				t.Fatalf("rows %+v", rows)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("collector did not persist")
}

func TestCollectorErrClosedAndAckFail(t *testing.T) {
	engine := mq.NewEngine(mq.Config{Partitions: 1})
	engine.Subscribe("t", "g", "c1")
	cons := mq.EngineConsumer{Engine: engine, Topic: "t", Group: "g", ConsumerID: "c1"}
	c := New(cons, storage.NewMemory(), Config{ConsumeTimeout: 20 * time.Millisecond}, nil)
	engine.Close()
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("closed should return nil, got %v", err)
	}
}

type stubConsumer struct {
	consume func(ctx context.Context, timeout time.Duration) (ports.Delivery, error)
	ack     error
	nack    error
}

func (s stubConsumer) Consume(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
	return s.consume(ctx, timeout)
}
func (s stubConsumer) Ack(context.Context, string) error  { return s.ack }
func (s stubConsumer) Nack(context.Context, string) error { return s.nack }
func (s stubConsumer) Close() error                       { return nil }

type stubWriter struct{ err error }

func (s stubWriter) Write(context.Context, domain.Telemetry) error { return s.err }

func TestCollectorHandlePaths(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ok, _ := json.Marshal(domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: ts})
	calls := 0
	cons := stubConsumer{
		consume: func(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
			calls++
			switch calls {
			case 1:
				return ports.Delivery{}, fmt.Errorf("transient")
			case 2:
				return ports.Delivery{ID: "1", Payload: []byte("bad")}, nil
			case 3:
				return ports.Delivery{ID: "2", Payload: ok}, nil
			case 4:
				return ports.Delivery{ID: "3", Payload: ok}, nil
			default:
				return ports.Delivery{}, constants.ErrClosed
			}
		},
		ack:  fmt.Errorf("ack fail"),
		nack: nil,
	}
	c := New(cons, stubWriter{}, Config{ConsumeTimeout: time.Millisecond}, nil)
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorRetryableWriteCancel(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ok, _ := json.Marshal(domain.Telemetry{UUID: "g", MetricName: "m", ProcessedAt: ts})
	n := 0
	cons := stubConsumer{
		consume: func(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
			n++
			if n == 1 {
				return ports.Delivery{ID: "1", Payload: ok}, nil
			}
			return ports.Delivery{}, constants.ErrTimeout
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := New(cons, stubWriter{err: fmt.Errorf("dial tcp: connection refused")}, Config{ConsumeTimeout: time.Millisecond}, nil)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := c.Run(ctx); err == nil {
		t.Fatal("expected cancel during retry wait")
	}
}

func TestCollectorInvalidTelemetry(t *testing.T) {
	body, _ := json.Marshal(domain.Telemetry{UUID: "g"})
	n := 0
	cons := stubConsumer{
		consume: func(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
			n++
			if n == 1 {
				return ports.Delivery{ID: "1", Payload: body}, nil
			}
			return ports.Delivery{}, constants.ErrClosed
		},
	}
	c := New(cons, stubWriter{}, Config{}, nil)
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRetryableWrite(t *testing.T) {
	if retryableWrite(nil) {
		t.Fatal("nil")
	}
	if !retryableWrite(fmt.Errorf("dial tcp: connection refused")) {
		t.Fatal("refused")
	}
	if retryableWrite(fmt.Errorf("invalid json")) {
		t.Fatal("json")
	}
	if !retryableWrite(fmt.Errorf("connection reset")) || !retryableWrite(fmt.Errorf("no such host")) {
		t.Fatal("retry strings")
	}
}

func TestCollectorNacksInvalidJSON(t *testing.T) {
	engine := mq.NewEngine(mq.Config{Partitions: 1, AckTimeout: 50 * time.Millisecond})
	t.Cleanup(engine.Close)
	engine.Subscribe("t", "g", "c1")
	cons := mq.EngineConsumer{Engine: engine, Topic: "t", Group: "g", ConsumerID: "c1"}
	repo := storage.NewMemory()
	c := New(cons, repo, Config{ConsumeTimeout: 30 * time.Millisecond}, nil)

	ctx := context.Background()
	_ = engine.Publish(ctx, "t", "k", []byte("not-json"))

	runCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(runCtx) }()
	time.Sleep(120 * time.Millisecond)

	gpus, _ := repo.ListGPUs(ctx)
	if len(gpus) != 0 {
		t.Fatal("invalid payload must not persist")
	}
}

package collector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/storage"
)

func TestCollectorPersistsAndAcks(t *testing.T) {
	engine := mq.NewEngine(mq.Config{Partitions: 2})
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

func TestCollectorNacksInvalidJSON(t *testing.T) {
	engine := mq.NewEngine(mq.Config{Partitions: 1, AckTimeout: 50 * time.Millisecond})
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

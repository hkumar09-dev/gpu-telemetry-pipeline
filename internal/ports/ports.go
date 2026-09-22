package ports

import (
	"context"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

// Publisher sends keyed messages to a topic. The key is used for partition affinity.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// Delivery is one in-flight message owned by a consumer until Ack or Nack.
type Delivery struct {
	ID        string
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Payload   []byte
}

// Consumer is a competing member of a consumer group.
type Consumer interface {
	Consume(ctx context.Context, timeout time.Duration) (Delivery, error)
	Ack(ctx context.Context, id string) error
	Nack(ctx context.Context, id string) error
	Close() error
}

// ConsumerFactory joins a consumer group.
type ConsumerFactory interface {
	Subscribe(ctx context.Context, topic, group, consumerID string) (Consumer, error)
}

// TelemetrySource yields CSV records that streamers shard and replay.
type TelemetrySource interface {
	Load(ctx context.Context) ([]domain.Telemetry, error)
}

// Repository persists parsed telemetry for the query API.
type Repository interface {
	Save(ctx context.Context, t domain.Telemetry) error
	ListGPUs(ctx context.Context) ([]domain.GPU, error)
	QueryByGPU(ctx context.Context, gpuID string, window domain.TimeWindow) ([]domain.Telemetry, error)
}

// Writer is the collector-side persistence port (may be remote HTTP).
type Writer interface {
	Write(ctx context.Context, t domain.Telemetry) error
}

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/ports"
)

type Config struct {
	ConsumeTimeout time.Duration
}

// Collector consumes from a consumer group, parses payloads, and persists them.
type Collector struct {
	consumer ports.Consumer
	writer   ports.Writer
	cfg      Config
	log      *slog.Logger
}

func New(consumer ports.Consumer, writer ports.Writer, cfg Config, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ConsumeTimeout <= 0 {
		cfg.ConsumeTimeout = time.Second
	}
	return &Collector{consumer: consumer, writer: writer, cfg: cfg, log: log}
}

func (c *Collector) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d, err := c.consumer.Consume(ctx, c.cfg.ConsumeTimeout)
		if err != nil {
			if errors.Is(err, constants.ErrTimeout) || errors.Is(err, context.Canceled) {
				continue
			}
			if errors.Is(err, constants.ErrClosed) {
				return nil
			}
			c.log.Warn("consume failed", "err", err)
			continue
		}
		if err := c.handle(ctx, d); err != nil {
			c.log.Error("handle delivery", "id", d.ID, "err", err)
			_ = c.consumer.Nack(ctx, d.ID)
			continue
		}
		if err := c.consumer.Ack(ctx, d.ID); err != nil {
			c.log.Warn("ack failed", "id", d.ID, "err", err)
		}
	}
}

func (c *Collector) handle(ctx context.Context, d ports.Delivery) error {
	var t domain.Telemetry
	if err := json.Unmarshal(d.Payload, &t); err != nil {
		return err
	}
	if err := t.Validate(); err != nil {
		return err
	}
	return c.writer.Write(ctx, t)
}

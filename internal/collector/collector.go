package collector

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/metrics"
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
			c.log.Info("collector shutting down")
			return nil
		}
		d, err := c.consumer.Consume(ctx, c.cfg.ConsumeTimeout)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.log.Info("collector shutting down")
				return nil
			}
			if errors.Is(err, constants.ErrTimeout) {
				continue
			}
			if errors.Is(err, constants.ErrClosed) {
				return nil
			}
			c.log.Warn("consume failed", "err", err)
			metrics.MQConsumerFailures.Inc()
			continue
		}
		metrics.CollectorConsumed.Inc()
		if err := c.handle(ctx, d); err != nil {
			c.log.Warn("handle delivery", "id", d.ID, "err", err)
			c.nack(d.ID)
			if retryableWrite(err) && ctx.Err() == nil {
				select {
				case <-ctx.Done():
					c.log.Info("collector shutting down")
					return nil
				case <-time.After(time.Second):
				}
			}
			continue
		}
		c.ack(d.ID)
	}
}

func (c *Collector) handle(ctx context.Context, d ports.Delivery) error {
	var t domain.Telemetry
	if err := json.Unmarshal(d.Payload, &t); err != nil {
		c.log.Error("Error in marshalling of payload")
		return err
	}
	if err := t.Validate(); err != nil {
		c.log.Error("Error in validating the response")
		return err
	}
	if err := c.writer.Write(ctx, t); err != nil {
		metrics.CollectorPersistFailures.Inc()
		return err
	}
	metrics.CollectorPersisted.Inc()
	return nil
}

func (c *Collector) ack(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.consumer.Ack(ctx, id); err != nil {
		c.log.Warn("ack failed", "id", id, "err", err)
	}
}

func (c *Collector) nack(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.consumer.Nack(ctx, id); err != nil {
		c.log.Warn("nack failed", "id", id, "err", err)
	}
}

func retryableWrite(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "no such host")
}

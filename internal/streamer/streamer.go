package streamer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/clock"
	"github.com/gpu-telemetry-pipeline/internal/domain"
	"github.com/gpu-telemetry-pipeline/internal/metrics"
	"github.com/gpu-telemetry-pipeline/internal/ports"
)

// Config controls CSV sharding and emit cadence.
type Config struct {
	Topic         string
	Index         int
	Count         int
	Interval      time.Duration
	Loop          bool
	MaxIterations int
}

// Streamer reads telemetry and publishes a shard of the dataset.
// Scaling: set Index/Count from a StatefulSet ordinal so replicas are disjoint.
type Streamer struct {
	src   ports.TelemetrySource
	pub   ports.Publisher
	clock clock.Clock
	cfg   Config
	log   *slog.Logger
	ready atomic.Bool
}

// New creates a new streamer.
func New(src ports.TelemetrySource, pub ports.Publisher, clk clock.Clock, cfg Config, log *slog.Logger) *Streamer {
	if clk == nil {
		clk = clock.SystemClock{}
	}

	if log == nil {
		log = slog.Default()
	}

	if cfg.Count <= 0 {
		cfg.Count = 1
	}

	if cfg.Index < 0 || cfg.Index >= cfg.Count {
		cfg.Index = 0
	}

	if cfg.Topic == "" {
		cfg.Topic = "gpu-telemetry"
	}

	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Millisecond
	}

	return &Streamer{src: src, pub: pub, clock: clk, cfg: cfg, log: log}
}

// Run starts the streamer.
func (s *Streamer) Run(ctx context.Context) error {
	rows, err := s.src.Load(ctx)
	if err != nil {
		s.log.Error("load telemetry failed", "err", err)
		return fmt.Errorf("load telemetry: %w", err)
	}

	s.log.Info("streamer loaded csv", "rows", len(rows), "index", s.cfg.Index, "count", s.cfg.Count)
	s.ready.Store(true)

	iter := 0
	for {
		if err := ctx.Err(); err != nil {
			return s.shutdown(err)
		}
		if err := s.emitPass(ctx, rows); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return s.shutdown(err)
			}
			s.log.Error("emit pass failed", "err", err)
			return err
		}
		iter++
		if !s.cfg.Loop || (s.cfg.MaxIterations > 0 && iter >= s.cfg.MaxIterations) {
			return nil
		}
	}
}

func (s *Streamer) shutdown(err error) error {
	s.ready.Store(false)
	s.log.Info("streamer shutting down")
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

// emitPass emits a pass of the dataset.
func (s *Streamer) emitPass(ctx context.Context, rows []domain.Telemetry) error {
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i%s.cfg.Count != s.cfg.Index {
			continue
		}
		row.ProcessedAt = s.clock.Now()
		if err := row.Validate(); err != nil {
			s.log.Warn("skip invalid row", "index", i, "err", err)
			continue
		}
		body, err := json.Marshal(row)
		if err != nil {
			s.log.Warn("skip invalid telemetry", "index", i, "err", err)
			continue
		}
		if err := s.pub.Publish(ctx, s.cfg.Topic, row.UUID, body); err != nil {
			s.log.Error("publish failed", "index", i, "err", err)
			metrics.StreamerPublishFailures.Inc()
			return fmt.Errorf("publish: %w", err)
		}
		metrics.StreamerProcessed.Inc()

		// Wait for interval
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.cfg.Interval):
		}
	}
	return nil
}

// Ready reports whether the CSV has been loaded.
func (s *Streamer) Ready() bool {
	return s.ready.Load()
}

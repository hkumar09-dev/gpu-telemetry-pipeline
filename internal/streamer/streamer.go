package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/clock"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/domain"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/ports"
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
	src    ports.TelemetrySource
	pub    ports.Publisher
	clock  clock.Clock
	cfg    Config
	log    *slog.Logger
}

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

func (s *Streamer) Run(ctx context.Context) error {
	rows, err := s.src.Load()
	if err != nil {
		return fmt.Errorf("load telemetry: %w", err)
	}
	s.log.Info("streamer loaded csv", "rows", len(rows), "index", s.cfg.Index, "count", s.cfg.Count)

	iter := 0
	for {
		if err := s.emitPass(ctx, rows); err != nil {
			return err
		}
		iter++
		if !s.cfg.Loop || (s.cfg.MaxIterations > 0 && iter >= s.cfg.MaxIterations) {
			return nil
		}
	}
}

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
			return err
		}
		if err := s.pub.Publish(ctx, s.cfg.Topic, row.UUID, body); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.cfg.Interval):
		}
	}
	return nil
}

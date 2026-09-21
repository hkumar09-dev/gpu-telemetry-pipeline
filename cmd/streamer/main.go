package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/clock"
	"github.com/gpu-telemetry-pipeline/internal/csvsource"
	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/ports"
	"github.com/gpu-telemetry-pipeline/internal/streamer"
	"github.com/gpu-telemetry-pipeline/utils"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	s := streamer.New(
		csvsource.FileSource{Path: utils.Getenv("CSV_PATH", "/data/dcgm_metrics.csv")},
		retryPublisher{inner: mq.NewClient(utils.Getenv("MQ_ADDR", "127.0.0.1:9000")), log: log},
		clock.SystemClock{},
		streamer.Config{
			Topic:    utils.Getenv("MQ_TOPIC", "gpu-telemetry"),
			Index:    shardIndex(),
			Count:    atoi(utils.Getenv("STREAMER_COUNT", "1")),
			Interval: duration(utils.Getenv("STREAM_INTERVAL", "10ms")),
			Loop:     utils.Getenv("STREAM_LOOP", "true") == "true",
		},
		log,
	)
	if err := s.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("streamer failed", "err", err)
		os.Exit(1)
	}
}

func shardIndex() int {
	if v := os.Getenv("STREAMER_INDEX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	name := os.Getenv("POD_NAME")
	if name == "" {
		name, _ = os.Hostname()
	}
	i := strings.LastIndex(name, "-")
	if i >= 0 {
		if n, err := strconv.Atoi(name[i+1:]); err == nil {
			return n
		}
	}
	return 0
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func duration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 10 * time.Millisecond
	}
	return d
}

type retryPublisher struct {
	inner ports.Publisher
	log   *slog.Logger
}

func (p retryPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	backoff := 200 * time.Millisecond
	for {
		err := p.inner.Publish(ctx, topic, key, payload)
		if err == nil {
			return nil
		}
		p.log.Error("publish failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
}

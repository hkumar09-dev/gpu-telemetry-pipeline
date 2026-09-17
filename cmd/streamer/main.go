package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/clock"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/csvsource"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/mq"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/streamer"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	s := streamer.New(
		csvsource.FileSource{Path: getenv("CSV_PATH", "/data/dcgm_metrics.csv")},
		mq.NewClient(getenv("MQ_ADDR", "127.0.0.1:9000")),
		clock.SystemClock{},
		streamer.Config{
			Topic:    getenv("MQ_TOPIC", "gpu-telemetry"),
			Index:    atoi(getenv("STREAMER_INDEX", "0")),
			Count:    atoi(getenv("STREAMER_COUNT", "1")),
			Interval: duration(getenv("STREAM_INTERVAL", "10ms")),
			Loop:     getenv("STREAM_LOOP", "true") == "true",
		},
		log,
	)
	if err := s.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("streamer failed", "err", err)
		os.Exit(1)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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

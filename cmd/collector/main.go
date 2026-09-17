package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/collector"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/mq"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := mq.NewClient(getenv("MQ_ADDR", "127.0.0.1:9000"))
	id := getenv("CONSUMER_ID", hostname())
	cons, err := client.Subscribe(ctx, getenv("MQ_TOPIC", "gpu-telemetry"), getenv("MQ_GROUP", "collectors"), id)
	if err != nil {
		log.Error("subscribe failed", "err", err)
		os.Exit(1)
	}
	defer cons.Close()

	c := collector.New(cons, storage.NewHTTPWriter(getenv("GATEWAY_URL", "http://127.0.0.1:8080")), collector.Config{
		ConsumeTimeout: 2 * time.Second,
	}, log)
	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("collector failed", "err", err)
		os.Exit(1)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "collector"
	}
	return h
}

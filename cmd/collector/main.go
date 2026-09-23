package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/collector"
	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/ports"
	"github.com/gpu-telemetry-pipeline/internal/storage"
)

var (
	osExit             = os.Exit
	subscribeRetryWait = 2 * time.Second
	hostnameFn         = os.Hostname
)

func main() {
	if err := run(context.Background()); err != nil {
		osExit(1)
	}
}

func run(parent context.Context) error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := mq.NewClient(getenv("MQ_ADDR", "127.0.0.1:9000"))
	id := getenv("CONSUMER_ID", hostname())
	cons, err := subscribeWithRetry(ctx, client, log, id)
	if err != nil {
		log.Error("subscribe failed", "err", err)
		return err
	}
	defer cons.Close()

	c := collector.New(cons, storage.NewHTTPWriter(getenv("GATEWAY_URL", "http://127.0.0.1:8080")), collector.Config{
		ConsumeTimeout: 2 * time.Second,
	}, log)

	_ = c.Run(ctx)
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, err := hostnameFn()
	if err != nil {
		return "collector"
	}
	return h
}

func subscribeWithRetry(ctx context.Context, client *mq.Client, log *slog.Logger, id string) (ports.Consumer, error) {
	topic := getenv("MQ_TOPIC", "gpu-telemetry")
	group := getenv("MQ_GROUP", "collectors")
	for {
		cons, err := client.Subscribe(ctx, topic, group, id)
		if err == nil {
			return cons, nil
		}
		log.Error("subscribe failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(subscribeRetryWait):
		}
	}
}

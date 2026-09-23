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
	"github.com/gpu-telemetry-pipeline/internal/observe"
	"github.com/gpu-telemetry-pipeline/internal/ports"
	"github.com/gpu-telemetry-pipeline/internal/storage"
	"github.com/gpu-telemetry-pipeline/utils"
)

var (
	osExit             = os.Exit
	background         = context.Background
	subscribeRetryWait = 2 * time.Second
	hostnameFn         = os.Hostname
	newLogger          = utils.NewJSONLogger
)

func main() {
	if err := run(background()); err != nil {
		osExit(1)
	}
}

func run(parent context.Context) error {
	log := newLogger()
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if v := os.Getenv("MQ_SUBSCRIBE_RETRY"); v != "" {
		subscribeRetryWait = utils.EnvDuration("MQ_SUBSCRIBE_RETRY", subscribeRetryWait)
	}

	client := mq.NewClient(getenv("MQ_ADDR", utils.DefaultMQClientAddr))
	id := getenv("CONSUMER_ID", hostname())
	cons, err := subscribeWithRetry(ctx, client, log, id)
	if err != nil {
		log.Error("subscribe failed", "err", err)
		return err
	}

	ready := &observe.Status{}
	metricsStop, err := observe.Listen(ctx, utils.Getenv("METRICS_ADDR", utils.DefaultCollectorMetrics), ready.Ready, log)
	if err != nil {
		_ = cons.Close()
		log.Error("metrics listen", "err", err)
		return err
	}
	ready.SetReady(true)

	c := collector.New(cons, storage.NewHTTPWriter(getenv("GATEWAY_URL", utils.DefaultGatewayURL)), collector.Config{
		ConsumeTimeout: consumeTimeout(),
	}, log)

	_ = c.Run(ctx)
	ready.SetReady(false)

	shctx, stop := context.WithTimeout(context.Background(), utils.ShutdownTimeout())
	defer stop()
	if err := metricsStop(shctx); err != nil {
		log.Error("metrics shutdown", "err", err)
	}
	if err := cons.Close(); err != nil {
		log.Error("close consumer", "err", err)
	}
	log.Info("collector shutdown complete")
	return nil
}

func consumeTimeout() time.Duration {
	return utils.EnvDuration("CONSUME_TIMEOUT", 2*time.Second)
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
	topic := getenv("MQ_TOPIC", utils.DefaultMQTopic)
	group := getenv("MQ_GROUP", utils.DefaultMQGroup)
	for {
		cons, err := client.Subscribe(ctx, topic, group, id)
		if err == nil {
			return cons, nil
		}
		log.Warn("subscribe failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(subscribeRetryWait):
		}
	}
}

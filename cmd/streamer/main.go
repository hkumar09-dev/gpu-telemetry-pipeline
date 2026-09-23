package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/clock"
	"github.com/gpu-telemetry-pipeline/internal/csvsource"
	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/observe"
	"github.com/gpu-telemetry-pipeline/internal/ports"
	"github.com/gpu-telemetry-pipeline/internal/streamer"
	"github.com/gpu-telemetry-pipeline/utils"
)

var (
	maxPublishAttempts    = constants.MAX_RETRY_ATTEMPTS
	osExit                = os.Exit
	initialPublishBackoff = constants.INITIAL_BACKOFF
	newLogger             = utils.NewJSONLogger
)

func main() {
	if err := run(context.Background()); err != nil {
		osExit(1)
	}
}

func run(parent context.Context) error {
	log := newLogger()
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if n := utils.EnvInt("PUBLISH_MAX_RETRIES", 0); n > 0 {
		maxPublishAttempts = n
	}
	if os.Getenv("PUBLISH_INITIAL_BACKOFF") != "" {
		initialPublishBackoff = utils.EnvDuration("PUBLISH_INITIAL_BACKOFF", initialPublishBackoff)
	}

	s := streamer.New(
		csvsource.FileSource{Path: utils.CSVFile()},
		retryPublisher{inner: mq.NewClient(utils.Getenv("MQ_ADDR", utils.DefaultMQClientAddr)), log: log},
		clock.SystemClock{},
		streamer.Config{
			Topic:    utils.Getenv("MQ_TOPIC", utils.DefaultMQTopic),
			Index:    shardIndex(),
			Count:    atoi(utils.Getenv("STREAMER_COUNT", "1")),
			Interval: utils.EnvDuration("STREAM_INTERVAL", utils.DefaultStreamInterval),
			Loop:     utils.EnvBool("STREAM_LOOP", true),
		},
		log,
	)

	metricsStop, err := observe.Listen(ctx, utils.Getenv("METRICS_ADDR", utils.DefaultStreamerMetrics), s.Ready, log)
	if err != nil {
		log.Error("metrics listen", "err", err)
		return err
	}

	runErr := s.Run(ctx)
	shctx, stop := context.WithTimeout(context.Background(), utils.ShutdownTimeout())
	defer stop()
	if err := metricsStop(shctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Error("metrics shutdown", "err", err)
	}
	if runErr != nil {
		log.Error("streamer failed", "err", runErr)
		return runErr
	}
	log.Info("streamer shutdown complete")
	return nil
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
	backoff := initialPublishBackoff

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := p.inner.Publish(ctx, topic, key, payload)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt >= maxPublishAttempts {
			p.log.Error("publish failed, max retries reached", "attempt", attempt, "err", err)
			return err
		}

		p.log.Error("publish failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > constants.MAX_BACKOFF {
			backoff = constants.MAX_BACKOFF
		}
	}
}

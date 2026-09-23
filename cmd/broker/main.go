package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/internal/observe"
	"github.com/gpu-telemetry-pipeline/utils"
)

var (
	osExit     = os.Exit
	background = context.Background
	newLogger  = utils.NewJSONLogger
)

func main() {
	if err := run(background()); err != nil {
		osExit(1)
	}
}

func run(parent context.Context) error {
	log := newLogger()
	addr := utils.Getenv("MQ_ADDR", utils.DefaultMQListenAddr)
	engine := mq.NewEngine(mq.Config{
		Partitions:      utils.EnvInt("MQ_PARTITIONS", utils.DefaultMQPartitions),
		MaxPerPartition: utils.EnvInt("MQ_MAX_QUEUE_SIZE", utils.DefaultMQMaxQueueSize),
		AckTimeout:      utils.EnvDuration("MQ_ACK_TIMEOUT", utils.DefaultMQAckTimeout),
		MaxRetries:      utils.EnvInt("MQ_RETRY_LIMIT", utils.DefaultMQRetryLimit),
		RetryBackoff:    utils.EnvDuration("MQ_RETRY_BACKOFF", utils.DefaultMQRetryBackoff),
		RetryQueueSize:  utils.EnvInt("MQ_RETRY_QUEUE_SIZE", utils.DefaultMQRetryQueueSize),
	})

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := mq.NewServer(ctx, engine, log)
	metricsStop, err := observe.Listen(ctx, utils.Getenv("METRICS_ADDR", utils.DefaultBrokerMetrics), func() bool {
		return srv.Addr() != "" && ctx.Err() == nil
	}, log)
	if err != nil {
		log.Error("metrics listen", "err", err)
		return err
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("starting broker",
			"addr", addr,
			"partitions", utils.EnvInt("MQ_PARTITIONS", utils.DefaultMQPartitions),
			"max_queue_size", utils.EnvInt("MQ_MAX_QUEUE_SIZE", utils.DefaultMQMaxQueueSize),
			"ack_timeout", utils.EnvDuration("MQ_ACK_TIMEOUT", utils.DefaultMQAckTimeout).String(),
			"retry_limit", utils.EnvInt("MQ_RETRY_LIMIT", utils.DefaultMQRetryLimit),
		)
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error("broker stopped", "err", err)
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			log.Error("broker stopped unexpectedly", "err", err)
			shctx, cancel := context.WithTimeout(context.Background(), utils.ShutdownTimeout())
			defer cancel()
			_ = metricsStop(shctx)
			if sd := srv.Shutdown(shctx); sd != nil && !errors.Is(sd, context.DeadlineExceeded) {
				log.Error("broker shutdown", "err", sd)
			}
			return err
		}
	}

	shctx, cancel := context.WithTimeout(context.Background(), utils.ShutdownTimeout())
	defer cancel()
	if err := metricsStop(shctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Error("metrics shutdown", "err", err)
	}
	if err := srv.Shutdown(shctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Error("broker shutdown", "err", err)
	}
	log.Info("broker shutdown complete")
	return nil
}

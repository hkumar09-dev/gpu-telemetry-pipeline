package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/api"
	"github.com/gpu-telemetry-pipeline/internal/observe"
	"github.com/gpu-telemetry-pipeline/internal/storage"
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
	cfg := storage.Config{
		Path:         utils.StorePath(),
		MaxRecords:   utils.EnvInt("DB_MAX_RECORDS", utils.DefaultDBMaxRecords),
		MaxOpenConns: utils.EnvInt("DB_MAX_OPEN_CONNS", utils.DefaultDBMaxOpenConns),
		MaxIdleConns: utils.EnvInt("DB_MAX_IDLE_CONNS", utils.DefaultDBMaxIdleConns),
	}
	db, err := storage.OpenConfig(cfg)
	if err != nil {
		log.Error("open db", "err", err)
		return err
	}

	svc := api.NewService(db, log)
	addr := utils.Getenv("HTTP_ADDR", utils.DefaultHTTPAddr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewHandler(svc),
		ReadHeaderTimeout: utils.EnvDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
	}

	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	httpErr := make(chan error, 1)
	go func() {
		log.Info("gateway listening",
			"addr", srv.Addr,
			"database_url", cfg.Path,
			"db_max_open_conns", cfg.MaxOpenConns,
			"db_max_idle_conns", cfg.MaxIdleConns,
			"db_max_records", cfg.MaxRecords,
		)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
			return
		}
		httpErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-httpErr:
		if err != nil {
			log.Error("gateway failed", "err", err)
			_ = db.Close()
			return err
		}
	}

	svc.Stop()
	shctx, stop := context.WithTimeout(context.Background(), utils.ShutdownTimeout())
	defer stop()
	if err := observe.HTTPShutdown(shctx, srv); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := db.Close(); err != nil {
		log.Error("close db", "err", err)
		return err
	}
	select {
	case err := <-httpErr:
		return err
	case <-shctx.Done():
		return nil
	}
}

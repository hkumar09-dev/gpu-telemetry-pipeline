package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/mq"
)

func TestMain(m *testing.M) {
	os.Setenv("METRICS_ADDR", "-")
	os.Exit(m.Run())
}

func useQuietLog(t *testing.T) {
	t.Helper()
	old := newLogger
	newLogger = func() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
	t.Cleanup(func() { newLogger = old })
}

func TestAtoiAndDuration(t *testing.T) {
	if atoi("3") != 3 || atoi("x") != 0 {
		t.Fatal("atoi")
	}
	if duration("5ms") != 5*time.Millisecond {
		t.Fatal("duration")
	}
	if duration("nope") != 10*time.Millisecond {
		t.Fatal("duration default")
	}
}

func TestShardIndex(t *testing.T) {
	t.Setenv("STREAMER_INDEX", "4")
	if shardIndex() != 4 {
		t.Fatal("explicit")
	}
	t.Setenv("STREAMER_INDEX", "nope")
	t.Setenv("POD_NAME", "gpu-streamer-2")
	if shardIndex() != 2 {
		t.Fatal("ordinal")
	}
	t.Setenv("POD_NAME", "gpu-streamer")
	if shardIndex() != 0 {
		t.Fatal("no ordinal")
	}
	t.Setenv("POD_NAME", "")
	if shardIndex() != 0 && shardIndex() < 0 {
		t.Fatal("hostname fallback")
	}
}

type failOnce struct {
	n int
}

func (f *failOnce) Publish(ctx context.Context, topic, key string, payload []byte) error {
	f.n++
	if f.n == 1 {
		return errors.New("connection refused")
	}
	return nil
}

type alwaysFail struct{}

func (alwaysFail) Publish(ctx context.Context, topic, key string, payload []byte) error {
	return errors.New("connection refused")
}

func TestRetryPublisher(t *testing.T) {
	old := maxPublishAttempts
	maxPublishAttempts = 3
	t.Cleanup(func() { maxPublishAttempts = old })
	p := retryPublisher{inner: &failOnce{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := p.Publish(context.Background(), "t", "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestRetryPublisherCanceled(t *testing.T) {
	p := retryPublisher{inner: &failOnce{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected cancel")
	}
}

func TestRetryPublisherBackoffCap(t *testing.T) {
	oldA, oldB := maxPublishAttempts, initialPublishBackoff
	maxPublishAttempts = 2
	initialPublishBackoff = constants.MAX_BACKOFF
	t.Cleanup(func() {
		maxPublishAttempts = oldA
		initialPublishBackoff = oldB
	})
	p := retryPublisher{inner: alwaysFail{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := p.Publish(context.Background(), "t", "k", []byte("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryPublisherMaxAttempts(t *testing.T) {
	old := maxPublishAttempts
	maxPublishAttempts = 1
	t.Cleanup(func() { maxPublishAttempts = old })
	p := retryPublisher{inner: alwaysFail{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := p.Publish(context.Background(), "t", "k", []byte("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryPublisherCancelDuringBackoff(t *testing.T) {
	old := maxPublishAttempts
	maxPublishAttempts = 5
	t.Cleanup(func() { maxPublishAttempts = old })
	p := retryPublisher{inner: alwaysFail{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected cancel during backoff")
	}
}

func TestRunStreamer(t *testing.T) {
	useQuietLog(t)
	engine := mq.NewEngine(mq.Config{Partitions: 1})
	t.Cleanup(engine.Close)
	srv := mq.NewServer(context.Background(), engine, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("broker")
	}
	t.Cleanup(func() { _ = srv.Close() })

	t.Setenv("MQ_ADDR", srv.Addr())
	t.Setenv("CSV_PATH", filepath.Join("..", "..", "testdata", "sample.csv"))
	t.Setenv("STREAM_LOOP", "false")
	t.Setenv("STREAM_INTERVAL", "1ms")
	t.Setenv("STREAMER_COUNT", "1")
	t.Setenv("STREAMER_INDEX", "0")
	if err := run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunStreamerLoadError(t *testing.T) {
	useQuietLog(t)
	t.Setenv("CSV_PATH", filepath.Join(t.TempDir(), "missing.csv"))
	t.Setenv("STREAM_LOOP", "false")
	t.Setenv("MQ_ADDR", "127.0.0.1:1")
	if err := run(context.Background()); err == nil {
		t.Fatal("expected load error")
	}
}

func TestRetryPublisherInnerSeesCancel(t *testing.T) {
	old := maxPublishAttempts
	maxPublishAttempts = 5
	t.Cleanup(func() { maxPublishAttempts = old })
	p := retryPublisher{inner: delayFail{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected cancel")
	}
}

type delayFail struct{}

func (delayFail) Publish(ctx context.Context, topic, key string, payload []byte) error {
	<-ctx.Done()
	return errors.New("after cancel")
}

func TestMainExitOnError(t *testing.T) {
	useQuietLog(t)
	code := -1
	osExit = func(c int) { code = c }
	t.Cleanup(func() { osExit = os.Exit })
	t.Setenv("CSV_PATH", filepath.Join(t.TempDir(), "missing.csv"))
	t.Setenv("STREAM_LOOP", "false")
	main()
	if code != 1 {
		t.Fatalf("exit %d", code)
	}
}

func TestMainSuccess(t *testing.T) {
	useQuietLog(t)
	engine := mq.NewEngine(mq.Config{Partitions: 1})
	t.Cleanup(engine.Close)
	srv := mq.NewServer(context.Background(), engine, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("broker")
	}
	t.Cleanup(func() { _ = srv.Close() })
	osExit = func(int) { t.Error("os.Exit") }
	t.Cleanup(func() { osExit = os.Exit })
	t.Setenv("MQ_ADDR", srv.Addr())
	t.Setenv("CSV_PATH", filepath.Join("..", "..", "testdata", "sample.csv"))
	t.Setenv("STREAM_LOOP", "false")
	t.Setenv("STREAM_INTERVAL", "1ms")
	t.Setenv("STREAMER_INDEX", "0")
	main()
}

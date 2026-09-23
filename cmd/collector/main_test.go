package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/mq"
)

func startTestBroker(t *testing.T) string {
	t.Helper()
	engine := mq.NewEngine(mq.Config{Partitions: 1})
	t.Cleanup(engine.Close)
	srv := mq.NewServer(context.Background(), engine, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("broker did not start")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv.Addr()
}

func TestGetenvAndHostname(t *testing.T) {
	t.Setenv("COLLECTOR_TEST", "v")
	if getenv("COLLECTOR_TEST", "d") != "v" {
		t.Fatal("env")
	}
	if getenv("COLLECTOR_TEST_MISSING", "d") != "d" {
		t.Fatal("default")
	}
	if hostname() == "" {
		t.Fatal("hostname")
	}
	old := hostnameFn
	hostnameFn = func() (string, error) { return "", errors.New("no host") }
	t.Cleanup(func() { hostnameFn = old })
	if hostname() != "collector" {
		t.Fatal("hostname fallback")
	}
}

func TestSubscribeWithRetryCanceled(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := subscribeWithRetry(ctx, mq.NewClient("127.0.0.1:1"), log, "c1")
	if err == nil {
		t.Fatal("expected cancel")
	}
}

func TestSubscribeWithRetryTimeout(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := subscribeWithRetry(ctx, mq.NewClient("127.0.0.1:1"), log, "c1")
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestSubscribeWithRetrySuccess(t *testing.T) {
	addr := startTestBroker(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("MQ_TOPIC", "gpu-telemetry")
	t.Setenv("MQ_GROUP", "collectors")
	cons, err := subscribeWithRetry(context.Background(), mq.NewClient(addr), log, "c-ok")
	if err != nil {
		t.Fatal(err)
	}
	_ = cons.Close()
}

func TestRunCollector(t *testing.T) {
	addr := startTestBroker(t)
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(gw.Close)

	t.Setenv("MQ_ADDR", addr)
	t.Setenv("GATEWAY_URL", gw.URL)
	t.Setenv("CONSUMER_ID", "test-collector")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRunCollectorSubscribeFail(t *testing.T) {
	t.Setenv("MQ_ADDR", "127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("expected subscribe error")
	}
}

func TestMainExitOnSubscribeFail(t *testing.T) {
	oldWait := subscribeRetryWait
	subscribeRetryWait = time.Millisecond
	t.Cleanup(func() { subscribeRetryWait = oldWait })
	code := -1
	osExit = func(c int) { code = c }
	t.Cleanup(func() { osExit = os.Exit })
	t.Setenv("MQ_ADDR", "127.0.0.1:1")
	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not return")
	}
	if code != 1 {
		t.Fatalf("exit %d", code)
	}
}

func TestMainShutdown(t *testing.T) {
	addr := startTestBroker(t)
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(gw.Close)
	t.Setenv("MQ_ADDR", addr)
	t.Setenv("GATEWAY_URL", gw.URL)
	t.Setenv("CONSUMER_ID", "main-collector")
	osExit = func(int) { t.Error("os.Exit") }
	t.Cleanup(func() { osExit = os.Exit })
	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not return")
	}
}

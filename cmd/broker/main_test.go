package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	os.Setenv("METRICS_ADDR", "-")
	os.Exit(m.Run())
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func useQuietLog(t *testing.T) {
	t.Helper()
	old := newLogger
	newLogger = quietLog
	t.Cleanup(func() { newLogger = old })
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestRunBrokerShutdown(t *testing.T) {
	useQuietLog(t)
	addr := freeAddr(t)
	t.Setenv("MQ_ADDR", addr)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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

func TestRunBrokerListenError(t *testing.T) {
	useQuietLog(t)
	t.Setenv("MQ_ADDR", "127.0.0.1:999999")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestMainExitOnError(t *testing.T) {
	useQuietLog(t)
	code := -1
	osExit = func(c int) { code = c }
	t.Cleanup(func() { osExit = os.Exit })
	t.Setenv("MQ_ADDR", "127.0.0.1:999999")
	main()
	if code != 1 {
		t.Fatalf("exit code %d", code)
	}
}

func TestMainSignalShutdown(t *testing.T) {
	useQuietLog(t)
	addr := freeAddr(t)
	t.Setenv("MQ_ADDR", addr)
	osExit = func(int) { t.Error("os.Exit should not be called") }
	t.Cleanup(func() {
		osExit = os.Exit
		background = context.Background
	})
	ctx, cancel := context.WithCancel(context.Background())
	background = func() context.Context { return ctx }
	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not return")
	}
}

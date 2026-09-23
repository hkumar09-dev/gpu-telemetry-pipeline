package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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

func waitHealth(t *testing.T, addr string) {
	t.Helper()
	url := "http://" + addr + "/healthz"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("gateway not healthy")
}

func TestRunGatewayShutdown(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("DB_PATH", filepath.Join(t.TempDir(), "telemetry.json"))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()
	waitHealth(t, addr)
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

func TestRunGatewayOpenDBError(t *testing.T) {
	t.Setenv("DB_PATH", filepath.Join(t.TempDir(), "blocked"))
	if err := os.WriteFile(os.Getenv("DB_PATH"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DB_PATH", filepath.Join(os.Getenv("DB_PATH"), "telemetry.json"))
	if err := run(context.Background()); err == nil {
		t.Fatal("expected open db error")
	}
}

func TestRunGatewayListenError(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:1")
	t.Setenv("DB_PATH", filepath.Join(t.TempDir(), "telemetry.json"))
	if err := run(context.Background()); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestMainExitOnError(t *testing.T) {
	code := -1
	osExit = func(c int) { code = c }
	t.Cleanup(func() { osExit = os.Exit })
	t.Setenv("HTTP_ADDR", "127.0.0.1:1")
	t.Setenv("DB_PATH", filepath.Join(t.TempDir(), "telemetry.json"))
	main()
	if code != 1 {
		t.Fatalf("exit code %d", code)
	}
}

func TestMainSignalShutdown(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("DB_PATH", filepath.Join(t.TempDir(), "telemetry.json"))
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
	waitHealth(t, addr)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("main did not return")
	}
}

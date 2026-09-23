package mq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitAddr(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("server did not start")
	}
	return srv.Addr()
}

func TestServerUnknownOp(t *testing.T) {
	engine := NewEngine(Config{Partitions: 1})
	ctx := context.Background()
	srv := NewServer(ctx, engine, quietLog())
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	addr := waitAddr(t, srv)
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := writeFrame(conn, frame{Op: "nope"}); err != nil {
		t.Fatal(err)
	}
	resp, err := readFrame(bufio.NewReader(conn))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == "" {
		t.Fatal("expected unknown op error")
	}
}

func TestServerPublishSubscribeAck(t *testing.T) {
	engine := NewEngine(Config{Partitions: 1})
	ctx := context.Background()
	srv := NewServer(ctx, engine, quietLog())
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	addr := waitAddr(t, srv)
	t.Cleanup(func() { _ = srv.Close() })

	client := NewClient(addr)
	if err := client.Publish(ctx, "t", "k", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	cons, err := client.Subscribe(ctx, "t", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	d, err := cons.Consume(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != "hi" {
		t.Fatalf("%s", d.Payload)
	}
	if err := cons.Ack(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
}

func TestServerListenErrorAndUnknownFrameSize(t *testing.T) {
	engine := NewEngine(Config{Partitions: 1})
	t.Cleanup(engine.Close)
	srv := NewServer(context.Background(), engine, quietLog())
	if err := srv.ListenAndServe("127.0.0.1:999999"); err == nil {
		t.Fatal("expected listen error")
	}

	ctx := context.Background()
	good := NewServer(ctx, engine, quietLog())
	go func() { _ = good.ListenAndServe("127.0.0.1:0") }()
	addr := waitAddr(t, good)
	t.Cleanup(func() { _ = good.Close() })

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte{0, 0, 0, 0})
	time.Sleep(20 * time.Millisecond)

	client := NewClient(addr)
	cons, err := client.Subscribe(ctx, "t", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	_, _ = cons.Consume(ctx, 0)
	if err := cons.Ack(ctx, "missing"); err == nil {
		t.Fatal("expected ack error")
	}
}

func TestServerCloseWithoutListen(t *testing.T) {
	engine := NewEngine(Config{})
	srv := NewServer(context.Background(), engine, quietLog())
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadFrameTruncatedBody(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 8)
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(append(hdr[:], 1)))); err == nil {
		t.Fatal("expected truncated body")
	}
}

func TestReadFrameInvalidJSON(t *testing.T) {
	var hdr [4]byte
	body := []byte("{")
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	r := bufio.NewReader(bytes.NewReader(append(hdr[:], body...)))
	if _, err := readFrame(r); err == nil {
		t.Fatal("expected json error")
	}
	tooBig := make([]byte, 4)
	binary.BigEndian.PutUint32(tooBig, 17<<20)
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(tooBig))); err == nil {
		t.Fatal("expected size error")
	}
}

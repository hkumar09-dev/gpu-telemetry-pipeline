package mq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
)

func startBroker(t *testing.T) (*Server, *Client) {
	t.Helper()
	engine := NewEngine(Config{Partitions: 1, MaxPerPartition: 2})
	ctx := context.Background()
	srv := NewServer(ctx, engine, quietLog())
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("server did not start")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, NewClient(srv.Addr())
}

func TestClientPublishSubscribeNack(t *testing.T) {
	_, client := startBroker(t)
	ctx := context.Background()
	if err := client.Publish(ctx, "t", "k", []byte("msg")); err != nil {
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
	if err := cons.Nack(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
}

func TestClientConsumeTimeout(t *testing.T) {
	_, client := startBroker(t)
	ctx := context.Background()
	cons, err := client.Subscribe(ctx, "empty", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	_, err = cons.Consume(ctx, 30*time.Millisecond)
	if !errors.Is(err, constants.ErrTimeout) {
		t.Fatalf("want timeout, got %v", err)
	}
}

func TestClientPublishDialError(t *testing.T) {
	c := NewClient("127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected dial error")
	}
	if _, err := c.Subscribe(ctx, "t", "g", "c1"); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestClientPublishBackpressureError(t *testing.T) {
	engine := NewEngine(Config{Partitions: 1, MaxPerPartition: 1})
	ctx := context.Background()
	srv := NewServer(ctx, engine, quietLog())
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("server")
	}
	t.Cleanup(func() { _ = srv.Close() })
	c := NewClient(srv.Addr())
	if err := c.Publish(ctx, "t", "k", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "t", "k", []byte("2")); err == nil || !errors.Is(err, constants.ErrBackpressure) {
		t.Fatalf("expected backpressure, got %v", err)
	}
}

func TestClientFrameErrorsViaStub(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				f, err := readFrame(br)
				if err != nil {
					return
				}
				switch f.Op {
				case opPublish:
					_ = writeFrame(c, frame{Op: opResponse, Error: "nope"})
				case opSub:
					_ = writeFrame(c, frame{Op: opResponse, Error: "bad-sub"})
				default:
					_ = writeFrame(c, frame{Op: opResponse, Error: "x"})
				}
			}(conn)
		}
	}()
	c := NewClient(ln.Addr().String())
	ctx := context.Background()
	if err := c.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("publish error")
	}
	if _, err := c.Subscribe(ctx, "t", "g", "c1"); err == nil {
		t.Fatal("sub error")
	}
}

func TestClientWriteOnClosedConn(t *testing.T) {
	c := NewClient("unused")
	c.dial = func(ctx context.Context, addr string) (net.Conn, error) {
		a, b := net.Pipe()
		b.Close()
		a.Close()
		return a, nil
	}
	ctx := context.Background()
	_ = c.Publish(ctx, "t", "k", []byte("x"))
	_, _ = c.Subscribe(ctx, "t", "g", "c1")
}

func TestConsumeReadErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		f, _ := readFrame(br)
		if f.Op == opSub {
			_ = writeFrame(conn, frame{Op: opResponse})
		}
		f, _ = readFrame(br)
		if f.Op == opConsume {
			conn.Close()
		}
	}()
	c := NewClient(ln.Addr().String())
	cons, err := c.Subscribe(context.Background(), "t", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cons.Consume(context.Background(), time.Second)
	_ = cons.Ack(context.Background(), "id")
}

func TestPublishAndSubscribeDroppedResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			br := bufio.NewReader(conn)
			_, _ = readFrame(br)
			conn.Close()
		}
	}()
	c := NewClient(ln.Addr().String())
	ctx := context.Background()
	if err := c.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected publish read error")
	}
	if _, err := c.Subscribe(ctx, "t", "g", "c1"); err == nil {
		t.Fatal("expected subscribe read error")
	}
}

func TestConsumeAndAckAfterClose(t *testing.T) {
	_, client := startBroker(t)
	ctx := context.Background()
	cons, err := client.Subscribe(ctx, "t", "g", "c-close")
	if err != nil {
		t.Fatal(err)
	}
	tc := cons.(*tcpConsumer)
	_ = tc.conn.Close()
	_, _ = cons.Consume(ctx, time.Millisecond)
	_ = cons.Ack(ctx, "x")
}

type deadlineConn struct {
	net.Conn
}

func (d deadlineConn) SetReadDeadline(time.Time) error {
	return fmt.Errorf("deadline")
}

func TestConsumeDeadlineError(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	go func() {
		br := bufio.NewReader(b)
		_, _ = readFrame(br)
	}()
	c := &tcpConsumer{conn: deadlineConn{a}, reader: bufio.NewReader(a)}
	_, err := c.Consume(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected deadline error")
	}
}

func TestDeadlineHelper(t *testing.T) {
	d := deadline(context.Background(), time.Second)
	if time.Until(d) > time.Second || time.Until(d) < 0 {
		t.Fatalf("deadline %v", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	d2 := deadline(ctx, time.Hour)
	if !d2.Equal(func() time.Time { dl, _ := ctx.Deadline(); return dl }()) {
		t.Fatal("ctx deadline")
	}
}

func TestSubscribeWriteThenReadFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()
	c := NewClient(ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = c.Subscribe(ctx, "t", "g", "c1")
}

func TestConsumeNonTimeoutError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		f, _ := readFrame(br)
		if f.Op == opSub {
			_ = writeFrame(conn, frame{Op: opResponse})
		}
		_, _ = readFrame(br)
		_ = writeFrame(conn, frame{Op: opResponse, Error: "boom"})
	}()
	c := NewClient(ln.Addr().String())
	cons, err := c.Subscribe(context.Background(), "t", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cons.Consume(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected consume error")
	}
}

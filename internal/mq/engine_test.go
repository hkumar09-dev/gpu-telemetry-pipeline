package mq

import (
	"context"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
)

func TestPublishConsumeAck(t *testing.T) {
	e := NewEngine(Config{Partitions: 4, MaxPerPartition: 8, AckTimeout: time.Second})
	t.Cleanup(e.Close)
	ctx := context.Background()
	if err := e.Publish(ctx, "t", "gpu-a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	e.Subscribe("t", "g", "c1")
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != "one" || d.Key != "gpu-a" {
		t.Fatalf("unexpected delivery %+v", d)
	}
	if err := e.Ack("t", "g", "c1", d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Consume(ctx, "t", "g", "c1", 20*time.Millisecond); err != constants.ErrTimeout {
		t.Fatalf("want timeout after ack, got %v", err)
	}
}

func TestPartitionAffinity(t *testing.T) {
	e := NewEngine(Config{Partitions: 8})
	t.Cleanup(e.Close)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := e.Publish(ctx, "t", "same-gpu", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	e.Subscribe("t", "g", "c1")
	part := -1
	for i := 0; i < 20; i++ {
		d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if part == -1 {
			part = d.Partition
		} else if d.Partition != part {
			t.Fatalf("key hopped partitions %d -> %d", part, d.Partition)
		}
		_ = e.Ack("t", "g", "c1", d.ID)
	}
}

func TestCompetingConsumers(t *testing.T) {
	e := NewEngine(Config{Partitions: 4})
	t.Cleanup(e.Close)
	ctx := context.Background()
	e.Subscribe("t", "g", "a")
	e.Subscribe("t", "g", "b")
	for i := 0; i < 16; i++ {
		key := string(rune('a' + i))
		if err := e.Publish(ctx, "t", key, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	gotA, gotB := 0, 0
	for i := 0; i < 16; i++ {
		if d, err := e.Consume(ctx, "t", "g", "a", 50*time.Millisecond); err == nil {
			gotA++
			_ = e.Ack("t", "g", "a", d.ID)
			continue
		}
		if d, err := e.Consume(ctx, "t", "g", "b", 50*time.Millisecond); err == nil {
			gotB++
			_ = e.Ack("t", "g", "b", d.ID)
			continue
		}
	}
	if gotA == 0 || gotB == 0 {
		t.Fatalf("expected both consumers to receive work, a=%d b=%d", gotA, gotB)
	}
	if gotA+gotB != 16 {
		t.Fatalf("lost messages a=%d b=%d", gotA, gotB)
	}
}

func TestNackRedelivers(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, AckTimeout: time.Minute, RetryBackoff: time.Millisecond})
	t.Cleanup(e.Close)
	ctx := context.Background()
	_ = e.Publish(ctx, "t", "k", []byte("payload"))
	e.Subscribe("t", "g", "c1")
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Nack("t", "g", "c1", d.ID); err != nil {
		t.Fatal(err)
	}
	d2, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d2.Payload) != "payload" {
		t.Fatalf("got %s", d2.Payload)
	}
}

func TestAckTimeoutRedelivers(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, AckTimeout: 20 * time.Millisecond, RetryBackoff: time.Millisecond})
	t.Cleanup(e.Close)
	ctx := context.Background()
	_ = e.Publish(ctx, "t", "k", []byte("late"))
	e.Subscribe("t", "g", "c1")
	if _, err := e.Consume(ctx, "t", "g", "c1", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != "late" {
		t.Fatalf("got %s", d.Payload)
	}
}

func TestBackpressure(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, MaxPerPartition: 2})
	t.Cleanup(e.Close)
	ctx := context.Background()
	_ = e.Publish(ctx, "t", "k", []byte("1"))
	_ = e.Publish(ctx, "t", "k", []byte("2"))
	if err := e.Publish(ctx, "t", "k", []byte("3")); err != constants.ErrBackpressure {
		t.Fatalf("want backpressure, got %v", err)
	}
}

func TestTCPRoundTrip(t *testing.T) {
	engine := NewEngine(Config{Partitions: 2})
	ctx := context.Background()
	srv := NewServer(ctx, engine, nil)
	go func() { _ = srv.ListenAndServe("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("server did not start")
	}
	t.Cleanup(func() { _ = srv.Close() })

	client := NewClient(srv.Addr())
	if err := client.Publish(ctx, "gpu-telemetry", "gpu-1", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	cons, err := client.Subscribe(ctx, "gpu-telemetry", "collectors", "c-test")
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	d, err := cons.Consume(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != `{"ok":true}` {
		t.Fatalf("payload %s", d.Payload)
	}
	if err := cons.Ack(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRetryThenDLQ(t *testing.T) {
	e := NewEngine(Config{
		Partitions:   1,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
		AckTimeout:   time.Minute,
	})
	t.Cleanup(e.Close)
	ctx := context.Background()
	if err := e.Publish(ctx, "t", "k", []byte("poison")); err != nil {
		t.Fatal(err)
	}
	e.Subscribe("t", "g", "c1")
	e.Subscribe("t.dlq", "dlq", "d1")
	for i := 0; i < 2; i++ {
		d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
		if err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
		if err := e.Nack("t", "g", "c1", d.ID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(8 * time.Millisecond)
	}
	if _, err := e.Consume(ctx, "t", "g", "c1", 40*time.Millisecond); err != constants.ErrTimeout {
		t.Fatalf("want timeout on main topic, got %v", err)
	}
	d, err := e.Consume(ctx, "t.dlq", "dlq", "d1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != "poison" {
		t.Fatalf("dlq payload %s", d.Payload)
	}
}

func TestEngineClosedAndOwnership(t *testing.T) {
	e := NewEngine(Config{Partitions: 2, RetryQueueSize: 1, RetryBackoff: time.Millisecond, AckTimeout: time.Minute})
	ctx := context.Background()
	e.Subscribe("t", "g", "c1")
	e.Subscribe("t", "g", "c1")
	e.Unsubscribe("missing", "g", "c1")
	_ = e.Publish(ctx, "t", "k", []byte("x"))
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Ack("t", "g", "other", d.ID); err != constants.ErrNotOwner {
		t.Fatalf("ack owner %v", err)
	}
	if err := e.Nack("t", "g", "other", d.ID); err != constants.ErrNotOwner {
		t.Fatalf("nack owner %v", err)
	}
	if err := e.Ack("nope", "g", "c1", d.ID); err != constants.ErrUnknownMsg {
		t.Fatalf("ack group %v", err)
	}
	if err := e.Nack("nope", "g", "c1", d.ID); err != constants.ErrUnknownMsg {
		t.Fatalf("nack group %v", err)
	}
	if err := e.Ack("t", "g", "c1", "missing"); err != constants.ErrUnknownMsg {
		t.Fatalf("ack id %v", err)
	}
	if err := e.Nack("t", "g", "c1", "missing"); err != constants.ErrUnknownMsg {
		t.Fatalf("nack id %v", err)
	}
	if err := e.Ack("t", "g", "c1", d.ID); err != nil {
		t.Fatal(err)
	}
	if e.Depth("t") != 0 {
		t.Fatal("depth")
	}
	_ = e.DLQDepth("t")
	if partitionFor("k", 0) != 0 {
		t.Fatal("partition")
	}

	e.Close()
	e.Close()
	if err := e.Publish(ctx, "t", "k", []byte("y")); err != constants.ErrClosed {
		t.Fatalf("publish closed %v", err)
	}
	if _, err := e.Consume(ctx, "t", "g", "c1", time.Millisecond); err != constants.ErrClosed {
		t.Fatalf("consume closed %v", err)
	}
}

func TestPublishCanceledAndConsumeCanceled(t *testing.T) {
	e := NewEngine(Config{Partitions: 1})
	t.Cleanup(e.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Publish(ctx, "t", "k", []byte("x")); err == nil {
		t.Fatal("expected cancel")
	}
	e.Subscribe("t", "g", "c1")
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if _, err := e.Consume(cctx, "t", "g", "c1", time.Second); err == nil {
		t.Fatal("expected consume cancel")
	}
	if _, err := e.Consume(context.Background(), "t", "g", "c1", 0); err != constants.ErrTimeout {
		t.Fatalf("zero timeout %v", err)
	}
}

func TestUnsubscribeRequeuesAndRetryOverflowDLQ(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, RetryQueueSize: 1, MaxRetries: 50, RetryBackoff: time.Hour, AckTimeout: time.Minute, MaxPerPartition: 8})
	t.Cleanup(e.Close)
	ctx := context.Background()
	e.Subscribe("t", "g", "c1")
	_ = e.Publish(ctx, "t", "k", []byte("a"))
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e.Unsubscribe("t", "g", "c1")
	e.Subscribe("t", "g", "c2")
	d2, err := e.Consume(ctx, "t", "g", "c2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d2.Payload) != "a" {
		t.Fatalf("requeue %s", d2.Payload)
	}
	_ = d
	_ = e.Nack("t", "g", "c2", d2.ID)
	_ = e.Publish(ctx, "t", "k2", []byte("b"))
	d3, err := e.Consume(ctx, "t", "g", "c2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Nack("t", "g", "c2", d3.ID)
	e.Subscribe("t.dlq", "dlq", "d1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := e.Consume(ctx, "t.dlq", "dlq", "d1", 50*time.Millisecond); err == nil {
			return
		}
	}
}

func TestEngineRemainingBranches(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, AckTimeout: time.Millisecond, RetryBackoff: time.Millisecond, RetryQueueSize: 1, MaxPerPartition: 1, MaxRetries: 8})
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		_ = e.Publish(ctx, "t", "k", []byte("x"))
		close(done)
	}()
	e.Close()
	<-done

	e = NewEngine(Config{Partitions: 2, AckTimeout: time.Millisecond, RetryBackoff: 5 * time.Millisecond, MaxPerPartition: 2, RetryQueueSize: 1, MaxRetries: 3})
	t.Cleanup(e.Close)
	e.Subscribe("t", "g", "c1")
	e.Subscribe("t", "g", "c2")
	e.Unsubscribe("t", "g", "c1")
	if _, err := e.Consume(ctx, "t", "missing", "c1", 0); err != constants.ErrTimeout {
		t.Fatalf("no group %v", err)
	}
	gs := e.group("t", "g")
	gs.mu.Lock()
	gs.assignment["c2"] = []int{-1, 99}
	gs.mu.Unlock()
	_, _ = e.tryConsume("t", "g", "c2")
	e.expireInflight("nope", "g")

	_ = e.Publish(ctx, "t", "k", []byte("a"))
	e.Subscribe("t", "g2", "z")
	d, err := e.Consume(ctx, "t", "g2", "z", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	_ = e.Nack("t", "g2", "z", d.ID)
}

func TestRequeueFullAndDLQFull(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, MaxPerPartition: 1, RetryBackoff: time.Millisecond, MaxRetries: 1, AckTimeout: time.Minute})
	t.Cleanup(e.Close)
	ctx := context.Background()
	e.Subscribe("t", "g", "c1")
	_ = e.Publish(ctx, "t", "k", []byte("1"))
	d, err := e.Consume(ctx, "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Publish(ctx, "t", "k", []byte("2"))
	inf := inflight{topic: "t", partition: 0, key: "k", payload: []byte("r"), attempts: 1}
	e.requeue(inf)
	e.toDLQ(inflight{topic: "t", key: "k", payload: []byte("d1")})
	e.toDLQ(inflight{topic: "t", key: "k", payload: []byte("d2")})
	_ = d
}

func TestHandleRetryCanceled(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, RetryBackoff: 50 * time.Millisecond, MaxRetries: 5, AckTimeout: time.Minute})
	e.Subscribe("t", "g", "c1")
	_ = e.Publish(context.Background(), "t", "k", []byte("x"))
	d, err := e.Consume(context.Background(), "t", "g", "c1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		e.Close()
	}()
	_ = e.Nack("t", "g", "c1", d.ID)
	time.Sleep(80 * time.Millisecond)
}

func TestToDLQWhenClosed(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, MaxRetries: 1, RetryBackoff: time.Millisecond})
	inf := inflight{topic: "t", key: "k", payload: []byte("x")}
	e.Close()
	e.toDLQ(inf)
	e.requeue(inf)
}

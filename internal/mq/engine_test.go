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

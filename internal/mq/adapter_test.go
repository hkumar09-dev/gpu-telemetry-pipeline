package mq

import (
	"context"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/ports"
)

func TestEnginePublisherAndConsumer(t *testing.T) {
	e := NewEngine(Config{Partitions: 2, AckTimeout: time.Second})
	t.Cleanup(e.Close)

	var pub ports.Publisher = EnginePublisher{Engine: e}
	var factory ports.ConsumerFactory = EnginePublisher{Engine: e}

	ctx := context.Background()
	if err := pub.Publish(ctx, "gpu-telemetry", "gpu-1", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	cons, err := factory.Subscribe(ctx, "gpu-telemetry", "collectors", "c1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cons.Close() })

	d, err := cons.Consume(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d.Payload) != `{"ok":true}` || d.Key != "gpu-1" {
		t.Fatalf("%+v", d)
	}
	if err := cons.Ack(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
}

func TestEngineConsumerNack(t *testing.T) {
	e := NewEngine(Config{Partitions: 1, RetryBackoff: time.Millisecond, AckTimeout: time.Minute})
	t.Cleanup(e.Close)
	pub := EnginePublisher{Engine: e}
	ctx := context.Background()
	_ = pub.Publish(ctx, "t", "k", []byte("x"))
	cons, err := pub.Subscribe(ctx, "t", "g", "c1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	d, err := cons.Consume(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := cons.Nack(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	d2, err := cons.Consume(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(d2.Payload) != "x" {
		t.Fatalf("got %s", d2.Payload)
	}
}

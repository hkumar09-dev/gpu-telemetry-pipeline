package mq

import (
	"context"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/ports"
)

// EnginePublisher adapts Engine to ports.Publisher.
type EnginePublisher struct {
	Engine *Engine
}

func (p EnginePublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	return p.Engine.Publish(ctx, topic, key, payload)
}

// EngineConsumer adapts Engine to ports.Consumer.
type EngineConsumer struct {
	Engine     *Engine
	Topic      string
	Group      string
	ConsumerID string
}

func (c EngineConsumer) Consume(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
	d, err := c.Engine.Consume(ctx, c.Topic, c.Group, c.ConsumerID, timeout)
	if err != nil {
		return ports.Delivery{}, err
	}
	return ports.Delivery{
		ID:        d.ID,
		Topic:     d.Topic,
		Partition: d.Partition,
		Offset:    d.Offset,
		Key:       d.Key,
		Payload:   d.Payload,
	}, nil
}

func (c EngineConsumer) Ack(ctx context.Context, id string) error {
	return c.Engine.Ack(c.Topic, c.Group, c.ConsumerID, id)
}

func (c EngineConsumer) Nack(ctx context.Context, id string) error {
	return c.Engine.Nack(c.Topic, c.Group, c.ConsumerID, id)
}

func (c EngineConsumer) Close() error {
	c.Engine.Unsubscribe(c.Topic, c.Group, c.ConsumerID)
	return nil
}

func (p EnginePublisher) Subscribe(_ context.Context, topic, group, consumerID string) (ports.Consumer, error) {
	p.Engine.Subscribe(topic, group, consumerID)
	return EngineConsumer{Engine: p.Engine, Topic: topic, Group: group, ConsumerID: consumerID}, nil
}

package mq

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
	"github.com/gpu-telemetry-pipeline/internal/ports"
)

type op string

const (
	opPublish  op = "publish"
	opConsume  op = "consume"
	opAck      op = "ack"
	opNack     op = "nack"
	opSub      op = "subscribe"
	opUnsub    op = "unsubscribe"
	opResponse op = "response"
)

// frame is a message frame.
type frame struct {
	Op         op     `json:"op"`
	Topic      string `json:"topic,omitempty"`
	Group      string `json:"group,omitempty"`
	ConsumerID string `json:"consumer_id,omitempty"`
	Key        string `json:"key,omitempty"`
	ID         string `json:"id,omitempty"`
	Partition  int    `json:"partition,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	TimeoutMS  int64  `json:"timeout_ms,omitempty"`
	Payload    []byte `json:"payload,omitempty"`
	Error      string `json:"error,omitempty"`
}

// writeFrame writes a frame to the writer.
func writeFrame(w io.Writer, f frame) error {
	body, err := json.Marshal(f)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// readFrame reads a frame from the reader.
func readFrame(r *bufio.Reader) (frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > 16<<20 {
		return frame{}, fmt.Errorf("invalid frame size %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return frame{}, err
	}
	var f frame
	if err := json.Unmarshal(body, &f); err != nil {
		return frame{}, err
	}
	return f, nil
}

// Client is a TCP publisher / consumer-factory for the custom broker.
type Client struct {
	addr string
	dial func(ctx context.Context, addr string) (net.Conn, error)
}

// NewClient creates a new client.
func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
		dial: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
	}
}

// Publish publishes a message.
func (c *Client) Publish(ctx context.Context, topic, key string, payload []byte) error {
	conn, err := c.dial(ctx, c.addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline(ctx, 10*time.Second))
	br := bufio.NewReader(conn)
	if err := writeFrame(conn, frame{Op: opPublish, Topic: topic, Key: key, Payload: payload}); err != nil {
		return err
	}

	resp, err := readFrame(br)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}

	return nil
}

// Subscribe subscribes to a topic.
func (c *Client) Subscribe(ctx context.Context, topic, group, consumerID string) (ports.Consumer, error) {
	conn, err := c.dial(ctx, c.addr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(deadline(ctx, 10*time.Second))
	br := bufio.NewReader(conn)
	if err := writeFrame(conn, frame{Op: opSub, Topic: topic, Group: group, ConsumerID: consumerID}); err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := readFrame(br)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.Error != "" {
		conn.Close()
		return nil, fmt.Errorf("%s", resp.Error)
	}
	_ = conn.SetDeadline(time.Time{})
	return &tcpConsumer{
		conn:       conn,
		reader:     br,
		topic:      topic,
		group:      group,
		consumerID: consumerID,
	}, nil
}

// tcpConsumer is a TCP consumer.
type tcpConsumer struct {
	mu         sync.Mutex
	conn       net.Conn
	reader     *bufio.Reader
	topic      string
	group      string
	consumerID string
}

// Consume consumes a message.
func (c *tcpConsumer) Consume(ctx context.Context, timeout time.Duration) (ports.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeFrame(c.conn, frame{
		Op:         opConsume,
		Topic:      c.topic,
		Group:      c.group,
		ConsumerID: c.consumerID,
		TimeoutMS:  timeout.Milliseconds(),
	}); err != nil {
		return ports.Delivery{}, err
	}

	if err := c.conn.SetReadDeadline(deadline(ctx, timeout+2*time.Second)); err != nil {
		return ports.Delivery{}, err
	}

	resp, err := readFrame(c.reader)

	_ = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return ports.Delivery{}, err
	}
	if resp.Error != "" {
		if resp.Error == constants.ErrTimeout.Error() {
			return ports.Delivery{}, constants.ErrTimeout
		}
		return ports.Delivery{}, fmt.Errorf("%s", resp.Error)
	}
	return ports.Delivery{
		ID:        resp.ID,
		Topic:     resp.Topic,
		Partition: resp.Partition,
		Offset:    resp.Offset,
		Key:       resp.Key,
		Payload:   resp.Payload,
	}, nil
}

func (c *tcpConsumer) Ack(ctx context.Context, id string) error {
	return c.ctrl(ctx, opAck, id)
}

func (c *tcpConsumer) Nack(ctx context.Context, id string) error {
	return c.ctrl(ctx, opNack, id)
}

func (c *tcpConsumer) ctrl(ctx context.Context, o op, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetDeadline(deadline(ctx, 10*time.Second))
	defer c.conn.SetDeadline(time.Time{})
	if err := writeFrame(c.conn, frame{
		Op:         o,
		Topic:      c.topic,
		Group:      c.group,
		ConsumerID: c.consumerID,
		ID:         id,
	}); err != nil {
		return err
	}

	resp, err := readFrame(c.reader)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}

	return nil
}

func (c *tcpConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = writeFrame(c.conn, frame{Op: opUnsub, Topic: c.topic, Group: c.group, ConsumerID: c.consumerID})
	return c.conn.Close()
}

func deadline(ctx context.Context, fallback time.Duration) time.Time {
	if dl, ok := ctx.Deadline(); ok {
		return dl
	}
	return time.Now().Add(fallback)
}

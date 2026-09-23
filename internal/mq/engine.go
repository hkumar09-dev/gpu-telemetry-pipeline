package mq

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
)

// Delivery is one in-flight message held until Ack or Nack.
type Delivery struct {
	ID        string
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Payload   []byte
	Attempts  int
}

// Config controls broker capacity, retries, and delivery semantics.
type Config struct {
	Partitions      int
	MaxPerPartition int
	AckTimeout      time.Duration
	MaxRetries      int
	RetryBackoff    time.Duration
	RetryQueueSize  int
}

func (c Config) withDefaults() Config {
	if c.Partitions <= 0 {
		c.Partitions = 8
	}
	if c.MaxPerPartition <= 0 {
		c.MaxPerPartition = 10_000
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = 30 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 5 * time.Millisecond
	}
	if c.RetryQueueSize <= 0 {
		c.RetryQueueSize = 1024
	}
	return c
}

type record struct {
	offset   int64
	key      string
	payload  []byte
	attempts int
}

type inflight struct {
	id        string
	topic     string
	partition int
	offset    int64
	key       string
	payload   []byte
	attempts  int
	consumer  string
	deadline  time.Time
}

type partition struct {
	mu  sync.Mutex
	next int64
	buf  chan record
}

type groupState struct {
	mu         sync.Mutex
	members    []string
	assignment map[string][]int
	inflight   map[string]inflight
}

// Engine is an in-process, multi-partition topic broker.
// Each partition is a bounded channel (backpressure when full).
// Consumer groups share work by partition assignment.
// Unacked messages retry, then land on topic+".dlq".
type Engine struct {
	cfg     Config
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
	closed  bool
	topics  map[string][]*partition
	groups  map[string]*groupState
	wake    chan struct{}
	retries chan inflight
}

func NewEngine(cfg Config) *Engine {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		topics:  make(map[string][]*partition),
		groups:  make(map[string]*groupState),
		wake:    make(chan struct{}, 1),
		retries: make(chan inflight, cfg.RetryQueueSize),
	}
	go e.retryLoop()
	go e.ackTimeoutLoop()
	return e
}

func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.mu.Unlock()
	e.cancel()
	e.signal()
}

func (e *Engine) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if err := e.errIfClosed(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return constants.ErrClosed
	}
	parts := e.ensureTopicLocked(topic)
	e.mu.Unlock()

	idx := partitionFor(key, len(parts))
	p := parts[idx]
	p.mu.Lock()
	rec := record{
		offset:  p.next,
		key:     key,
		payload: append([]byte(nil), payload...),
	}
	p.next++
	p.mu.Unlock()

	select {
	case p.buf <- rec:
		e.signal()
		return nil
	default:
		return constants.ErrBackpressure
	}
}

func (e *Engine) Subscribe(topic, group, consumerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.ensureTopicLocked(topic)
	gs := e.ensureGroupLocked(topic, group)
	gs.mu.Lock()
	defer gs.mu.Unlock()
	for _, m := range gs.members {
		if m == consumerID {
			return
		}
	}
	gs.members = append(gs.members, consumerID)
	gs.rebalance(e.cfg.Partitions)
	e.signal()
}

func (e *Engine) Unsubscribe(topic, group, consumerID string) {
	e.mu.Lock()
	gs := e.groups[groupKey(topic, group)]
	e.mu.Unlock()
	if gs == nil {
		return
	}
	gs.mu.Lock()
	members := gs.members[:0]
	for _, m := range gs.members {
		if m != consumerID {
			members = append(members, m)
		}
	}
	gs.members = members
	var pending []inflight
	for id, inf := range gs.inflight {
		if inf.consumer == consumerID {
			pending = append(pending, inf)
			delete(gs.inflight, id)
		}
	}
	gs.rebalance(e.cfg.Partitions)
	gs.mu.Unlock()
	for _, inf := range pending {
		e.requeue(inf)
	}
	e.signal()
}

func (e *Engine) Consume(ctx context.Context, topic, group, consumerID string, timeout time.Duration) (Delivery, error) {
	deadline := time.Now().Add(timeout)
	for {
		if err := e.errIfClosed(); err != nil {
			return Delivery{}, err
		}
		if d, ok := e.tryConsume(topic, group, consumerID); ok {
			return d, nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return Delivery{}, constants.ErrTimeout
		}
		remaining := time.Until(deadline)
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Delivery{}, ctx.Err()
		case <-timer.C:
			return Delivery{}, constants.ErrTimeout
		case <-e.wake:
			timer.Stop()
		}
	}
}

func (e *Engine) Ack(topic, group, consumerID, id string) error {
	gs := e.group(topic, group)
	if gs == nil {
		return constants.ErrUnknownMsg
	}
	gs.mu.Lock()
	defer gs.mu.Unlock()
	inf, ok := gs.inflight[id]
	if !ok {
		return constants.ErrUnknownMsg
	}
	if inf.consumer != consumerID {
		return constants.ErrNotOwner
	}
	delete(gs.inflight, id)
	return nil
}

func (e *Engine) Nack(topic, group, consumerID, id string) error {
	gs := e.group(topic, group)
	if gs == nil {
		return constants.ErrUnknownMsg
	}
	gs.mu.Lock()
	inf, ok := gs.inflight[id]
	if !ok {
		gs.mu.Unlock()
		return constants.ErrUnknownMsg
	}
	if inf.consumer != consumerID {
		gs.mu.Unlock()
		return constants.ErrNotOwner
	}
	delete(gs.inflight, id)
	gs.mu.Unlock()
	inf.attempts++
	e.enqueueRetry(inf)
	return nil
}

func (e *Engine) Depth(topic string) int {
	e.mu.RLock()
	parts := e.topics[topic]
	e.mu.RUnlock()
	n := 0
	for _, p := range parts {
		n += len(p.buf)
	}
	return n
}

func (e *Engine) DLQDepth(topic string) int {
	return e.Depth(dlqTopic(topic))
}

func (e *Engine) tryConsume(topic, group, consumerID string) (Delivery, bool) {
	e.expireInflight(topic, group)

	e.mu.RLock()
	parts := e.topics[topic]
	gs := e.groups[groupKey(topic, group)]
	e.mu.RUnlock()
	if gs == nil || len(parts) == 0 {
		return Delivery{}, false
	}

	gs.mu.Lock()
	assigned := append([]int(nil), gs.assignment[consumerID]...)
	gs.mu.Unlock()

	now := time.Now()
	for _, idx := range assigned {
		if idx < 0 || idx >= len(parts) {
			continue
		}
		p := parts[idx]
		select {
		case rec := <-p.buf:
			id := fmt.Sprintf("%s-%d-%d-%d", topic, idx, rec.offset, now.UnixNano())
			d := Delivery{
				ID:        id,
				Topic:     topic,
				Partition: idx,
				Offset:    rec.offset,
				Key:       rec.key,
				Payload:   rec.payload,
				Attempts:  rec.attempts,
			}
			gs.mu.Lock()
			gs.inflight[id] = inflight{
				id:        id,
				topic:     topic,
				partition: idx,
				offset:    rec.offset,
				key:       rec.key,
				payload:   rec.payload,
				attempts:  rec.attempts,
				consumer:  consumerID,
				deadline:  now.Add(e.cfg.AckTimeout),
			}
			gs.mu.Unlock()
			return d, true
		default:
		}
	}
	return Delivery{}, false
}

func (e *Engine) expireInflight(topic, group string) {
	gs := e.group(topic, group)
	if gs == nil {
		return
	}
	e.releaseExpired(gs)
}

func (e *Engine) expireAllInflight() {
	e.mu.RLock()
	groups := make([]*groupState, 0, len(e.groups))
	for _, gs := range e.groups {
		groups = append(groups, gs)
	}
	e.mu.RUnlock()
	for _, gs := range groups {
		e.releaseExpired(gs)
	}
}

func (e *Engine) releaseExpired(gs *groupState) {
	now := time.Now()
	gs.mu.Lock()
	var expired []inflight
	for id, inf := range gs.inflight {
		if now.After(inf.deadline) {
			expired = append(expired, inf)
			delete(gs.inflight, id)
		}
	}
	gs.mu.Unlock()
	for _, inf := range expired {
		inf.attempts++
		e.enqueueRetry(inf)
	}
}

func (e *Engine) enqueueRetry(inf inflight) {
	select {
	case <-e.ctx.Done():
		return
	case e.retries <- inf:
	default:
		e.toDLQ(inf)
	}
}

func (e *Engine) retryLoop() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case inf := <-e.retries:
			e.handleRetry(inf)
		}
	}
}

func (e *Engine) handleRetry(inf inflight) {
	if inf.attempts >= e.cfg.MaxRetries {
		e.toDLQ(inf)
		return
	}
	backoff := e.cfg.RetryBackoff * time.Duration(inf.attempts)
	if backoff <= 0 {
		backoff = e.cfg.RetryBackoff
	}
	timer := time.NewTimer(backoff)
	select {
	case <-e.ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}
	e.requeue(inf)
}

func (e *Engine) ackTimeoutLoop() {
	interval := e.cfg.AckTimeout / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.expireAllInflight()
		}
	}
}

func (e *Engine) requeue(inf inflight) {
	e.mu.RLock()
	parts := e.topics[inf.topic]
	e.mu.RUnlock()
	if inf.partition < 0 || inf.partition >= len(parts) {
		return
	}
	rec := record{
		offset:   inf.offset,
		key:      inf.key,
		payload:  inf.payload,
		attempts: inf.attempts,
	}
	select {
	case <-e.ctx.Done():
		return
	case parts[inf.partition].buf <- rec:
		e.signal()
	case <-time.After(e.cfg.RetryBackoff):
		e.enqueueRetry(inf)
	}
}

func (e *Engine) toDLQ(inf inflight) {
	topic := dlqTopic(inf.topic)
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	parts := e.ensureTopicLocked(topic)
	e.mu.Unlock()
	idx := partitionFor(inf.key, len(parts))
	p := parts[idx]
	p.mu.Lock()
	rec := record{
		offset:   p.next,
		key:      inf.key,
		payload:  inf.payload,
		attempts: inf.attempts,
	}
	p.next++
	p.mu.Unlock()
	select {
	case p.buf <- rec:
		e.signal()
	default:
	}
}

func (e *Engine) ensureTopicLocked(topic string) []*partition {
	parts, ok := e.topics[topic]
	if ok {
		return parts
	}
	parts = make([]*partition, e.cfg.Partitions)
	for i := range parts {
		parts[i] = &partition{buf: make(chan record, e.cfg.MaxPerPartition)}
	}
	e.topics[topic] = parts
	return parts
}

func (e *Engine) ensureGroupLocked(topic, group string) *groupState {
	k := groupKey(topic, group)
	gs, ok := e.groups[k]
	if ok {
		return gs
	}
	gs = &groupState{
		assignment: make(map[string][]int),
		inflight:   make(map[string]inflight),
	}
	e.groups[k] = gs
	return gs
}

func (e *Engine) group(topic, group string) *groupState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.groups[groupKey(topic, group)]
}

func (e *Engine) errIfClosed() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return constants.ErrClosed
	}
	return nil
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (g *groupState) rebalance(n int) {
	g.assignment = make(map[string][]int, len(g.members))
	if len(g.members) == 0 {
		return
	}
	for i := 0; i < n; i++ {
		owner := g.members[i%len(g.members)]
		g.assignment[owner] = append(g.assignment[owner], i)
	}
}

func groupKey(topic, group string) string {
	return topic + "/" + group
}

func dlqTopic(topic string) string {
	return topic + ".dlq"
}

func partitionFor(key string, n int) int {
	if n <= 0 {
		return 0
	}
	sum := sha1.Sum([]byte(key))
	v := binary.BigEndian.Uint64(sum[:8])
	return int(v % uint64(n))
}

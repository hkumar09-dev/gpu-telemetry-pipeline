package mq

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpu-telemetry-pipeline/constants"
)

// Server exposes Engine over a length-prefixed JSON TCP protocol.
type Server struct {
	ctx      context.Context
	engine   *Engine
	log      *slog.Logger
	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
	stopping atomic.Bool
}

func NewServer(ctx context.Context, engine *Engine, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{ctx: ctx, engine: engine, log: log, conns: make(map[net.Conn]struct{})}
}

// ListenAndServe starts a broker server.
func (s *Server) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.log.Error("listen failed", "err", err)
		return err
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	s.log.Info("broker listening", "addr", listener.Addr().String())
	return s.serve(listener)
}

func (s *Server) serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.stopping.Load() {
				s.log.Info("broker closed")
				return nil
			}
			s.log.Error("accept failed", "err", err)
			return err
		}
		if s.stopping.Load() {
			_ = conn.Close()
			continue
		}
		s.track(conn)
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer s.untrack(c)
			s.handle(c)
		}(conn)
	}
}

// Shutdown stops accepting, waits for in-flight TCP handlers, then closes the engine.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stopping.Store(true)

	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.log.Error("close listener", "err", err)
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.engine.Close()
		return nil
	case <-ctx.Done():
		s.forceCloseConns()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
		}
		s.engine.Close()
		return ctx.Err()
	}
}

// Close is Shutdown with a 5s timeout (tests and fallbacks).
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.Shutdown(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func (s *Server) track(c net.Conn) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Server) forceCloseConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	var sub *struct{ topic, group, id string }
	for {
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		f, err := readFrame(br)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if s.stopping.Load() {
					return
				}
				continue
			}
			return
		}
		resp := s.dispatch(f, &sub)
		if err := writeFrame(conn, resp); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(f frame, sub **struct{ topic, group, id string }) frame {
	switch f.Op {
	case opPublish:
		if s.stopping.Load() {
			return errFrame(constants.ErrClosed)
		}
		err := s.engine.Publish(s.ctx, f.Topic, f.Key, f.Payload)
		return errFrame(err)
	case opSub:
		if s.stopping.Load() {
			return errFrame(constants.ErrClosed)
		}
		s.engine.Subscribe(f.Topic, f.Group, f.ConsumerID)
		*sub = &struct{ topic, group, id string }{f.Topic, f.Group, f.ConsumerID}
		return frame{Op: opResponse}
	case opUnsub:
		s.engine.Unsubscribe(f.Topic, f.Group, f.ConsumerID)
		*sub = nil
		return frame{Op: opResponse}
	case opConsume:
		timeout := time.Duration(f.TimeoutMS) * time.Millisecond
		if timeout <= 0 {
			timeout = time.Second
		}
		d, err := s.engine.Consume(s.ctx, f.Topic, f.Group, f.ConsumerID, timeout)
		if err != nil {
			return errFrame(err)
		}
		return frame{
			Op:        opResponse,
			Topic:     d.Topic,
			ID:        d.ID,
			Partition: d.Partition,
			Offset:    d.Offset,
			Key:       d.Key,
			Payload:   d.Payload,
		}
	case opAck:
		return errFrame(s.engine.Ack(f.Topic, f.Group, f.ConsumerID, f.ID))
	case opNack:
		return errFrame(s.engine.Nack(f.Topic, f.Group, f.ConsumerID, f.ID))
	default:
		return errFrame(errors.New("unknown op"))
	}
}

func errFrame(err error) frame {
	if err == nil {
		return frame{Op: opResponse}
	}
	return frame{Op: opResponse, Error: err.Error()}
}

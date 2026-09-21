package mq

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Server exposes Engine over a length-prefixed JSON TCP protocol.
type Server struct {
	engine *Engine
	log    *slog.Logger
	mu     sync.Mutex
	ln     net.Listener
	wg     sync.WaitGroup
}

func NewServer(engine *Engine, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{engine: engine, log: log}
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("broker listening", "addr", ln.Addr().String())
	return s.serve(ln)
}

func (s *Server) serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	s.engine.Close()
	s.wg.Wait()
	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	var sub *struct{ topic, group, id string }
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		f, err := readFrame(br)
		if err != nil {
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
		err := s.engine.Publish(context.Background(), f.Topic, f.Key, f.Payload)
		return errFrame(err)
	case opSub:
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
		d, err := s.engine.Consume(context.Background(), f.Topic, f.Group, f.ConsumerID, timeout)
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

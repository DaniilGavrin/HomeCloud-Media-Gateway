package mediaframe

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
)

type pending struct{ seq, end uint64 }

// Sender holds at most WindowBytes of unacknowledged data and one 64 KiB read
// buffer. It does not store previous payloads or materialize an entire file.
// One goroutine calls Run and another may call Acknowledge.
type Sender struct {
	id                    Identity
	mu                    sync.Mutex
	wake                  chan struct{}
	pending               []pending
	sent, acked, sequence uint64
	lastAck               pending
	hasAck                bool
	finished              bool
}

func NewSender(id Identity) *Sender {
	return &Sender{id: id, wake: make(chan struct{}), pending: make([]pending, 0, MaxPendingFrames)}
}

func (s *Sender) InFlight() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.sent - s.acked }

func (s *Sender) Acknowledge(ack Ack) error {
	if ack.V != Version || ack.Type != "media.ack" || ack.RoomID != s.id.RoomID ||
		ack.PlaybackID != s.id.PlaybackID || ack.Generation != s.id.Generation || ack.Epoch != s.id.Epoch {
		return ErrAck
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasAck && ack.Sequence == s.lastAck.seq && ack.ReceivedBytes == s.lastAck.end {
		return nil
	}
	// No forward credit is allowed unless it acknowledges a genuine sent frame.
	index := -1
	for i, frame := range s.pending {
		if frame.seq == ack.Sequence && frame.end == ack.ReceivedBytes {
			index = i
			break
		}
	}
	if index == -1 {
		return ErrAck
	}
	s.lastAck = s.pending[index]
	s.hasAck = true
	s.acked = s.lastAck.end
	s.pending = s.pending[index+1:]
	close(s.wake)
	s.wake = make(chan struct{})
	return nil
}

// waitCredit blocks reading the source when the receiver cannot accept more.
func (s *Sender) waitCredit(ctx context.Context) (int, error) {
	for {
		s.mu.Lock()
		outstanding := s.sent - s.acked
		if outstanding < WindowBytes && len(s.pending) < MaxPendingFrames {
			free := uint64(WindowBytes) - outstanding
			if free > MaxPayloadBytes {
				free = MaxPayloadBytes
			}
			s.mu.Unlock()
			return int(free), nil
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (s *Sender) record(size int, eof bool) (seq, offset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished || len(s.pending) >= MaxPendingFrames && !eof || s.sequence == math.MaxUint64 || s.sent > math.MaxUint64-uint64(size) || s.sent-s.acked+uint64(size) > WindowBytes {
		return 0, 0, ErrFrame
	}
	seq, offset = s.sequence, s.sent
	s.sequence++
	if eof {
		s.finished = true
	} else {
		s.sent += uint64(size)
		s.pending = append(s.pending, pending{seq: seq, end: s.sent})
	}
	return seq, offset, nil
}

// Run has the sole ownership of outbound media writes. Reads stop at the
// 4 MiB window; cancellation closes src to interrupt blocking io.ReadCloser.
// write must be a single-writer callback and honor ctx.
func (s *Sender) Run(ctx context.Context, src io.ReadCloser, write func(context.Context, []byte) error) error {
	if src == nil || write == nil || s.id.RoomID == "" || s.id.PlaybackID == "" || s.id.Generation == 0 || s.id.Epoch == 0 {
		return ErrFrame
	}
	var closeOnce sync.Once
	closeSource := func() { closeOnce.Do(func() { _ = src.Close() }) }
	defer closeSource()
	readDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeSource()
		case <-readDone:
		}
	}()
	defer close(readDone)
	buf := make([]byte, MaxPayloadBytes)
	emptyReads := 0
	for {
		capacity, err := s.waitCredit(ctx)
		if err != nil {
			return err
		}
		n, readErr := src.Read(buf[:capacity])
		if err := ctx.Err(); err != nil {
			return err
		}
		if n < 0 || n > capacity {
			return ErrFrame
		}
		if n > 0 {
			emptyReads = 0
			seq, offset, e := s.record(n, false)
			if e != nil {
				return e
			}
			frame, e := (Frame{Identity: s.id, Sequence: seq, Offset: offset, Payload: buf[:n]}).MarshalBinary()
			if e != nil {
				return e
			}
			if e = write(ctx, frame); e != nil {
				return e
			}
		} else if readErr == nil {
			emptyReads++
			if emptyReads > 100 {
				return io.ErrNoProgress
			}
		}
		if errors.Is(readErr, io.EOF) {
			if err := ctx.Err(); err != nil {
				return err
			}
			seq, offset, e := s.record(0, true)
			if e != nil {
				return e
			}
			frame, e := (Frame{Identity: s.id, Sequence: seq, Offset: offset, EOF: true}).MarshalBinary()
			if e != nil {
				return e
			}
			return write(ctx, frame)
		}
		if readErr != nil {
			return readErr
		}
	}
}

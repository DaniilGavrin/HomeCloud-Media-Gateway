package mediaframe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func testID() Identity {
	return Identity{RoomID: "room-test", PlaybackID: "play-123", Generation: 7, Epoch: 3}
}

func TestFrameLayoutAndFencing(t *testing.T) {
	id := testID()
	f := Frame{Identity: id, Sequence: 42, Offset: 998, Payload: []byte("hello")}
	raw, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != HeaderSize+5 || string(raw[:4]) != "HCM1" || binary.BigEndian.Uint16(raw[6:8]) != HeaderSize {
		t.Fatal("wrong binary header")
	}
	got, err := Parse(raw, id)
	if err != nil || got.Sequence != 42 || got.Offset != 998 || string(got.Payload) != "hello" {
		t.Fatalf("%+v %v", got, err)
	}
	for _, different := range []Identity{
		{RoomID: id.RoomID, PlaybackID: "other", Generation: id.Generation, Epoch: id.Epoch},
		{RoomID: id.RoomID, PlaybackID: id.PlaybackID, Generation: id.Generation + 1, Epoch: id.Epoch},
		{RoomID: id.RoomID, PlaybackID: id.PlaybackID, Generation: id.Generation, Epoch: id.Epoch + 1},
	} {
		if _, err := Parse(raw, different); !errors.Is(err, ErrFrame) {
			t.Fatal("stale frame accepted")
		}
	}
	invalid := append([]byte(nil), raw...)
	invalid[5] = 0x80
	if _, err := Parse(invalid, id); err == nil {
		t.Fatal("unknown flag accepted")
	}
	invalid = append([]byte(nil), raw...)
	invalid[36] = 1
	if _, err := Parse(invalid, id); err == nil {
		t.Fatal("bad length accepted")
	}
	eof, err := (Frame{Identity: id, Sequence: 43, Offset: 1003, EOF: true}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(eof, id)
	if err != nil || !parsed.EOF || len(parsed.Payload) != 0 {
		t.Fatal("EOF not parsed", err)
	}
	if _, err := (Frame{Identity: id, EOF: true, Payload: []byte{1}}).MarshalBinary(); err == nil {
		t.Fatal("EOF with data")
	}
}

type virtualReader struct {
	remaining uint64
	reads     atomic.Uint64
	closed    atomic.Bool
}

func (r *virtualReader) Read(b []byte) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(b)
	if uint64(n) > r.remaining {
		n = int(r.remaining)
	}
	// Every iteration is synthesized, no file and no 10 GiB allocation.
	r.remaining -= uint64(n)
	r.reads.Add(1)
	return n, nil
}
func (r *virtualReader) Close() error { r.closed.Store(true); return nil }

func TestStreamOverTenGiBBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("10GiB transport stress test")
	}
	const total = uint64(10<<30) + 17
	id := testID()
	reader := &virtualReader{remaining: total}
	sender := NewSender(id)
	var received, expectedSeq uint64
	var eof bool
	err := sender.Run(context.Background(), reader, func(_ context.Context, raw []byte) error {
		f, e := Parse(raw, id)
		if e != nil {
			return e
		}
		if f.Sequence != expectedSeq || f.Offset != received {
			t.Fatalf("out of order frame seq=%d offset=%d want=%d,%d", f.Sequence, f.Offset, expectedSeq, received)
		}
		expectedSeq++
		if f.EOF {
			eof = true
			return nil
		}
		received += uint64(len(f.Payload))
		if sender.InFlight() > WindowBytes {
			t.Fatalf("window exceeded: %d", sender.InFlight())
		}
		return sender.Acknowledge(Ack{V: 1, Type: "media.ack", RoomID: id.RoomID, PlaybackID: id.PlaybackID, Generation: id.Generation, Epoch: id.Epoch, Sequence: f.Sequence, ReceivedBytes: received})
	})
	if err != nil || !eof || received != total || sender.InFlight() != 0 || !reader.closed.Load() {
		t.Fatalf("err=%v eof=%v bytes=%d inflight=%d closed=%v", err, eof, received, sender.InFlight(), reader.closed.Load())
	}
}

func TestFlowControlSlowReceiverAndACKValidation(t *testing.T) {
	id := testID()
	reader := &virtualReader{remaining: 10 << 30}
	sender := NewSender(id)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	written := make(chan Frame, 256)
	result := make(chan error, 1)
	go func() {
		result <- sender.Run(ctx, reader, func(_ context.Context, raw []byte) error {
			f, err := Parse(raw, id)
			if err == nil {
				written <- f
			}
			return err
		})
	}()
	var first Frame
	for i := 0; i < WindowBytes/MaxPayloadBytes; i++ {
		select {
		case f := <-written:
			if i == 0 {
				first = f
			}
		case <-time.After(5 * time.Second):
			t.Fatal("stalled before window filled")
		}
	}
	if sender.InFlight() != WindowBytes {
		t.Fatalf("window is %d", sender.InFlight())
	}
	select {
	case <-written:
		t.Fatal("sent data beyond unacknowledged window")
	case <-time.After(100 * time.Millisecond):
	}
	wrong := Ack{V: 1, Type: "media.ack", RoomID: id.RoomID, PlaybackID: id.PlaybackID, Generation: id.Generation, Epoch: id.Epoch, Sequence: first.Sequence, ReceivedBytes: uint64(len(first.Payload))}
	wrong.Epoch++
	if sender.Acknowledge(wrong) == nil {
		t.Fatal("wrong epoch accepted")
	}
	wrong.Epoch = id.Epoch
	wrong.ReceivedBytes = 1
	if sender.Acknowledge(wrong) == nil {
		t.Fatal("unearned credit accepted")
	}
	wrong.ReceivedBytes = uint64(len(first.Payload))
	if err := sender.Acknowledge(wrong); err != nil {
		t.Fatal(err)
	}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("ACK failed to release sender")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not stop")
	}
	if !reader.closed.Load() {
		t.Fatal("source not closed")
	}
}

func TestCanceledBlockedReader(t *testing.T) {
	in, out := io.Pipe()
	defer out.Close()
	sender := NewSender(testID())
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- sender.Run(ctx, in, func(context.Context, []byte) error { return nil }) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read blocked after STOP")
	}
}

func TestRejectDuplicateAndOldACK(t *testing.T) {
	s := NewSender(testID())
	seq, offset, err := s.record(8, false)
	if err != nil || seq != 0 || offset != 0 {
		t.Fatal("record", err)
	}
	ack := Ack{V: 1, Type: "media.ack", RoomID: s.id.RoomID, PlaybackID: s.id.PlaybackID, Generation: s.id.Generation, Epoch: s.id.Epoch, Sequence: 0, ReceivedBytes: 8}
	if err := s.Acknowledge(ack); err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(ack); err != nil {
		t.Fatal("identical duplicate should be idempotent", err)
	}
	if _, _, err := s.record(4, false); err != nil {
		t.Fatal(err)
	}
	ack.Sequence = 1
	ack.ReceivedBytes = 12
	if err := s.Acknowledge(ack); err != nil {
		t.Fatal(err)
	}
	ack.Sequence = 0
	ack.ReceivedBytes = 8
	if err := s.Acknowledge(ack); err == nil {
		t.Fatal("old ACK accepted after progress")
	}
	s.sequence = math.MaxUint64
	if _, _, err := s.record(4, false); err == nil {
		t.Fatal("seq overflow accepted")
	}
}

func TestFrameBoundaries(t *testing.T) {
	id := testID()
	if _, err := (Frame{Identity: id, Payload: make([]byte, MaxPayloadBytes+1)}).MarshalBinary(); err == nil {
		t.Fatal("oversized payload")
	}
	raw, _ := (Frame{Identity: id, Payload: bytes.Repeat([]byte{3}, MaxPayloadBytes)}).MarshalBinary()
	if len(raw) != HeaderSize+MaxPayloadBytes {
		t.Fatal("size mismatch")
	}
	if _, err := Parse(raw, id); err != nil {
		t.Fatal(err)
	}
}

// Even with a pathological source returning one byte per Read, the queue is
// bounded by frame count as well as byte count.
type tinyReader struct {
	reads  atomic.Uint64
	closed atomic.Bool
}

func (r *tinyReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if len(p) == 0 {
		return 0, io.ErrShortBuffer
	}
	p[0] = 7
	return 1, nil
}
func (r *tinyReader) Close() error { r.closed.Store(true); return nil }
func TestTinyReaderCannotGrowQueue(t *testing.T) {
	id := testID()
	src := &tinyReader{}
	sender := NewSender(id)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sender.Run(ctx, src, func(context.Context, []byte) error { return nil }) }()
	deadline := time.Now().Add(3 * time.Second)
	for src.reads.Load() < MaxPendingFrames && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := src.reads.Load(); n != MaxPendingFrames {
		t.Fatalf("unbounded read ahead: %d", n)
	}
	sender.mu.Lock()
	pending := len(sender.pending)
	sender.mu.Unlock()
	if pending != MaxPendingFrames {
		t.Fatalf("pending records=%d", pending)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop blocked")
	}
	if !src.closed.Load() {
		t.Fatal("source left open")
	}
}

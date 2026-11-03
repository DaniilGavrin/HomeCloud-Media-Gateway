package mediaframe

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestPausePreservesStreamAndResumesSamePlayback(t *testing.T) {
	id := Identity{RoomID: "r", PlaybackID: "p", Generation: 1, Epoch: 1}
	s := NewSender(id)
	if !s.SetPaused(true) {
		t.Fatal("could not pause")
	}
	writes := make(chan Frame, 5)
	done := make(chan error, 1)
	go func() {
		done <- s.Run(context.Background(), io.NopCloser(bytes.NewReader([]byte("original"))), func(_ context.Context, raw []byte) error {
			f, e := Parse(raw, id)
			if e == nil {
				writes <- f
			}
			return e
		})
	}()
	select {
	case f := <-writes:
		t.Fatalf("wrote while paused %+v", f)
	case <-time.After(50 * time.Millisecond):
	}
	if !s.SetPaused(false) {
		t.Fatal("resume failed")
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sender blocked after resume")
	}
	if len(writes) != 2 {
		t.Fatalf("expected payload and EOF, got %d", len(writes))
	}
	if s.SetPaused(true) {
		t.Fatal("EOF sender can be resumed")
	}
}

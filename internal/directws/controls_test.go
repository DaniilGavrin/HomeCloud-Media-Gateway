package directws

import (
	"errors"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"testing"
)

func TestStreamControlFencesAndPauseResume(t *testing.T) {
	c := NewStreamControl()
	done := make(chan struct{})
	lease := Lease{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 4, Done: done}
	c.Register(lease, mediaframe.NewSender(mediaframe.Identity{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 4}))
	q := PlaybackControl{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 4, CommandID: "one", CommandSequence: 1, Action: "pause"}
	if e := c.Apply(q); e != nil {
		t.Fatal(e)
	}
	if e := c.Apply(q); e != nil {
		t.Fatal("idempotency", e)
	}
	other := q
	other.CommandSequence = 2
	other.CommandID = "two"
	other.Action = "resume"
	if e := c.Apply(other); e != nil {
		t.Fatal(e)
	}
	if e := c.Apply(q); !errors.Is(e, ErrStaleControl) {
		t.Fatal("replay", e)
	}
	wrong := other
	wrong.Generation = 3
	wrong.CommandSequence = 3
	if e := c.Apply(wrong); !errors.Is(e, ErrStaleControl) {
		t.Fatal("generation", e)
	}
	c.Unregister(Lease{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 3})
	if e := c.Apply(PlaybackControl{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 4, CommandID: "three", CommandSequence: 3, Action: "pause"}); e != nil {
		t.Fatal(e)
	}
	c.Unregister(lease)
	if e := c.Apply(q); !errors.Is(e, ErrNoStream) {
		t.Fatal(e)
	}
}

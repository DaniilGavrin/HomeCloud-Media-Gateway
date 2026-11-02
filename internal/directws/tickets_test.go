package directws

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func grant() Grant {
	return Grant{RoomID: "room-x", PlaybackID: "play-1", DeviceID: "tv-1", Generation: 1, Ticket: strings.Repeat("ab", 32), ExpiresAt: time.Now().Add(time.Minute)}
}
func hello(g Grant) Hello {
	return Hello{V: 1, Type: "media.hello", RoomID: g.RoomID, PlaybackID: g.PlaybackID, DeviceID: g.DeviceID, Generation: g.Generation, AttachTicket: g.Ticket}
}
func TestOneUseAndEpoch(t *testing.T) {
	b := New(2)
	g := grant()
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	l, e := b.Claim(hello(g))
	if e != nil || l.Epoch != 1 {
		t.Fatalf("lease %+v %v", l, e)
	}
	if _, e = b.Claim(hello(g)); !errors.Is(e, ErrRejected) {
		t.Fatal("ticket replay", e)
	}
	g.Ticket = strings.Repeat("cd", 32)
	g.ExpiresAt = g.ExpiresAt.Add(time.Millisecond)
	if e = b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	l2, e := b.Claim(hello(g))
	if e != nil || l2.Epoch != 2 {
		t.Fatalf("epoch %+v %v", l2, e)
	}
	select {
	case <-l.Done:
	default:
		t.Fatal("old connection not revoked")
	}
	if b.Release(l) {
		t.Fatal("stale socket released replacement")
	}
	if !b.Valid(g.RoomID, l2) {
		t.Fatal("new lease lost")
	}
	b.Remove(g.RoomID)
	select {
	case <-l2.Done:
	default:
		t.Fatal("room stop did not revoke")
	}
}
func TestExpiryAndIdentity(t *testing.T) {
	b := New(2)
	g := grant()
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	h := hello(g)
	h.DeviceID = "other"
	if _, e := b.Claim(h); !errors.Is(e, ErrRejected) {
		t.Fatal(e)
	}
	h = hello(g)
	h.AttachTicket = "wrong"
	if _, e := b.Claim(h); !errors.Is(e, ErrRejected) {
		t.Fatal(e)
	}
	b.now = func() time.Time { return g.ExpiresAt.Add(time.Second) }
	if _, e := b.Claim(hello(g)); !errors.Is(e, ErrRejected) {
		t.Fatal(e)
	}
}
func TestSwitchMakesOldGenerationStale(t *testing.T) {
	b := New(1)
	g := grant()
	_ = b.Prepare(g)
	l, e := b.Claim(hello(g))
	if e != nil {
		t.Fatal(e)
	}
	g.PlaybackID = "play-2"
	g.Generation = 2
	g.Ticket = strings.Repeat("ef", 32)
	if e = b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	select {
	case <-l.Done:
	default:
		t.Fatal("old worker still active")
	}
	h := hello(g)
	h.Generation = 1
	if _, e = b.Claim(h); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if _, e = b.Claim(hello(g)); e != nil {
		t.Fatal(e)
	}
}

func TestDuplicateDeliveryCannotReplayConsumedToken(t *testing.T) {
	b := New(1)
	g := grant()
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Claim(hello(g)); e != nil {
		t.Fatal(e)
	}
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Claim(hello(g)); !errors.Is(e, ErrRejected) {
		t.Fatalf("duplicate rearmed spent ticket: %v", e)
	}
	old := g
	g.Ticket = strings.Repeat("34", 32)
	g.ExpiresAt = g.ExpiresAt.Add(time.Second)
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	if e := b.Prepare(old); !errors.Is(e, ErrStale) {
		t.Fatalf("old ticket after refresh: %v", e)
	}
}

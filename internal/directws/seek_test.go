package directws

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestSeekLeaseRevokesOldGenerationAndRetainsTime(t *testing.T) {
	b := New(1)
	g := grant()
	if err := b.Prepare(g); err != nil {
		t.Fatal(err)
	}
	old, err := b.Claim(hello(g))
	if err != nil {
		t.Fatal(err)
	}
	target := int64(120500)
	next := g
	next.Generation++
	next.Ticket = "0123456789abcdef0123456789abcdef"
	next.ExpiresAt = time.Now().Add(time.Minute)
	next.SeekPositionMS = &target
	if next.Ticket == g.Ticket {
		next.Ticket = "fedcba9876543210fedcba9876543210"
	}
	if err = b.Prepare(next); err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.Done:
	default:
		t.Fatal("old socket not revoked")
	}
	latest, err := b.Claim(hello(next))
	if err != nil {
		t.Fatal(err)
	}
	if latest.Generation != next.Generation || latest.SeekPositionMS == nil || *latest.SeekPositionMS != target || latest.Epoch <= old.Epoch {
		t.Fatalf("bad seek lease %+v", latest)
	}
	if !validSeekResolution(latest, 115000) || validSeekResolution(latest, -1) {
		t.Fatal("invalid timeline resolution validation")
	}
	if _, err = b.Claim(hello(g)); !errors.Is(err, ErrStale) {
		t.Fatalf("old ticket reused %v", err)
	}
	b.Remove(g.RoomID)
	select {
	case <-latest.Done:
	default:
		t.Fatal("seek socket not revoked")
	}
}
func TestSeekOpenerInterfaceCanReturnActualKeyframe(t *testing.T) {
	target := int64(60000)
	l := Lease{RoomID: "room", PlaybackID: "p", Generation: 2, Epoch: 4, SeekPositionMS: &target}
	open := SeekStreamOpener(func(_ context.Context, got Lease) (io.ReadCloser, int64, error) {
		if got.SeekPositionMS == nil || *got.SeekPositionMS != 60000 {
			t.Fatal("lost timeline target")
		}
		return io.NopCloser(&zeroReader{}), 58500, nil
	})
	src, actual, err := open(context.Background(), l)
	if err != nil || !validSeekResolution(l, actual) {
		t.Fatal(err)
	}
	_ = src.Close()
}

type zeroReader struct{}

func (*zeroReader) Read([]byte) (int, error) { return 0, io.EOF }

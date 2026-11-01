package rooms

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var tv = Target{NodeID: "node-main", DeviceID: "living-tv"}
var film1 = Source{StorageNodeID: "storage-1", FileID: "film-1"}
var film2 = Source{StorageNodeID: "storage-1", FileID: "film-2"}

func manager(t *testing.T, limit int) *Manager {
	t.Helper()
	m, err := New(limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}
func activeRoom(t *testing.T, m *Manager) (Snapshot, context.Context) {
	t.Helper()
	s, ctx, err := m.Create(tv)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.RoomID) != len("room-")+32 || !strings.HasPrefix(s.RoomID, "room-") {
		t.Fatalf("room_id not 128 bit random identifier: %s", s.RoomID)
	}
	if s.State != Creating || s.PlaybackState != Idle || s.Revision != 1 {
		t.Fatalf("initial: %+v", s)
	}
	s, err = m.AdvanceRoom(s.RoomID, Connecting)
	if err != nil {
		t.Fatal(err)
	}
	s, err = m.AdvanceRoom(s.RoomID, Active)
	if err != nil {
		t.Fatal(err)
	}
	return s, ctx
}
func startedFilm(t *testing.T, m *Manager, id string) (Snapshot, Token, context.Context) {
	t.Helper()
	s, tok, ctx, err := m.StartPlayback(id, film1)
	if err != nil {
		t.Fatal(err)
	}
	return s, tok, ctx
}

func TestNewValidation(t *testing.T) {
	for _, i := range []int{-1, 0, 129} {
		if _, err := New(i); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("limit %d err=%v", i, err)
		}
	}
	m := manager(t, 4)
	if m.Count() != 0 || len(m.List()) != 0 {
		t.Fatal("unexpected rooms")
	}
}
func TestCreateTransitionAndTargetExclusivity(t *testing.T) {
	m := manager(t, 2)
	s, _, err := m.Create(Target{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	s, _, err = m.Create(tv)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Create(tv); !errors.Is(err, ErrRoomConflict) {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Active); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Connecting); err != nil {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Connecting); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Active); err != nil {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Creating); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	_, _, err = m.Create(Target{NodeID: "another", DeviceID: tv.DeviceID})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = m.Create(Target{NodeID: "another", DeviceID: "another"})
	if !errors.Is(err, ErrRoomLimit) {
		t.Fatal(err)
	}
	if len(m.List()) != 2 {
		t.Fatal("wrong room count")
	}
}
func TestSwitchEOFStopIndependentLifecycle(t *testing.T) {
	m := manager(t, 1)
	original, roomCtx := activeRoom(t, m)
	a, tok1, worker1 := startedFilm(t, m, original.RoomID)
	if a.RoomID != original.RoomID || a.Generation != 1 || a.PlaybackState != Preparing || a.Revision != original.Revision+1 {
		t.Fatalf("start: %+v", a)
	}
	if len(tok1.PlaybackID) != len("play-")+32 {
		t.Fatalf("playback id %q", tok1.PlaybackID)
	}
	if _, err := m.MovePlayback(tok1, Buffering); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MovePlayback(tok1, Playing); err != nil {
		t.Fatal(err)
	}
	eof, err := m.EOF(tok1)
	if err != nil {
		t.Fatal(err)
	}
	if eof.State != Active || eof.PlaybackState != Ended || eof.PlaybackID != tok1.PlaybackID {
		t.Fatalf("EOF killed room: %+v", eof)
	}
	select {
	case <-worker1.Done():
	default:
		t.Fatal("EOF did not cancel decoder/source work")
	}
	select {
	case <-roomCtx.Done():
		t.Fatal("EOF closed room")
	default:
	}
	if _, err = m.MovePlayback(tok1, Idle); err != nil {
		t.Fatal(err)
	}
	next, tok2, worker2, err := m.StartPlayback(original.RoomID, film2)
	if err != nil {
		t.Fatal(err)
	}
	if next.RoomID != original.RoomID || tok2.PlaybackID == tok1.PlaybackID || tok2.Generation != 2 || next.Source.FileID != "film-2" {
		t.Fatalf("switch: %+v %+v", next, tok2)
	}
	if _, err = m.EOF(tok1); !errors.Is(err, ErrStalePlayback) {
		t.Fatalf("old EOF: %v", err)
	}
	if _, err = m.MovePlayback(tok1, Playing); !errors.Is(err, ErrStalePlayback) {
		t.Fatal(err)
	}
	closed, err := m.Stop(original.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.State != Closed || closed.CloseReason != ReasonStop || m.Count() != 0 {
		t.Fatalf("stop: %+v", closed)
	}
	select {
	case <-worker2.Done():
	default:
		t.Fatal("STOP did not cancel playback")
	}
	select {
	case <-roomCtx.Done():
	default:
		t.Fatal("STOP did not cancel room")
	}
	again, err := m.Stop(original.RoomID)
	if err != nil || !reflect.DeepEqual(again, closed) {
		t.Fatalf("repeat stop %+v %v", again, err)
	}
	if _, err = m.Get(original.RoomID); !errors.Is(err, ErrRoomNotFound) {
		t.Fatal(err)
	}
	if _, _, err = m.Create(tv); err != nil {
		t.Fatalf("TV was not released: %v", err)
	}
}

func TestSeekContextAndGenerationFence(t *testing.T) {
	m := manager(t, 1)
	r, _ := activeRoom(t, m)
	_, old, oldCtx := startedFilm(t, m, r.RoomID)
	if _, err := m.MovePlayback(old, Buffering); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MovePlayback(old, Playing); err != nil {
		t.Fatal(err)
	}
	state, next, newCtx, err := m.Seek(old, 75000)
	if err != nil {
		t.Fatal(err)
	}
	if next.PlaybackID != old.PlaybackID || next.Generation != old.Generation+1 || state.PlaybackState != Preparing || state.PositionMS != 75000 {
		t.Fatalf("seek %+v", state)
	}
	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("seek did not cancel old generation")
	}
	if !m.IsCurrent(next) || m.IsCurrent(old) {
		t.Fatal("current token fence violated")
	}
	if _, err = m.EOF(old); !errors.Is(err, ErrStalePlayback) {
		t.Fatal(err)
	}
	if _, _, _, err = m.Seek(old, 80000); !errors.Is(err, ErrStalePlayback) {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(next, Buffering); err != nil {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(next, Playing); err != nil {
		t.Fatal(err)
	}
	if _, err = m.EOF(next); err != nil {
		t.Fatal(err)
	}
	if m.IsCurrent(next) {
		t.Fatal("ended token still active")
	}
	select {
	case <-newCtx.Done():
	default:
		t.Fatal("EOF does not release worker")
	}
}
func TestRejectInvalidTransitionsAndInputs(t *testing.T) {
	m := manager(t, 1)
	s, _, err := m.Create(tv)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = m.StartPlayback(s.RoomID, film1); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, _, _, err = m.StartPlayback(s.RoomID, Source{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = m.EOF(Token{RoomID: s.RoomID}); !errors.Is(err, ErrStalePlayback) {
		t.Fatal(err)
	}
	if _, err = m.Stop("missing"); !errors.Is(err, ErrRoomNotFound) {
		t.Fatal(err)
	}
	if _, err = m.Fail(s.RoomID, ReasonStop); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Connecting); err != nil {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(s.RoomID, Active); err != nil {
		t.Fatal(err)
	}
	_, tok, _, err := m.StartPlayback(s.RoomID, film1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(tok, Playing); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, _, _, err = m.Seek(tok, -1); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(tok, Buffering); err != nil {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(tok, Playing); err != nil {
		t.Fatal(err)
	}
	if _, err = m.MovePlayback(tok, Playing); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, err = m.Fail(s.RoomID, ReasonLost); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotDetachedFromInternalSource(t *testing.T) {
	m := manager(t, 1)
	s, _ := activeRoom(t, m)
	snap, _, _, err := m.StartPlayback(s.RoomID, film1)
	if err != nil {
		t.Fatal(err)
	}
	snap.Source.FileID = "fake-change"
	got, err := m.Get(s.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.FileID != "film-1" {
		t.Fatal("snapshot escaped manager mutex")
	}
}
func TestCloseIdempotentAndContextCancellation(t *testing.T) {
	m := manager(t, 1)
	s, roomCtx := activeRoom(t, m)
	_, _, workerCtx := startedFilm(t, m, s.RoomID)
	m.Close()
	m.Close()
	select {
	case <-roomCtx.Done():
	default:
		t.Fatal("room did not stop")
	}
	select {
	case <-workerCtx.Done():
	default:
		t.Fatal("playback did not stop")
	}
	if _, _, err := m.Create(tv); !errors.Is(err, ErrManagerClosed) {
		t.Fatal(err)
	}
	closed, err := m.Stop(s.RoomID)
	if err != nil || closed.CloseReason != ReasonShutdown {
		t.Fatalf("%+v, %v", closed, err)
	}
}
func TestConcurrentStopAndPlaybackCallbacks(t *testing.T) {
	m := manager(t, 1)
	s, _ := activeRoom(t, m)
	_, tok, _ := startedFilm(t, m, s.RoomID)
	var wg sync.WaitGroup
	errCh := make(chan error, 128)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.MovePlayback(tok, Buffering)
			if err != nil && !errors.Is(err, ErrInvalidTransition) && !errors.Is(err, ErrRoomNotFound) && !errors.Is(err, ErrStalePlayback) {
				errCh <- err
			}
		}()
	}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Stop(s.RoomID); err != nil {
				errCh <- err
			}
		}()
	}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Get(s.RoomID)
			if err != nil && !errors.Is(err, ErrRoomNotFound) {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
	if m.Count() != 0 {
		t.Fatal("room leaked")
	}
}
func TestConcurrentCreationAtLimit(t *testing.T) {
	const limit = 4
	m := manager(t, limit)
	var wg sync.WaitGroup
	success := make(chan string, 64)
	for i := 0; i < 64; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _, err := m.Create(Target{NodeID: "node-main", DeviceID: fmt.Sprintf("tv-%d", i)})
			if err == nil {
				success <- s.RoomID
			} else if !errors.Is(err, ErrRoomLimit) {
				t.Errorf("unexpected %v", err)
			}
		}()
	}
	wg.Wait()
	close(success)
	if len(success) != limit || m.Count() != limit {
		t.Fatalf("limit exceeded or under-filled: %d", m.Count())
	}
}
func TestBoundedStopTombstones(t *testing.T) {
	m := manager(t, 1)
	var first, last string
	for i := 0; i < closedHistoryLimit+1; i++ {
		s, _, err := m.Create(tv)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = s.RoomID
		}
		last = s.RoomID
		if _, err = m.Stop(s.RoomID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Stop(first); !errors.Is(err, ErrRoomNotFound) {
		t.Fatalf("old tombstone should be bounded: %v", err)
	}
	if _, err := m.Stop(last); err != nil {
		t.Fatal(err)
	}
	if len(m.closed) != closedHistoryLimit {
		t.Fatal("tombstone memory unbounded")
	}
}
func TestPlaybackContextCancellationIsPrompt(t *testing.T) {
	m := manager(t, 1)
	s, _ := activeRoom(t, m)
	_, tok, ctx := startedFilm(t, m, s.RoomID)
	if _, err := m.MovePlayback(tok, PlaybackError); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancel not promptly delivered")
	}
}

func TestBackendAssignedIdentifiers(t *testing.T) {
	m := manager(t, 2)
	roomID := "room-3e08d8f177a513a1e43705c925345423"
	s, _, err := m.CreateAssigned(roomID, tv)
	if err != nil || s.RoomID != roomID {
		t.Fatalf("assigned room: %+v %v", s, err)
	}
	if _, _, err = m.CreateAssigned(roomID, Target{NodeID: "n2", DeviceID: "tv2"}); !errors.Is(err, ErrRoomConflict) {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(roomID, Connecting); err != nil {
		t.Fatal(err)
	}
	if _, err = m.AdvanceRoom(roomID, Active); err != nil {
		t.Fatal(err)
	}
	_, token, ctx, err := m.StartAssignedPlayback(roomID, "play-from-backend-1", film1)
	if err != nil || token.PlaybackID != "play-from-backend-1" {
		t.Fatalf("assigned playback: %+v %v", token, err)
	}
	if _, _, _, err = m.StartAssignedPlayback(roomID, "play-from-backend-1", film2); !errors.Is(err, ErrRoomConflict) {
		t.Fatal(err)
	}
	_, token2, _, err := m.StartAssignedPlayback(roomID, "play-from-backend-2", film2)
	if err != nil || token2.Generation != token.Generation+1 {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("assigned playback did not cancel old work")
	}
	if _, err = m.EOF(token); !errors.Is(err, ErrStalePlayback) {
		t.Fatal(err)
	}
	if _, _, _, err = m.StartAssignedPlayback(roomID, "play-from-backend-1", film1); !errors.Is(err, ErrRoomConflict) {
		t.Fatalf("old playback_id must never be reused within a room: %v", err)
	}
	if _, err = m.Stop(roomID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.CreateAssigned(roomID, tv); !errors.Is(err, ErrRoomConflict) {
		t.Fatal("room ID reused")
	}
}
func TestGenerationOverflowDoesNotCancelOldWorker(t *testing.T) {
	m := manager(t, 1)
	s, _ := activeRoom(t, m)
	_, token, ctx := startedFilm(t, m, s.RoomID)
	if _, err := m.MovePlayback(token, Buffering); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MovePlayback(token, Playing); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.rooms[s.RoomID].snapshot.Generation = ^uint32(0)
	m.mu.Unlock()
	_, _, _, err := m.StartPlayback(s.RoomID, film2)
	if !errors.Is(err, ErrGenerationOverflow) {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("failed operation canceled current worker")
	default:
	}
}

// Package rooms implements the local, in-memory lifecycle of Gateway rooms and
// their independent playback. The main HC WS Backend remains authoritative;
// this package never sends control messages, opens sockets, or streams bytes.
package rooms

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

type RoomState string

const (
	Creating   RoomState = "CREATING"
	Connecting RoomState = "CONNECTING"
	Active     RoomState = "ACTIVE"
	Lost       RoomState = "LOST"
	Closing    RoomState = "CLOSING"
	Closed     RoomState = "CLOSED"
)

type PlaybackState string

const (
	Idle          PlaybackState = "IDLE"
	Preparing     PlaybackState = "PREPARING"
	Buffering     PlaybackState = "BUFFERING"
	Playing       PlaybackState = "PLAYING"
	Paused        PlaybackState = "PAUSED"
	Ended         PlaybackState = "ENDED"
	PlaybackError PlaybackState = "ERROR"
)

type CloseReason string

const (
	ReasonStop     CloseReason = "STOP"
	ReasonLost     CloseReason = "LOST"
	ReasonError    CloseReason = "ERROR"
	ReasonShutdown CloseReason = "SHUTDOWN"
)

var (
	ErrRoomNotFound       = errors.New("ROOM_NOT_FOUND")
	ErrRoomConflict       = errors.New("ROOM_CONFLICT")
	ErrRoomLimit          = errors.New("ROOM_LIMIT")
	ErrInvalidInput       = errors.New("INVALID_REQUEST")
	ErrInvalidTransition  = errors.New("INVALID_TRANSITION")
	ErrStalePlayback      = errors.New("STALE_PLAYBACK")
	ErrManagerClosed      = errors.New("MANAGER_CLOSED")
	ErrGenerationOverflow = errors.New("GENERATION_OVERFLOW")
)

type Target struct {
	NodeID   string `json:"node_id"`
	DeviceID string `json:"device_id"`
}

type Source struct {
	StorageNodeID string `json:"storage_node_id"`
	FileID        string `json:"file_id"`
}

// Token is the local callback fence. All asynchronous worker results MUST
// carry the token they were created for. Checking only room_id is unsafe.
type Token struct {
	RoomID     string `json:"room_id"`
	PlaybackID string `json:"playback_id"`
	Generation uint32 `json:"generation"`
}

// Snapshot is a detached immutable-by-value view. Contexts and cancel funcs
// are deliberately never exposed in snapshots or JSON.
type Snapshot struct {
	RoomID        string        `json:"room_id"`
	Target        Target        `json:"target"`
	State         RoomState     `json:"state"`
	PlaybackID    string        `json:"playback_id,omitempty"`
	PlaybackState PlaybackState `json:"playback_state"`
	Source        *Source       `json:"source,omitempty"`
	Generation    uint32        `json:"generation"`
	Revision      uint64        `json:"revision"`
	PositionMS    int64         `json:"position_ms"`
	CloseReason   CloseReason   `json:"close_reason,omitempty"`
}

type room struct {
	snapshot        Snapshot
	usedPlaybackIDs map[string]struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	playbackCtx     context.Context
	playbackCancel  context.CancelFunc
}

// Manager uses one mutex for target exclusivity, room mutations and snapshots.
// This makes STOP, EOF, seek, switch and callback validation atomic. The limit
// is <=128 rooms; serializing short in-memory operations is intentional.
type Manager struct {
	mu          sync.RWMutex
	rooms       map[string]*room
	targets     map[Target]string
	closed      map[string]Snapshot
	closedOrder []string
	maxRooms    int
	ctx         context.Context
	cancel      context.CancelFunc
	stopped     bool
}

const closedHistoryLimit = 256

func New(maxRooms int) (*Manager, error) {
	if maxRooms < 1 || maxRooms > 128 {
		return nil, fmt.Errorf("%w: max_rooms must be within 1..128", ErrInvalidInput)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{rooms: make(map[string]*room), targets: make(map[Target]string),
		closed: make(map[string]Snapshot), maxRooms: maxRooms, ctx: ctx, cancel: cancel}, nil
}

// makeID uses OS cryptographic randomness rather than predictable sequence IDs.
func makeID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}
	return prefix + hex.EncodeToString(bytes[:]), nil
}

func validPart(s string) bool   { return s != "" && strings.TrimSpace(s) == s && len(s) <= 256 }
func validTarget(t Target) bool { return validPart(t.NodeID) && validPart(t.DeviceID) }
func validSource(s Source) bool { return validPart(s.StorageNodeID) && validPart(s.FileID) }

// Create is a convenience for local/internal testing. The authoritative HC
// Backend should use CreateAssigned with its own room_id (KAN-47).
func (m *Manager) Create(target Target) (Snapshot, context.Context, error) {
	if !validTarget(target) {
		return Snapshot{}, nil, ErrInvalidInput
	}
	id, err := makeID("room-")
	if err != nil {
		return Snapshot{}, nil, err
	}
	return m.CreateAssigned(id, target)
}

// CreateAssigned mirrors an identity allocated by the authoritative Backend.
// Caller authentication/authorization belongs to the control plane, not here.
func (m *Manager) CreateAssigned(id string, target Target) (Snapshot, context.Context, error) {
	if !validPart(id) || !validTarget(target) {
		return Snapshot{}, nil, ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return Snapshot{}, nil, ErrManagerClosed
	}
	if _, exists := m.rooms[id]; exists {
		return Snapshot{}, nil, ErrRoomConflict
	}
	if _, exists := m.closed[id]; exists {
		return Snapshot{}, nil, ErrRoomConflict
	}
	if _, occupied := m.targets[target]; occupied {
		return Snapshot{}, nil, ErrRoomConflict
	}
	if len(m.rooms) >= m.maxRooms {
		return Snapshot{}, nil, ErrRoomLimit
	}
	ctx, cancel := context.WithCancel(m.ctx)
	entry := &room{snapshot: Snapshot{RoomID: id, Target: target, State: Creating,
		PlaybackState: Idle, Revision: 1}, ctx: ctx, cancel: cancel, usedPlaybackIDs: make(map[string]struct{})}
	m.rooms[id] = entry
	m.targets[target] = id
	return copySnapshot(entry.snapshot), ctx, nil
}

func (m *Manager) AdvanceRoom(id string, next RoomState) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, ErrRoomNotFound
	}
	current := r.snapshot.State
	if (current != Creating || next != Connecting) && (current != Connecting || next != Active) {
		return Snapshot{}, ErrInvalidTransition
	}
	r.snapshot.State = next
	r.snapshot.Revision++
	return copySnapshot(r.snapshot), nil
}

// StartPlayback keeps room_id and TV, but replaces the file and playback_id.
// Prior playback context is canceled only after new ID generation succeeds.
func (m *Manager) StartPlayback(id string, source Source) (Snapshot, Token, context.Context, error) {
	if !validSource(source) {
		return Snapshot{}, Token{}, nil, ErrInvalidInput
	}
	playbackID, err := makeID("play-")
	if err != nil {
		return Snapshot{}, Token{}, nil, err
	}
	return m.StartAssignedPlayback(id, playbackID, source)
}

// StartAssignedPlayback accepts the canonical playback_id from HC Backend.
// The generation fence protects against callbacks from prior playback/seek.
func (m *Manager) StartAssignedPlayback(id, playbackID string, source Source) (Snapshot, Token, context.Context, error) {
	if !validPart(playbackID) || !validSource(source) {
		return Snapshot{}, Token{}, nil, ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, Token{}, nil, ErrRoomNotFound
	}
	if r.snapshot.State != Active {
		return Snapshot{}, Token{}, nil, ErrInvalidTransition
	}
	if r.snapshot.Generation == math.MaxUint32 {
		return Snapshot{}, Token{}, nil, ErrGenerationOverflow
	}
	if _, used := r.usedPlaybackIDs[playbackID]; used {
		return Snapshot{}, Token{}, nil, ErrRoomConflict
	}
	if r.playbackCancel != nil {
		r.playbackCancel()
	}
	workCtx, cancel := context.WithCancel(r.ctx)
	r.playbackCtx, r.playbackCancel = workCtx, cancel
	r.usedPlaybackIDs[playbackID] = struct{}{}
	r.snapshot.PlaybackID = playbackID
	r.snapshot.PlaybackState = Preparing
	file := source
	r.snapshot.Source = &file
	r.snapshot.Generation++ // nonzero; increments on switch and seek
	r.snapshot.PositionMS = 0
	r.snapshot.Revision++
	token := Token{RoomID: id, PlaybackID: playbackID, Generation: r.snapshot.Generation}
	return copySnapshot(r.snapshot), token, workCtx, nil
}

// MovePlayback accepts only current-generation callbacks; late callbacks after
// seek or switch cannot overwrite a newer playback or modify the room.
func (m *Manager) MovePlayback(t Token, next PlaybackState) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.current(t)
	if err != nil {
		return Snapshot{}, err
	}
	if !allowedPlayback(r.snapshot.PlaybackState, next) {
		return Snapshot{}, ErrInvalidTransition
	}
	r.snapshot.PlaybackState = next
	r.snapshot.Revision++
	if next == Ended || next == PlaybackError || next == Idle {
		if r.playbackCancel != nil {
			r.playbackCancel()
			r.playbackCancel = nil
		}
		r.playbackCtx = nil
	}
	return copySnapshot(r.snapshot), nil
}

func allowedPlayback(old, next PlaybackState) bool {
	switch old {
	case Preparing:
		return next == Buffering || next == PlaybackError
	case Buffering:
		return next == Playing || next == Paused || next == Ended || next == PlaybackError
	case Playing:
		return next == Paused || next == Ended || next == PlaybackError
	case Paused:
		return next == Playing || next == Ended || next == PlaybackError
	case Ended, PlaybackError:
		return next == Idle
	}
	return false
}

// EOF is playback-only: room remains ACTIVE; TV stays attached and may play
// a different file in the same room. Stale EOF cannot stop the next movie.
func (m *Manager) EOF(t Token) (Snapshot, error) { return m.MovePlayback(t, Ended) }

// Seek invalidates every outstanding worker's token and context. Generation
// is room-wide and strictly increases, even when playback_id stays the same.
func (m *Manager) Seek(t Token, positionMS int64) (Snapshot, Token, context.Context, error) {
	if positionMS < 0 {
		return Snapshot{}, Token{}, nil, ErrInvalidInput
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.current(t)
	if err != nil {
		return Snapshot{}, Token{}, nil, err
	}
	if r.snapshot.PlaybackState != Playing && r.snapshot.PlaybackState != Paused && r.snapshot.PlaybackState != Buffering && r.snapshot.PlaybackState != Preparing && r.snapshot.PlaybackState != Ended {
		return Snapshot{}, Token{}, nil, ErrInvalidTransition
	}
	if r.snapshot.Generation == math.MaxUint32 {
		return Snapshot{}, Token{}, nil, ErrGenerationOverflow
	}
	if r.playbackCancel != nil {
		r.playbackCancel()
	}
	workCtx, cancel := context.WithCancel(r.ctx)
	r.playbackCtx, r.playbackCancel = workCtx, cancel
	r.snapshot.Generation++
	r.snapshot.PositionMS = positionMS
	r.snapshot.PlaybackState = Preparing
	r.snapshot.Revision++
	newToken := Token{RoomID: t.RoomID, PlaybackID: t.PlaybackID, Generation: r.snapshot.Generation}
	return copySnapshot(r.snapshot), newToken, workCtx, nil
}

func (m *Manager) current(t Token) (*room, error) {
	r, ok := m.rooms[t.RoomID]
	if !ok {
		return nil, ErrRoomNotFound
	}
	if r.snapshot.State != Active || r.snapshot.PlaybackID == "" ||
		r.snapshot.PlaybackID != t.PlaybackID || r.snapshot.Generation != t.Generation {
		return nil, ErrStalePlayback
	}
	return r, nil
}

func (m *Manager) IsCurrent(t Token) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, err := m.current(t)
	return err == nil && r.snapshot.PlaybackState != Idle && r.snapshot.PlaybackState != Ended && r.snapshot.PlaybackState != PlaybackError
}

func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, ErrRoomNotFound
	}
	return copySnapshot(r.snapshot), nil
}
func (m *Manager) List() []Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Snapshot, 0, len(m.rooms))
	for _, r := range m.rooms {
		result = append(result, copySnapshot(r.snapshot))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RoomID < result[j].RoomID })
	return result
}
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

func copySnapshot(s Snapshot) Snapshot {
	if s.Source != nil {
		source := *s.Source
		s.Source = &source
	}
	return s
}

// Stop/Fail atomically close a room, release TV exclusivity, cancel both
// contexts and retain a bounded tombstone for idempotent repeated STOP.
func (m *Manager) Stop(id string) (Snapshot, error) { return m.closeRoom(id, ReasonStop) }
func (m *Manager) Fail(id string, reason CloseReason) (Snapshot, error) {
	if reason != ReasonLost && reason != ReasonError {
		return Snapshot{}, ErrInvalidInput
	}
	return m.closeRoom(id, reason)
}
func (m *Manager) closeRoom(id string, reason CloseReason) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.closed[id]; ok {
		return copySnapshot(old), nil
	}
	r, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, ErrRoomNotFound
	}
	return m.closeLocked(id, r, reason), nil
}
func (m *Manager) closeLocked(id string, r *room, reason CloseReason) Snapshot {
	if reason == ReasonLost || reason == ReasonError {
		r.snapshot.State = Lost
		r.snapshot.Revision++
	}
	r.snapshot.State = Closing
	r.snapshot.Revision++
	if r.playbackCancel != nil {
		r.playbackCancel()
	}
	r.cancel()
	r.snapshot.State = Closed
	r.snapshot.CloseReason = reason
	r.snapshot.Revision++
	final := copySnapshot(r.snapshot)
	delete(m.targets, r.snapshot.Target)
	delete(m.rooms, id)
	m.closed[id] = final
	m.closedOrder = append(m.closedOrder, id)
	if len(m.closedOrder) > closedHistoryLimit {
		delete(m.closed, m.closedOrder[0])
		m.closedOrder = m.closedOrder[1:]
	}
	return final
}

// Close cancels every local resource, is safe to call repeatedly, and doesn't
// create or require background goroutines. Future workers must honor Context.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	m.stopped = true
	for id, r := range m.rooms {
		m.closeLocked(id, r, ReasonShutdown)
	}
	m.cancel()
}

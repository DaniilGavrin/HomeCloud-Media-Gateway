// Package directws owns transient, one-use TV attachment grants. Only Gateway
// control offers from its co-located Backend may create these grants.
package directws

import (
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

var (
	ErrRejected = errors.New("ATTACH_REJECTED")
	ErrStale    = errors.New("STALE_PLAYBACK")
	ErrConflict = errors.New("ROOM_CONFLICT")
)

type Grant struct {
	RoomID         string    `json:"room_id"`
	PlaybackID     string    `json:"playback_id"`
	DeviceID       string    `json:"device_id"`
	Generation     uint32    `json:"generation"`
	SeekPositionMS *int64    `json:"seek_position_ms,omitempty"`
	Ticket         string    `json:"attach_ticket"`
	ExpiresAt      time.Time `json:"attach_expires_at"`
}
type Hello struct {
	V            int            `json:"v"`
	Type         string         `json:"type"`
	RoomID       string         `json:"room_id"`
	PlaybackID   string         `json:"playback_id"`
	DeviceID     string         `json:"device_id"`
	Generation   uint32         `json:"generation"`
	AttachTicket string         `json:"attach_ticket"`
	Capabilities map[string]any `json:"capabilities,omitempty"`
}
type Lease struct {
	RoomID         string
	PlaybackID     string
	DeviceID       string
	Generation     uint32
	SeekPositionMS *int64
	Epoch          uint64
	Done           <-chan struct{}
	// RoomDone survives playback generation switches, but closes on STOP.
	RoomDone       <-chan struct{}
}
type slot struct {
	grant  Grant
	used   bool
	epoch  uint64
	active chan struct{}
	roomDone chan struct{}
}
type Book struct {
	mu     sync.Mutex
	byRoom map[string]*slot
	closed bool
	max    int
	now    func() time.Time
}

func New(max int) *Book {
	if max < 1 {
		max = 1
	}
	return &Book{max: max, byRoom: map[string]*slot{}, now: time.Now}
}
func validGrant(g Grant, now time.Time) bool {
	b, e := hex.DecodeString(g.Ticket)
	if g.SeekPositionMS != nil && (*g.SeekPositionMS < 0 || *g.SeekPositionMS > 7*24*60*60*1000) {
		return false
	}
	return g.RoomID != "" && g.PlaybackID != "" && g.DeviceID != "" && g.Generation > 0 && e == nil && len(b) >= 16 && len(b) <= 64 && !g.ExpiresAt.IsZero() && g.ExpiresAt.After(now) && !g.ExpiresAt.After(now.Add(61*time.Second))
}

// Prepare atomically replaces a grant. A new playback fences any old TV socket.
// A same-playback refresh leaves the current socket alive until new attach.
func (b *Book) Prepare(g Grant) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || !validGrant(g, b.now()) {
		return ErrRejected
	}
	s, ok := b.byRoom[g.RoomID]
	if !ok {
		if len(b.byRoom) >= b.max {
			return ErrConflict
		}
		s = &slot{roomDone:make(chan struct{})}
		b.byRoom[g.RoomID] = s
	}
	if s.grant.DeviceID != "" && s.grant.DeviceID != g.DeviceID {
		return ErrConflict
	}
	if s.grant.Generation > g.Generation {
		return ErrStale
	}
	if s.grant.Generation == g.Generation && s.grant.PlaybackID != "" && s.grant.PlaybackID != g.PlaybackID {
		return ErrStale
	}
	if s.grant.Generation == g.Generation && s.grant.PlaybackID == g.PlaybackID {
		if s.grant.Ticket == g.Ticket {
			return nil
		} // duplicate delivery must NEVER re-arm a consumed ticket
		if !g.ExpiresAt.After(s.grant.ExpiresAt) {
			return ErrStale
		} // reject an out-of-order older refresh
	}
	if s.grant.Generation != g.Generation && s.active != nil {
		close(s.active)
		s.active = nil
	}
	s.grant = g
	s.used = false
	return nil
}

// Claim burns the ticket before any media activity; retransmission fails.
func (b *Book) Claim(h Hello) (Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.byRoom[h.RoomID]
	if !ok || b.closed {
		return Lease{}, ErrRejected
	}
	if h.Generation != s.grant.Generation || h.PlaybackID != s.grant.PlaybackID {
		return Lease{}, ErrStale
	}
	if h.V != 1 || h.Type != "media.hello" || h.DeviceID != s.grant.DeviceID || h.AttachTicket == "" || s.used || !b.now().Before(s.grant.ExpiresAt) {
		return Lease{}, ErrRejected
	}
	// Normalize neither token nor device labels. A ticket is an opaque secret.
	if subtle.ConstantTimeCompare([]byte(h.AttachTicket), []byte(s.grant.Ticket)) != 1 {
		return Lease{}, ErrRejected
	}
	for id, other := range b.byRoom {
		if id != h.RoomID && other.active != nil && other.grant.DeviceID == h.DeviceID {
			return Lease{}, ErrConflict
		}
	}
	if s.epoch == ^uint64(0) {
		return Lease{}, ErrConflict
	}
	if s.active != nil {
		close(s.active)
	}
	s.epoch++
	s.used = true
	s.active = make(chan struct{})
	var position *int64
	if s.grant.SeekPositionMS != nil {
		value := *s.grant.SeekPositionMS
		position = &value
	}
	return Lease{RoomID: h.RoomID, PlaybackID: h.PlaybackID, DeviceID: h.DeviceID, Generation: h.Generation, SeekPositionMS: position, Epoch: s.epoch, Done: s.active, RoomDone:s.roomDone}, nil
}

// Release reports true only for the newest connection, preventing late cleanup.
func (b *Book) Release(l Lease) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.byRoom[l.RoomID]
	if s == nil || s.epoch != l.Epoch || s.active == nil || s.grant.PlaybackID != l.PlaybackID || s.grant.Generation != l.Generation {
		return false
	}
	close(s.active)
	s.active = nil
	return true
}
func (b *Book) Remove(roomID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.byRoom[roomID]; s != nil {
		if s.active != nil {
			close(s.active)
		}
		close(s.roomDone)
		delete(b.byRoom, roomID)
	}
}
func (b *Book) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.byRoom {
		if s.active != nil {
			close(s.active)
		}
		close(s.roomDone)
	}
	b.byRoom = map[string]*slot{}
}
func (b *Book) Close()     { b.Reset(); b.mu.Lock(); b.closed = true; b.mu.Unlock() }
func (b *Book) Count() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.byRoom) }
func (b *Book) Valid(roomID string, lease Lease) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.byRoom[roomID]
	return s != nil && s.active != nil && s.epoch == lease.Epoch && s.grant.PlaybackID == lease.PlaybackID && s.grant.Generation == lease.Generation
}
func Code(err error) string {
	if errors.Is(err, ErrStale) {
		return "STALE_PLAYBACK"
	}
	if errors.Is(err, ErrConflict) {
		return "ROOM_CONFLICT"
	}
	return "ATTACH_REJECTED"
}

type Update struct {
	RoomID           string `json:"room_id"`
	PlaybackID       string `json:"playback_id"`
	Generation       uint32 `json:"generation"`
	Epoch            uint64 `json:"connection_epoch"`
	Phase            string `json:"phase"`
	ActualPositionMS int64  `json:"actual_position_ms,omitempty"`
	Sequence         uint64 `json:"report_sequence,omitempty"`
	ErrorCode        string `json:"error_code,omitempty"`
}

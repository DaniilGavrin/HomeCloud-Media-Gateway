package directws

import (
	"errors"
	"sync"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
)

var ErrNoStream = errors.New("NO_ACTIVE_STREAM")
var ErrStaleControl = errors.New("STALE_CONTROL")
var ErrEndedStream = errors.New("STREAM_ENDED")

type PlaybackControl struct {
	RoomID          string `json:"room_id"`
	PlaybackID      string `json:"playback_id"`
	Generation      uint32 `json:"generation"`
	Epoch           uint64 `json:"connection_epoch"`
	CommandID       string `json:"command_id"`
	CommandSequence uint64 `json:"command_sequence"`
	Action          string `json:"action"`
}

type controlledStream struct {
	lease         Lease
	sender        *mediaframe.Sender
	lastCommandID string
	lastSequence  uint64
	passiveHTTP   bool
}

// StreamControl attaches control-plane pause/resume to exactly one currently
// attached HCM1 sender. It never changes a playback ID or opens a file.
type StreamControl struct {
	mu     sync.Mutex
	active map[string]*controlledStream
}

func NewStreamControl() *StreamControl {
	return &StreamControl{active: make(map[string]*controlledStream)}
}
func (c *StreamControl) Register(lease Lease, sender *mediaframe.Sender) {
	if c == nil || sender == nil {
		return
	}
	c.mu.Lock()
	c.active[lease.RoomID] = &controlledStream{lease: lease, sender: sender}
	c.mu.Unlock()
}
// RegisterHTTP tracks a lease whose media data is delivered by HTTP Range.
// HTTP media requests are demand-driven by the TV decoder; Gateway has no
// HCM1 sender to pause, so it only fences/acknowledges control-plane intent.
// TV's independent execution ACK and decoder observation prove actual pause.
func (c *StreamControl) RegisterHTTP(lease Lease) {
	if c == nil { return }
	c.mu.Lock()
	c.active[lease.RoomID] = &controlledStream{lease:lease, passiveHTTP:true}
	c.mu.Unlock()
}

func (c *StreamControl) Unregister(lease Lease) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.active[lease.RoomID]; st != nil && st.lease.Epoch == lease.Epoch && st.lease.Generation == lease.Generation && st.lease.PlaybackID == lease.PlaybackID {
		delete(c.active, lease.RoomID)
	}
}
func (c *StreamControl) Apply(q PlaybackControl) error {
	if c == nil {
		return ErrNoStream
	}
	if q.CommandID == "" || q.CommandSequence == 0 {
		return ErrStaleControl
	}
	if q.Action != "pause" && q.Action != "resume" && q.Action != "play" {
		return ErrStaleControl
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.active[q.RoomID]
	if st == nil {
		return ErrNoStream
	}
	if st.lease.PlaybackID != q.PlaybackID || st.lease.Generation != q.Generation || st.lease.Epoch != q.Epoch {
		return ErrStaleControl
	}
	select {
	case <-st.lease.Done:
		return ErrNoStream
	default:
	}
	if q.CommandSequence == st.lastSequence && q.CommandID == st.lastCommandID {
		return nil
	}
	if q.CommandSequence <= st.lastSequence {
		return ErrStaleControl
	}
	if !st.passiveHTTP {
		if st.sender == nil || !st.sender.SetPaused(q.Action == "pause") {
			return ErrEndedStream
		}
	}
	// In HTTP Range mode there is no producer to pause: the native TV
	// decoder controls network reads. Never mark PLAYING from this ACK.
	st.lastSequence = q.CommandSequence
	st.lastCommandID = q.CommandID
	return nil
}

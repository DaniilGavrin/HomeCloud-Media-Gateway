package mediaframe

// SetPaused gates future source reads without closing the current HCM1
// WebSocket, releasing the source or discarding already acknowledged data.
// One already in-progress read/write of at most MaxPayloadBytes may finish.
// False means the sender has passed EOF and cannot be resumed.
func (s *Sender) SetPaused(paused bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return false
	}
	if s.paused != paused {
		s.paused = paused
		close(s.wake)
		s.wake = make(chan struct{})
	}
	return true
}

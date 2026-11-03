package directws

// RemoveRoom drops sender routing immediately after STOP. Book.Remove is
// responsible for revoking the lease and closing its direct WebSocket;
// this method does not pretend that merely removing a map entry stops IO.
func (c *StreamControl) RemoveRoom(roomID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.active, roomID)
	c.mu.Unlock()
}

// Reset fences sender control for every room following Backend reconnect.
func (c *StreamControl) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.active)
	c.mu.Unlock()
}

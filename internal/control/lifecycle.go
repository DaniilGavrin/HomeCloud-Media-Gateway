package control

import (
	"errors"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
)

// observeStreamLifecycle advances only the Gateway-local playback worker.
// STREAMING is a real sender write; ENDED is the actual HCM1 EOF. Neither
// event means that the TV has completed decoding: Backend waits for TV's
// independent ENDED report before notifying the user interfaces.
// EOF cancels the local source worker, NOT the room context or direct WS lease.
func observeStreamLifecycle(manager *rooms.Manager, u directws.Update) error {
	if manager == nil {
		return nil
	}
	if u.Phase != "STREAMING" && u.Phase != "ENDED" && u.Phase != "ERROR" {
		return nil
	}
	current, err := manager.Get(u.RoomID)
	if err != nil {
		return err
	}
	if current.PlaybackID != u.PlaybackID || current.Generation != u.Generation {
		return rooms.ErrStalePlayback
	}
	tok := rooms.Token{RoomID: u.RoomID, PlaybackID: u.PlaybackID, Generation: u.Generation}
	phase := current.PlaybackState
	if phase == rooms.Idle || phase == rooms.Ended || phase == rooms.PlaybackError {
		// A stale delayed streaming callback must never resurrect completed media.
		if (phase == rooms.Ended && u.Phase == "ENDED") || (phase == rooms.PlaybackError && u.Phase == "ERROR") {
			return nil
		}
		return rooms.ErrStalePlayback
	}
	if phase == rooms.Preparing && u.Phase != "ERROR" {
		if _, err = manager.MovePlayback(tok, rooms.Buffering); err != nil {
			return err
		}
		phase = rooms.Buffering
	}
	switch u.Phase {
	case "STREAMING":
		if phase == rooms.Buffering {
			_, err = manager.MovePlayback(tok, rooms.Playing)
		}
		return err
	case "ENDED":
		if phase == rooms.Buffering || phase == rooms.Playing || phase == rooms.Paused {
			_, err = manager.MovePlayback(tok, rooms.Ended)
		}
		return err
	case "ERROR":
		_, err = manager.MovePlayback(tok, rooms.PlaybackError)
		if errors.Is(err, rooms.ErrInvalidTransition) && phase == rooms.PlaybackError {
			return nil
		}
		return err
	}
	return nil
}

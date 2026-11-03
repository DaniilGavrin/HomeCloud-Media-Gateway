package control

import (
	"context"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

// prepareRoomOfferSource performs a bounded metadata lookup on the registered
// owner Storage Service, after room generation validation but before Gateway
// acknowledges the offer as APPLIED. Nil coordinator preserves the pre-HC-SS
// control-only mode; it is NOT proof of media readiness.
func prepareRoomOfferSource(ctx context.Context, coordinator *source.Coordinator, offer roomOffer) error {
	if coordinator == nil {
		return nil
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	_, err := coordinator.Prepare(resolveCtx,
		source.Token{RoomID: offer.RoomID, PlaybackID: offer.PlaybackID, Generation: offer.Generation},
		source.Reference{StorageNodeID: offer.Source.StorageNodeID, FileID: offer.Source.FileID})
	return err
}

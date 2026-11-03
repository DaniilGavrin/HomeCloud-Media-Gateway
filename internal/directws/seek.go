package directws

import (
	"context"
	"errors"
	"io"
)

var ErrSeekUnsupported = errors.New("SEEK_UNSUPPORTED")
var ErrSeekResolution = errors.New("INVALID_SEEK_RESOLUTION")

// SeekStreamOpener is the future source/pipeline boundary. Only trusted
// in-process storage readers or transcoding pipelines may implement it.
// The caller must open the returned stream at its achieved timeline position,
// not merely rewind to byte zero. Source/keyframe precision may differ from
// target; the TV independently confirms its displayed position.
type SeekStreamOpener func(context.Context, Lease) (io.ReadCloser, int64, error)

func validSeekResolution(lease Lease, actualMS int64) bool {
	return lease.SeekPositionMS != nil && *lease.SeekPositionMS >= 0 &&
		*lease.SeekPositionMS <= 7*24*60*60*1000 && actualMS >= 0 && actualMS <= 7*24*60*60*1000
}

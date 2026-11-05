package source

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
)

// RemoteReaderFactory fetches only authorized, version-fenced HCS1 blocks.
// Until the on-disk KAN-58 cache is implemented it uses a bounded streaming
// fallback for BOTH remote strategies. It never preloads a whole media file.
type RemoteReaderFactory struct { Registry Registry }

func (f RemoteReaderFactory) Open(ctx context.Context, plan Decision, byteOffset int64) (Reader, error) {
	if ctx.Err() != nil { return nil, ctx.Err() }
	if f.Registry == nil || (plan.Strategy != RemoteBlockStream && plan.Strategy != RemotePrefetch) {
		return nil, ErrNotConfigured
	}
	meta := plan.Metadata()
	ref := plan.Reference()
	if err := meta.Validate(ref); err != nil { return nil, err }
	if !meta.RangeReadable || byteOffset < 0 || (meta.SizeBytes != nil && byteOffset > *meta.SizeBytes) {
		return nil, ErrInvalidReference
	}
	client, err := f.Registry.ForNode(ctx, ref.StorageNodeID)
	if err != nil { return nil, fmt.Errorf("%w: %v", ErrSourceOffline, err) }
	if client == nil { return nil, ErrSourceOffline }
	live, cancel := context.WithCancel(ctx)
	return &remoteReader{
		ctx:live, cancel:cancel, client:client, meta:meta,
		offset:byteOffset, version:meta.Version,
	}, nil
}

type remoteReader struct {
	ctx context.Context
	cancel context.CancelFunc
	client Client
	meta Metadata
	version string
	offset int64
	buffer []byte
	end bool
	closed atomic.Bool
}

// Not safe for simultaneous Read calls. Close is concurrency-safe and cancels
// a blocked HCS1 request.
func (r *remoteReader) Read(dst []byte) (int,error) {
	if len(dst) == 0 { return 0,nil }
	if r.closed.Load() { return 0,io.ErrClosedPipe }
	if err := r.ctx.Err(); err != nil { return 0,err }
	for len(r.buffer) == 0 {
		if r.end || (r.meta.SizeBytes != nil && r.offset >= *r.meta.SizeBytes) { return 0,io.EOF }
		const requestSize = 256 << 10
		length := int64(requestSize)
		if r.meta.SizeBytes != nil && length > *r.meta.SizeBytes-r.offset {
			length = *r.meta.SizeBytes-r.offset
		}
		if length <= 0 { r.end = true; return 0,io.EOF }
		read, reply, err := r.client.OpenRange(r.ctx, RangeRequest{
			FileID:r.meta.FileID,Version:r.version,Offset:r.offset,Length:length,
		})
		if err != nil { return 0,err }
		// Per-range buffering is capped at 256KiB and independent of file size.
		payload, err := io.ReadAll(io.LimitReader(read, length+1))
		closeErr := read.Close()
		if err != nil { return 0,err }
		if closeErr != nil { return 0,closeErr }
		if int64(len(payload)) != reply.Length || len(payload) > int(length) ||
			(len(payload) == 0 && !reply.EOF) {
			return 0,ErrInvalidMetadata
		}
		if reply.Version != r.version || reply.Offset != r.offset { return 0,ErrSourceChanged }
		r.offset += int64(len(payload))
		r.buffer = payload
		r.end = reply.EOF
	}
	n := copy(dst,r.buffer)
	r.buffer = r.buffer[n:]
	return n,nil
}

func (r *remoteReader) Close() error {
	if r.closed.CompareAndSwap(false,true) {
		r.cancel()
	}
	return nil
}
var _ io.ReadCloser = (*remoteReader)(nil)
var _ ReaderFactory = RemoteReaderFactory{}

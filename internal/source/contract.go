// Package source defines Gateway's storage-authorized file access contract.
// HomeCloud WS Backend owns rooms; this package never sends media bytes there.
// A trusted HC Storage Service owns each file_id and serves remote byte ranges.
package source

import (
	"context"
	"errors"
	"io"
	"math"
	"path/filepath"
	"strings"
)

const (
	// Exactly 1 GiB is eligible for disk prefetch; the threshold is NOT RAM.
	PrefetchLimitBytes int64 = 1 << 30
	// A bounded single range request, not an unbounded whole-file download.
	MaxRangeBytes int64 = 4 << 20
)

type Strategy string

const (
	LocalDirect       Strategy = "LOCAL_DIRECT"
	RemotePrefetch    Strategy = "REMOTE_PREFETCH"
	RemoteBlockStream Strategy = "REMOTE_BLOCK_STREAM"
)

var (
	ErrInvalidReference = errors.New("INVALID_SOURCE_REFERENCE")
	ErrInvalidMetadata  = errors.New("INVALID_SOURCE_METADATA")
	ErrUntrustedPath    = errors.New("UNTRUSTED_LOCAL_PATH")
	ErrSourceOffline    = errors.New("STORAGE_UNAVAILABLE")
	ErrRangeUnsupported = errors.New("RANGE_UNSUPPORTED")
	ErrSourceChanged    = errors.New("SOURCE_VERSION_CHANGED")
	ErrNotConfigured    = errors.New("SOURCE_ADAPTER_NOT_CONFIGURED")
	ErrStalePlan        = errors.New("STALE_SOURCE_PLAN")
)

// Reference travels only through trusted Backend -> Gateway room offers.
// A TV cannot select a Storage Service endpoint or arbitrary filesystem path.
type Reference struct {
	StorageNodeID string `json:"storage_node_id"`
	FileID        string `json:"file_id"`
}

// LocalHint is issued by authenticated HC Storage Service. RelativePath is
// relative to an explicit shared_root_id; it is NEVER an arbitrary absolute path.
// Both processes must map that ID to the same filesystem and verify real access.
type LocalHint struct {
	SharedRootID string `json:"shared_root_id"`
	RelativePath string `json:"relative_path"`
	// If the Storage Agent cannot supply a stable timestamp, do not use local.
	ModifiedUnixNano *int64 `json:"modified_unix_nano,omitempty"`
}

// Metadata is an HC SS contract proposal, not a claim that HC SS already
// implements these commands. Version is an opaque file-revision fence.
// SizeBytes==nil means unknown, 0 means an empty regular file.
type Metadata struct {
	OwnerNodeID   string     `json:"owner_node_id"`
	FileID        string     `json:"file_id"`
	Version       string     `json:"version"`
	SizeBytes     *int64     `json:"size_bytes"`
	ContentType   string     `json:"content_type,omitempty"`
	RangeReadable bool       `json:"range_readable"`
	Local         *LocalHint `json:"local_hint,omitempty"`
}

// Registry routes solely by StorageNodeID from the Backend-validated offer.
// It MUST NOT resolve attacker-supplied URLs or user-controlled hostnames.
type Registry interface {
	ForNode(ctx context.Context, storageNodeID string) (Client, error)
}

// Client is a trusted HC SS connection: Stat and OpenRange travel over its
// authenticated/approved private WS. Binary payloads never go via main Backend.
type Client interface {
	Stat(ctx context.Context, fileID string) (Metadata, error)
	OpenRange(ctx context.Context, req RangeRequest) (io.ReadCloser, RangeReply, error)
}

// RangeRequest translates into a bounded HC SS request. The connector must
// reject mismatching Version and never return more than Length bytes.
type RangeRequest struct {
	FileID  string `json:"file_id"`
	Version string `json:"version"`
	Offset  int64  `json:"offset"`
	Length  int64  `json:"length"`
}

type RangeReply struct {
	Offset         int64  `json:"offset"`
	Length         int64  `json:"length"`
	Version        string `json:"version"`
	EOF            bool   `json:"eof"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
}

func validID(s string) bool {
	return s != "" && len(s) <= 256 && s == strings.TrimSpace(s) && !strings.ContainsAny(s, "\r\n\x00")
}
func (r Reference) Validate() error {
	if !validID(r.StorageNodeID) || !validID(r.FileID) {
		return ErrInvalidReference
	}
	return nil
}
func (m Metadata) Validate(ref Reference) error {
	if !validID(m.OwnerNodeID) || !validID(m.FileID) || !validID(m.Version) ||
		m.OwnerNodeID != ref.StorageNodeID || m.FileID != ref.FileID ||
		(m.SizeBytes != nil && *m.SizeBytes < 0) ||
		(m.Local != nil && (!validID(m.Local.SharedRootID) ||
			m.Local.RelativePath == "" || m.Local.RelativePath == "." ||
			!filepath.IsLocal(filepath.FromSlash(m.Local.RelativePath)))) {
		return ErrInvalidMetadata
	}
	return nil
}
func (q RangeRequest) Validate() error {
	if !validID(q.FileID) || !validID(q.Version) || q.Offset < 0 || q.Length < 1 ||
		q.Length > MaxRangeBytes || q.Offset > math.MaxInt64-q.Length {
		return ErrInvalidReference
	}
	return nil
}
func (r RangeReply) Validate(q RangeRequest) error {
	if r.Version != q.Version {
		return ErrSourceChanged
	}
	if r.Offset != q.Offset || r.Length < 0 || r.Length > q.Length {
		return ErrInvalidMetadata
	}
	return nil
}

// Reader is deliberately the same for local, prefetched and block-streamed
// sources. Seek opens a NEW byte-range reader; milliseconds -> byte mapping
// belongs to the media inspector/pipeline (KAN-61–64), not the resolver.
type Reader = io.ReadCloser

type ReaderFactory interface {
	Open(ctx context.Context, plan Decision, byteOffset int64) (Reader, error)
}

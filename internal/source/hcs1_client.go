package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const hcs1Path = "/ws/source"

// StaticRegistry explicitly binds a trusted HC Storage Service NODE ID to
// its private, administrator-configured endpoint. TV/UI cannot set URLs.
type StaticRegistry struct {
	Nodes map[string]string
}

func ValidateHCS1URL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil { return err }
	if u.Scheme != "ws" && u.Scheme != "wss" { return fmt.Errorf("HCS1 requires ws/wss") }
	if u.Host == "" || u.Port() == "" || u.Path != hcs1Path || u.RawQuery != "" ||
		u.Fragment != "" || u.User != nil {
		return fmt.Errorf("invalid HCS1 endpoint syntax")
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("HCS1 endpoint must use an explicit IP")
	}
	ip = ip.Unmap()
	if !(ip.IsPrivate() || ip.IsLoopback() || netip.MustParsePrefix("100.64.0.0/10").Contains(ip)) {
		return fmt.Errorf("HCS1 endpoint must be private LAN/loopback/Tailscale")
	}
	return nil
}

func (r StaticRegistry) ForNode(ctx context.Context, storageNodeID string) (Client, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	if !validID(storageNodeID) { return nil, ErrInvalidReference }
	endpoint, ok := r.Nodes[storageNodeID]
	if !ok { return nil, ErrSourceOffline }
	if err := ValidateHCS1URL(endpoint); err != nil { return nil, err }
	return HCS1Client{URL:endpoint}, nil
}

// HCS1Client uses a fresh private WS for each bounded request. The service
// limits concurrent active requests by its HTTP/WS listener. A future pooled
// connector may reuse sockets, but must preserve serialized header+binary
// response correlation and generation fencing.
type HCS1Client struct { URL string }

type hcs1Reply struct {
	V int `json:"v"`
	Type string `json:"type"`
	ID string `json:"id"`
	Code string `json:"code,omitempty"`
	Metadata Metadata `json:"metadata,omitempty"`
	RangeReply
}

func (c HCS1Client) request(ctx context.Context, data map[string]any, binary bool) (hcs1Reply, []byte, error) {
	if err := ValidateHCS1URL(c.URL); err != nil { return hcs1Reply{}, nil, err }
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(opCtx, c.URL, nil)
	if err != nil { return hcs1Reply{}, nil, fmt.Errorf("%w: HCS1 dial: %v", ErrSourceOffline, err) }
	defer ws.CloseNow()
	ws.SetReadLimit(MaxRangeBytes + 8192)
	raw, err := json.Marshal(data)
	if err != nil { return hcs1Reply{}, nil, err }
	if err := ws.Write(opCtx, websocket.MessageText, raw); err != nil {
		return hcs1Reply{}, nil, fmt.Errorf("%w: HCS1 request: %v", ErrSourceOffline, err)
	}
	kind, raw, err := ws.Read(opCtx)
	if err != nil || kind != websocket.MessageText || len(raw) > 8192 {
		return hcs1Reply{}, nil, fmt.Errorf("%w: invalid HCS1 response: %v", ErrSourceOffline, err)
	}
	var resp hcs1Reply
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil || dec.Decode(new(any)) != io.EOF {
		return hcs1Reply{}, nil, fmt.Errorf("%w: malformed HCS1 header", ErrInvalidMetadata)
	}
	if resp.V != 1 || resp.ID != data["id"] {
		return hcs1Reply{}, nil, ErrInvalidMetadata
	}
	if resp.Type == "source.error" {
		switch resp.Code {
		case "SOURCE_VERSION_CHANGED": return hcs1Reply{}, nil, ErrSourceChanged
		case "INVALID_RANGE", "INVALID_FILE_ID", "PATH_NOT_ALLOWED": return hcs1Reply{}, nil, ErrInvalidReference
		default: return hcs1Reply{}, nil, fmt.Errorf("%w: HCS1 %s", ErrSourceOffline, resp.Code)
		}
	}
	expected := "source.metadata"
	if binary { expected = "source.range" }
	if resp.Type != expected { return hcs1Reply{}, nil, ErrInvalidMetadata }
	if !binary { return resp, nil, nil }
	kind, payload, err := ws.Read(opCtx)
	if err != nil || kind != websocket.MessageBinary || len(payload) > int(MaxRangeBytes) {
		return hcs1Reply{}, nil, fmt.Errorf("%w: invalid HCS1 binary frame: %v", ErrSourceOffline, err)
	}
	return resp, payload, nil
}

func (c HCS1Client) Stat(ctx context.Context, fileID string) (Metadata, error) {
	if fileID == "" || len(fileID) > 2048 { return Metadata{}, ErrInvalidReference }
	reply, _, err := c.request(ctx, map[string]any{
		"v":1, "type":"source.stat", "id":"stat-1", "file_id":fileID,
	}, false)
	if err != nil { return Metadata{}, err }
	if reply.Metadata.FileID != fileID { return Metadata{}, ErrInvalidMetadata }
	return reply.Metadata, nil
}

func (c HCS1Client) OpenRange(ctx context.Context, q RangeRequest) (io.ReadCloser, RangeReply, error) {
	if err := q.Validate(); err != nil { return nil, RangeReply{}, err }
	reply, payload, err := c.request(ctx, map[string]any{
		"v":1, "type":"source.read", "id":"range-1",
		"file_id":q.FileID,"version":q.Version, "offset":q.Offset,"length":q.Length,
	}, true)
	if err != nil { return nil, RangeReply{}, err }
	head := reply.RangeReply
	if err := head.Validate(q); err != nil { return nil, RangeReply{}, err }
	if int64(len(payload)) != head.Length || len(head.ChecksumSHA256) != 64 ||
		(len(payload) == 0 && !head.EOF) {
		return nil, RangeReply{}, ErrInvalidMetadata
	}
	sum := sha256.Sum256(payload)
	if !strings.EqualFold(head.ChecksumSHA256, hex.EncodeToString(sum[:])) {
		return nil, RangeReply{}, fmt.Errorf("%w: checksum mismatch", ErrInvalidMetadata)
	}
	return io.NopCloser(bytes.NewReader(payload)), head, nil
}
var _ Client = HCS1Client{}
var _ Registry = StaticRegistry{}

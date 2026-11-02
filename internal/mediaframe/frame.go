// Package mediaframe defines the bounded, ordered HCM1 media transport.
// The channel carries bytes; decoding and file selection belong elsewhere.
package mediaframe

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	Version               = 1
	HeaderSize            = 56
	MaxPayloadBytes       = 64 * 1024
	WindowBytes           = 4 * 1024 * 1024
	MaxPendingFrames      = 64
	FlagEOF          byte = 1
)

var (
	ErrFrame = errors.New("invalid HCM1 media frame")
	ErrAck   = errors.New("invalid or stale media ACK")
)

// Identity fences frames to both the playback and the attached WebSocket epoch.
type Identity struct {
	RoomID     string
	PlaybackID string
	Generation uint32
	Epoch      uint64
}

type Frame struct {
	Identity Identity
	Sequence uint64
	Offset   uint64
	EOF      bool
	Payload  []byte
}

// A frame is exactly a 56-byte header followed by up to 64 KiB of payload.
// The truncated SHA-256 fingerprint disambiguates playback IDs without storing
// variable-length, untrusted strings in binary frames.
func (f Frame) MarshalBinary() ([]byte, error) {
	if f.Identity.PlaybackID == "" || f.Identity.Generation == 0 || f.Identity.Epoch == 0 || len(f.Payload) > MaxPayloadBytes ||
		(f.EOF && len(f.Payload) != 0) || (!f.EOF && len(f.Payload) == 0) {
		return nil, ErrFrame
	}
	raw := make([]byte, HeaderSize+len(f.Payload))
	copy(raw[:4], "HCM1")
	raw[4] = Version
	if f.EOF {
		raw[5] = FlagEOF
	}
	binary.BigEndian.PutUint16(raw[6:8], HeaderSize)
	binary.BigEndian.PutUint32(raw[8:12], f.Identity.Generation)
	binary.BigEndian.PutUint64(raw[12:20], f.Identity.Epoch)
	binary.BigEndian.PutUint64(raw[20:28], f.Sequence)
	binary.BigEndian.PutUint64(raw[28:36], f.Offset)
	binary.BigEndian.PutUint32(raw[36:40], uint32(len(f.Payload)))
	digest := sha256.Sum256([]byte(f.Identity.PlaybackID))
	copy(raw[40:56], digest[:16])
	copy(raw[HeaderSize:], f.Payload)
	return raw, nil
}

func Parse(raw []byte, id Identity) (Frame, error) {
	if len(raw) < HeaderSize || len(raw) > HeaderSize+MaxPayloadBytes || string(raw[:4]) != "HCM1" ||
		raw[4] != Version || raw[5] & ^FlagEOF != 0 || binary.BigEndian.Uint16(raw[6:8]) != HeaderSize ||
		binary.BigEndian.Uint32(raw[8:12]) != id.Generation || binary.BigEndian.Uint64(raw[12:20]) != id.Epoch ||
		int(binary.BigEndian.Uint32(raw[36:40])) != len(raw)-HeaderSize || id.PlaybackID == "" {
		return Frame{}, ErrFrame
	}
	digest := sha256.Sum256([]byte(id.PlaybackID))
	for i := 0; i < 16; i++ {
		if raw[40+i] != digest[i] {
			return Frame{}, ErrFrame
		}
	}
	eof := raw[5] == FlagEOF
	if (eof && len(raw) != HeaderSize) || (!eof && len(raw) == HeaderSize) {
		return Frame{}, ErrFrame
	}
	return Frame{Identity: id, Sequence: binary.BigEndian.Uint64(raw[20:28]), Offset: binary.BigEndian.Uint64(raw[28:36]), EOF: eof, Payload: raw[HeaderSize:]}, nil
}

// Ack is cumulative. The receiver ACKs a *complete* data frame by sequence
// and end offset; arbitrary offset advances are never credited.
type Ack struct {
	V             int    `json:"v"`
	Type          string `json:"type"`
	RoomID        string `json:"room_id"`
	PlaybackID    string `json:"playback_id"`
	Generation    uint32 `json:"generation"`
	Epoch         uint64 `json:"connection_epoch"`
	Sequence      uint64 `json:"sequence"`
	ReceivedBytes uint64 `json:"received_bytes"`
}

package mux

import (
	"fmt"
	"time"
)

// TCP envelope: one direction byte, then an unsigned 16-bit little-endian
// payload length, then the payload.
const (
	MaxPayloadSize          = 176
	ClientToCompanionMarker = byte('<')
	CompanionToClientMarker = byte('>')
)

// FrameError reports an invalid, truncated, or too slowly assembled frame.
type FrameError struct{ msg string }

func (e *FrameError) Error() string { return e.msg }

// Decoder incrementally removes the TCP envelope and returns owned payloads.
//
// `now` must come from a monotonic clock. The decoder does not read a clock
// itself, which keeps deadline handling deterministic.
type Decoder struct {
	marker      byte
	timeout     time.Duration
	header      [3]byte
	headerSize  int
	payload     []byte
	payloadSize int
	startedAt   time.Duration
	started     bool
}

// NewDecoder creates a decoder expecting the given direction marker.
func NewDecoder(marker byte, assemblyTimeout time.Duration) *Decoder {
	if marker != ClientToCompanionMarker && marker != CompanionToClientMarker {
		panic("frame marker must be '<' or '>'")
	}
	if assemblyTimeout <= 0 {
		panic("assembly timeout must be positive")
	}
	return &Decoder{marker: marker, timeout: assemblyTimeout}
}

// Feed accepts any portion of the stream and returns every payload completed
// by it. Payloads completed before an error are still returned.
func (d *Decoder) Feed(b []byte, now time.Duration) ([][]byte, error) {
	if err := d.CheckDeadline(now); err != nil {
		return nil, err
	}
	var out [][]byte
	off := 0
	for off < len(b) {
		if d.headerSize == 0 && d.payload == nil {
			d.startedAt = now
			d.started = true
		}
		if d.headerSize < 3 {
			d.header[d.headerSize] = b[off]
			d.headerSize++
			off++
			if d.headerSize == 1 && d.header[0] != d.marker {
				return out, &FrameError{fmt.Sprintf("wrong frame marker 0x%x; expected 0x%x", d.header[0], d.marker)}
			}
			if d.headerSize == 3 {
				size := int(d.header[1]) | int(d.header[2])<<8
				if size < 1 || size > MaxPayloadSize {
					return out, &FrameError{fmt.Sprintf("payload length %d is outside 1..%d", size, MaxPayloadSize)}
				}
				d.payload = make([]byte, size)
				d.payloadSize = 0
			}
		} else {
			n := copy(d.payload[d.payloadSize:], b[off:])
			d.payloadSize += n
			off += n
		}
		if d.payload != nil && d.payloadSize == len(d.payload) {
			out = append(out, d.payload)
			d.reset()
		}
	}
	return out, nil
}

// CheckDeadline fails once a partial frame's assembly deadline has elapsed.
func (d *Decoder) CheckDeadline(now time.Duration) error {
	if d.started && now-d.startedAt >= d.timeout {
		return &FrameError{"frame assembly deadline exceeded"}
	}
	return nil
}

// Finish validates EOF: any retained byte means the peer disconnected mid-frame.
func (d *Decoder) Finish() error {
	if d.Partial() {
		return &FrameError{"EOF in incomplete frame"}
	}
	return nil
}

// Partial reports whether a frame is partially assembled.
func (d *Decoder) Partial() bool { return d.headerSize != 0 || d.payload != nil }

func (d *Decoder) reset() {
	d.headerSize = 0
	d.payload = nil
	d.payloadSize = 0
	d.started = false
}

// EncodeFrame wraps a payload in the TCP envelope.
func EncodeFrame(payload []byte, marker byte) ([]byte, error) {
	if marker != ClientToCompanionMarker && marker != CompanionToClientMarker {
		return nil, fmt.Errorf("frame marker must be '<' or '>'")
	}
	n := len(payload)
	if n < 1 || n > MaxPayloadSize {
		return nil, fmt.Errorf("payload length %d is outside 1..%d", n, MaxPayloadSize)
	}
	frame := make([]byte, n+3)
	frame[0] = marker
	frame[1] = byte(n & 0xff)
	frame[2] = byte((n >> 8) & 0xff)
	copy(frame[3:], payload)
	return frame, nil
}

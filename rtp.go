package rtpaudio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// RTPVersion is the only accepted RTP version.
	RTPVersion = 2
	// FixedHeaderSize is the size of an RTP packet without CSRC or extension.
	FixedHeaderSize = 12
	// PayloadType is the only accepted dynamic payload type.
	PayloadType = 96
	// SamplesPerPacket is the fixed 20 ms frame size at 8 kHz.
	SamplesPerPacket = 160
	// ClockRate is the RTP timestamp clock rate.
	ClockRate = 8000
	// TimestampStep is added to the RTP timestamp every 20 ms frame.
	TimestampStep = SamplesPerPacket
)

// Packet is the decoded view of one fixed-header RTP packet.
type Packet struct {
	Version     uint8
	Marker      bool
	Extension   bool
	Padding     bool
	PayloadType uint8
	Sequence    uint16
	Timestamp   uint32
	SSRC        uint32
	Payload     []byte
}

// ParseRTP accepts only the exact profile used by this receiver:
// RTP version 2, no padding, no CSRC list, no extension, PT 96, and a
// 160-sample big-endian PCM16 payload.
func ParseRTP(data []byte) (*Packet, error) {
	if len(data) < FixedHeaderSize {
		return nil, fmt.Errorf("%w: packet is %d bytes, RTP header is %d",
			ErrMalformed, len(data), FixedHeaderSize)
	}

	version := data[0] >> 6
	if version != RTPVersion {
		return nil, fmt.Errorf("%w: RTP version is %d, want %d", ErrMalformed, version, RTPVersion)
	}
	padding := data[0]&0x20 != 0
	extension := data[0]&0x10 != 0
	csrcCount := data[0] & 0x0f
	marker := data[1]&0x80 != 0
	payloadType := data[1] & 0x7f

	if padding {
		return nil, fmt.Errorf("%w: padded RTP packets are not supported", ErrUnsupported)
	}
	if extension {
		return nil, fmt.Errorf("%w: RTP extension headers are not supported", ErrUnsupported)
	}
	if csrcCount != 0 {
		return nil, fmt.Errorf("%w: CSRC list is not supported", ErrUnsupported)
	}
	if payloadType != PayloadType {
		return nil, fmt.Errorf("%w: payload type is %d, want %d", ErrUnsupported, payloadType, PayloadType)
	}

	payload := data[FixedHeaderSize:]
	if len(payload) != SamplesPerPacket*2 {
		return nil, fmt.Errorf("%w: payload is %d bytes, want %d PCM16 samples",
			ErrMalformed, len(payload), SamplesPerPacket*2)
	}

	return &Packet{
		Version:     version,
		Marker:      marker,
		Extension:   extension,
		Padding:     padding,
		PayloadType: payloadType,
		Sequence:    binary.BigEndian.Uint16(data[2:4]),
		Timestamp:   binary.BigEndian.Uint32(data[4:8]),
		SSRC:        binary.BigEndian.Uint32(data[8:12]),
		Payload:     payload,
	}, nil
}

// DecodePCM16BE decodes the fixed-size big-endian PCM payload.
// The destination must have SamplesPerPacket entries.
func DecodePCM16BE(payload []byte, dst []int16) error {
	if len(payload) != SamplesPerPacket*2 {
		return fmt.Errorf("%w: payload is %d bytes, want %d",
			ErrMalformed, len(payload), SamplesPerPacket*2)
	}
	if len(dst) != SamplesPerPacket {
		return fmt.Errorf("internal error: PCM destination has %d samples, want %d",
			len(dst), SamplesPerPacket)
	}
	for i := 0; i < SamplesPerPacket; i++ {
		dst[i] = int16(binary.BigEndian.Uint16(payload[i*2:]))
	}
	return nil
}

var (
	// ErrMalformed describes an RTP packet that cannot be used because it
	// violates the packet format or fixed frame size.
	ErrMalformed = errors.New("malformed RTP packet")
	// ErrUnsupported describes an otherwise parseable RTP feature that this
	// receiver deliberately does not support.
	ErrUnsupported = errors.New("unsupported RTP feature")
)

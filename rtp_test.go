package rtpaudio_test

import (
	"encoding/binary"
	"testing"

	"rtpaudio"
)

func TestParseStrictRTPProfile(t *testing.T) {
	pkt := makeRTP(1, 160, testSSRC)

	parsed, err := rtpaudio.ParseRTP(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Version != 2 || parsed.PayloadType != rtpaudio.PayloadType {
		t.Fatalf("parsed packet = %+v", parsed)
	}
	if parsed.Extension || parsed.Padding {
		t.Fatal("fixed-header flags unexpectedly set")
	}

	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"wrong version", func(b []byte) { b[0] = 1 << 6 }},
		{"extension", func(b []byte) { b[0] |= 0x10 }},
		{"padding", func(b []byte) { b[0] |= 0x20 }},
		{"csrc", func(b []byte) { b[0] |= 1 }},
		{"payload type", func(b []byte) { b[1] = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := append([]byte(nil), pkt...)
			tc.mutate(bad)
			if _, err := rtpaudio.ParseRTP(bad); err == nil {
				t.Fatal("accepted invalid packet")
			}
		})
	}
	if _, err := rtpaudio.ParseRTP(pkt[:len(pkt)-2]); err == nil {
		t.Fatal("accepted truncated packet")
	}
}

func TestPCMIsBigEndianOnWireAndLittleEndianWAV(t *testing.T) {
	pkt := makeRTP(7, 1120, testSSRC)
	parsed, err := rtpaudio.ParseRTP(pkt)
	if err != nil {
		t.Fatal(err)
	}
	samples := make([]int16, rtpaudio.SamplesPerPacket)
	if err := rtpaudio.DecodePCM16BE(parsed.Payload, samples); err != nil {
		t.Fatal(err)
	}
	wantWire := uint16(expectedSample(7, 3))
	gotWire := binary.BigEndian.Uint16(parsed.Payload[3*2:])
	if gotWire != wantWire {
		t.Fatalf("wire sample = %d, want %d", gotWire, wantWire)
	}
	if samples[3] != expectedSample(7, 3) {
		t.Fatalf("decoded sample = %d", samples[3])
	}
}

package rtpaudio

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeReportTestRTP(seq uint16, ts, ssrc uint32) []byte {
	pkt := make([]byte, FixedHeaderSize+SamplesPerPacket*2)
	pkt[0] = 2 << 6
	pkt[1] = PayloadType
	binary.BigEndian.PutUint16(pkt[2:4], seq)
	binary.BigEndian.PutUint32(pkt[4:8], ts)
	binary.BigEndian.PutUint32(pkt[8:12], ssrc)
	for i := 0; i < SamplesPerPacket; i++ {
		binary.BigEndian.PutUint16(pkt[FixedHeaderSize+i*2:], uint16(seq)+uint16(i))
	}
	return pkt
}

func TestReportsAndWAVUseOutputRecords(t *testing.T) {
	start := time.Date(2026, 10, 3, 15, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(start)
	r := NewReceiver(clock, Config{QueueCapacity: 8, ReorderWindow: 8})
	key := SourceKey("198.51.100.9:7000")

	const ssrc uint32 = 0x12345678
	// Frames 0 and 2 arrive; frame 1 must become one zero-filled output.
	for _, n := range []int{0, 2} {
		data := makeReportTestRTP(uint16(n), uint32(n*SamplesPerPacket), ssrc)
		if status := r.HandlePacket(key, data, start); !status.Accepted {
			t.Fatalf("packet %d rejected: %s", n, status.Reason)
		}
	}
	clock.Advance(100 * time.Millisecond) // first deadline 60 ms + two intervals
	r.Pump()

	frames, err := r.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
	if !frames[1].Missing || frames[0].Missing || frames[2].Missing {
		t.Fatalf("missing flags = %v, %v, %v", frames[0].Missing, frames[1].Missing, frames[2].Missing)
	}

	report, err := r.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if report.MissingFrames != 1 || report.Missing[0].Sequence != 1 || report.Missing[0].Timestamp != SamplesPerPacket {
		t.Fatalf("report did not come from output records: %+v", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MissingReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Missing[0].Index != report.Missing[0].Index {
		t.Fatal("missing report is not JSON round-trip stable")
	}

	wav := WAVData(frames)
	if got := binary.LittleEndian.Uint32(wav[24:28]); got != ClockRate {
		t.Fatalf("WAV sample rate = %d", got)
	}
	if got := binary.LittleEndian.Uint16(wav[22:24]); got != 1 {
		t.Fatalf("WAV channels = %d", got)
	}
	if got := binary.LittleEndian.Uint16(wav[34:36]); got != 16 {
		t.Fatalf("WAV bits per sample = %d", got)
	}
	body := wav[44:]
	frameStart := SamplesPerPacket * 2
	for i := frameStart; i < frameStart+SamplesPerPacket*2; i += 2 {
		if binary.LittleEndian.Uint16(body[i:]) != 0 {
			t.Fatalf("missing output frame had nonzero WAV byte at %d", i)
		}
	}
}

func TestExportGenerationZeroResolvesSnapshotOnce(t *testing.T) {
	start := time.Date(2026, 10, 3, 18, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(start)
	r := NewReceiver(clock, Config{QueueCapacity: 8, ReorderWindow: 8})
	key := SourceKey("192.0.2.40:5000")

	for _, data := range [][]byte{
		makeReportTestRTP(0, 0, 1),
		makeReportTestRTP(2, 320, 1),
	} {
		if status := r.HandlePacket(key, data, start); !status.Accepted {
			t.Fatal(status.Reason)
		}
	}
	clock.Advance(100 * time.Millisecond)
	r.Pump()

	dir := t.TempDir()
	wavPath, reportPath, err := r.ExportGeneration(dir, key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(wavPath); got != "192.0.2.40_5000.gen1.wav" {
		t.Fatalf("WAV name = %q", got)
	}
	if got := filepath.Base(reportPath); got != "192.0.2.40_5000.gen1.missing.json" {
		t.Fatalf("report name = %q", got)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report MissingReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Generation != 1 || report.Frames != 3 || report.MissingFrames != 1 {
		t.Fatalf("exported report = %+v", report)
	}
}

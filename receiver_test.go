package rtpaudio_test

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"rtpaudio"
)

const testSSRC uint32 = 0x12345678

func makeRTP(seq uint16, ts uint32, ssrc uint32) []byte {
	pkt := make([]byte, rtpaudio.FixedHeaderSize+rtpaudio.SamplesPerPacket*2)
	pkt[0] = 2 << 6 // version 2, no padding/extension/CSRC
	pkt[1] = rtpaudio.PayloadType
	binary.BigEndian.PutUint16(pkt[2:4], seq)
	binary.BigEndian.PutUint32(pkt[4:8], ts)
	binary.BigEndian.PutUint32(pkt[8:12], ssrc)
	for i := 0; i < rtpaudio.SamplesPerPacket; i++ {
		v := int16(int(seq)*17 + i)
		binary.BigEndian.PutUint16(pkt[rtpaudio.FixedHeaderSize+i*2:], uint16(v))
	}
	return pkt
}

func expectedSample(seq uint64, i int) int16 {
	return int16(int(seq%65536)*17 + i)
}

func submit(t *testing.T, r *rtpaudio.Receiver, key rtpaudio.SourceKey, seq uint16, ts uint32, at time.Time) rtpaudio.PacketStatus {
	t.Helper()
	status := r.HandlePacket(key, makeRTP(seq, ts, testSSRC), at)
	if !status.Accepted {
		t.Fatalf("sequence %d was not accepted: %s: %v", seq, status.Reason, status.Err)
	}
	return status
}

func assertFrame(t *testing.T, frame rtpaudio.OutputFrame, seq, ts uint64, missing bool) {
	t.Helper()
	if frame.Sequence != seq || frame.Timestamp != ts || frame.Missing != missing {
		t.Fatalf("frame %d = seq %d/ts %d/missing %v, want seq %d/ts %d/missing %v",
			frame.Index, frame.Sequence, frame.Timestamp, frame.Missing, seq, ts, missing)
	}
	if missing {
		for i, v := range frame.Samples {
			if v != 0 {
				t.Fatalf("missing frame seq %d had nonzero sample %d at %d", seq, v, i)
			}
		}
		return
	}
	for i, v := range frame.Samples {
		want := expectedSample(seq, i)
		if v != want {
			t.Fatalf("frame seq %d sample %d = %d, want %d", seq, i, v, want)
		}
	}
}

func TestReorderDuplicateMissingAndLatePackets(t *testing.T) {
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{
		QueueCapacity: 8,
		ReorderWindow: 8,
	})
	key := rtpaudio.SourceKey("192.0.2.1:5000")

	submit(t, r, key, 0, 1000, start)
	r.Pump()

	clock.Advance(5 * time.Millisecond)
	submit(t, r, key, 2, 1320, clock.Now())
	submit(t, r, key, 3, 1480, clock.Now())
	submit(t, r, key, 2, 1320, clock.Now())
	r.Pump()
	info, err := r.Info(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.DuplicatePackets != 1 {
		t.Fatalf("duplicate packets = %d, want 1", info.DuplicatePackets)
	}

	// Sequence 1 is out of order but still well inside the reorder window.
	clock.Advance(45 * time.Millisecond) // 50 ms
	submit(t, r, key, 1, 1160, clock.Now())
	r.Pump()

	clock.Advance(10 * time.Millisecond) // 60 ms: frames 0,1,2 are due
	r.Pump()
	frames, err := r.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	assertFrame(t, frames[0], 0, 1000, false)

	clock.Advance(1 * time.Millisecond) // 61 ms
	r.HandlePacket(key, makeRTP(0, 1000, testSSRC), clock.Now())
	r.Pump()
	info, _ = r.Info(key, 0)
	if info.LatePackets != 1 || info.DuplicatePackets != 2 {
		t.Fatalf("late duplicate counters = %+v", info)
	}
	before := frames[0].Samples
	r.Pump()
	frames, _ = r.Frames(key, 0)
	if frames[0].Samples != before {
		t.Fatal("late packet rewrote an already emitted frame")
	}

	// Sequence 4 never arrives before its 140 ms playout deadline.
	clock.Advance(24 * time.Millisecond) // 85 ms: emit reordered sequence 1
	r.Pump()
	clock.Advance(40 * time.Millisecond) // 125 ms: emit sequences 2 and 3
	r.Pump()
	clock.Advance(20 * time.Millisecond) // 145 ms: zero-fill sequence 4
	r.Pump()
	r.HandlePacket(key, makeRTP(4, 1640, testSSRC), clock.Now())
	r.Pump()

	frames, _ = r.Frames(key, 0)
	if len(frames) != 5 {
		t.Fatalf("frames = %d, want 5", len(frames))
	}
	assertFrame(t, frames[4], 4, 1640, true)

	report, err := r.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if report.Frames != 5 || report.MissingFrames != 1 {
		t.Fatalf("report = %+v", report)
	}
	if len(report.Missing) != 1 || report.Missing[0].Sequence != 4 {
		t.Fatalf("missing report = %+v", report.Missing)
	}
	if len(report.LatePackets) != 2 {
		t.Fatalf("late evidence = %+v", report.LatePackets)
	}
	if !report.LatePackets[0].Duplicate || report.LatePackets[1].Duplicate {
		t.Fatalf("late duplicate flags = %+v", report.LatePackets)
	}

	wav := rtpaudio.WAVData(frames)
	if len(wav) != 44+5*rtpaudio.SamplesPerPacket*2 {
		t.Fatalf("WAV length = %d", len(wav))
	}
	if string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
		t.Fatal("WAV does not start with RIFF/WAVE")
	}
	// Check one received and one zero-filled sample in the little-endian body.
	offset := 44 + 4*rtpaudio.SamplesPerPacket*2
	if got := int16(binary.LittleEndian.Uint16(wav[offset:])); got != 0 {
		t.Fatalf("missing WAV sample = %d", got)
	}
	offset = 44 + int16Sample(0, 0)
	if got := int16(binary.LittleEndian.Uint16(wav[offset:])); got != expectedSample(0, 0) {
		t.Fatalf("WAV sample = %d", got)
	}
}

func int16Sample(frame, sample int) int {
	return frame*rtpaudio.SamplesPerPacket*2 + sample*2
}

func TestSequenceDoubleWrapAndTimestampWrap(t *testing.T) {
	// End-to-end sequence double wrap: 65534 -> ... -> 131073.
	start := time.Date(2026, 10, 3, 13, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{
		QueueCapacity: 64,
		ReorderWindow: 64,
	})
	key := rtpaudio.SourceKey("192.0.2.2:5000")
	const firstSeq uint64 = 65534
	submit(t, r, key, uint16(firstSeq), 0, start)
	r.Pump()

	const totalPackets = 65540 // crosses 65536 twice: ...65535, 0 and ...65535, 0.
	for j := 1; j < totalPackets; j++ {
		clock.Advance(20 * time.Millisecond)
		ext := firstSeq + uint64(j)
		submit(t, r, key, uint16(ext), uint32((ext-firstSeq)*160), clock.Now())
		r.Pump()
	}
	clock.Advance(60 * time.Millisecond)
	r.Pump()

	frames, err := r.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != totalPackets {
		t.Fatalf("frames = %d, want %d", len(frames), totalPackets)
	}
	for i, frame := range frames {
		assertFrame(t, frame, firstSeq+uint64(i), uint64(i)*160, false)
	}
	if frames[65537].Sequence != 131071 {
		t.Fatalf("second wrap boundary = %d", frames[65537].Sequence)
	}
	report, _ := r.MissingReportFor(key, 0)
	if report.MissingFrames != 0 || len(report.LatePackets) != 0 {
		t.Fatalf("double-wrap report unexpectedly had gaps: %+v", report)
	}

	// The equivalent 32-bit RTP timestamp transitions, including the second
	// wrap, are tested directly in wrap_test.go. An end-to-end stream would
	// require more than 2^33 audio clock ticks.
}

func TestStopRejectsLateDataAndRestartUsesNewGeneration(t *testing.T) {
	start := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 8, ReorderWindow: 8})
	key := rtpaudio.SourceKey("192.0.2.3:5000")

	submit(t, r, key, 0, 0, start)
	clock.Advance(120 * time.Millisecond)
	r.Pump()
	if err := r.StopSource(key, clock.Now()); err != nil {
		t.Fatal(err)
	}
	framesBefore, _ := r.Frames(key, 1)
	if len(framesBefore) != 4 {
		t.Fatalf("frames at stop = %d, want 4", len(framesBefore))
	}

	stopped := r.HandlePacket(key, makeRTP(4, 640, testSSRC), clock.Now())
	if stopped.Reason != rtpaudio.ReasonStopped {
		t.Fatalf("stopped packet reason = %q", stopped.Reason)
	}
	framesAfter, _ := r.Frames(key, 1)
	if len(framesAfter) != 4 {
		t.Fatal("packet after stop altered finalized generation")
	}

	gen, err := r.RestartSource(key, clock.Now())
	if err != nil || gen != 2 {
		t.Fatalf("new generation = %d, %v", gen, err)
	}
	submit(t, r, key, 500, 9000, clock.Now())
	clock.Advance(60 * time.Millisecond)
	r.Pump()
	newFrames, err := r.Frames(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(newFrames) != 1 || newFrames[0].Sequence != 500 || newFrames[0].Timestamp != 9000 {
		t.Fatalf("new generation frames = %+v", newFrames)
	}

	oldFrames, err := r.Frames(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldFrames) != 4 {
		t.Fatalf("old generation frames = %d, want 4", len(oldFrames))
	}

	dir := t.TempDir()
	wav1, report1, err := r.ExportGeneration(dir, key, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, report2, err := r.ExportGeneration(dir, key, 2)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(wav1)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 44+4*rtpaudio.SamplesPerPacket*2 {
		t.Fatalf("exported WAV size = %d", len(data))
	}
	var report rtpaudio.MissingReport
	rb, err := os.ReadFile(report1)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rb, &report); err != nil {
		t.Fatal(err)
	}
	if report.Generation != 1 || len(report.LatePackets) != 1 || report.LatePackets[0].Sequence != 4 {
		t.Fatalf("old generated report = %+v", report)
	}
	if filepath.Dir(report2) != dir {
		t.Fatal("second report was not generated in requested directory")
	}
}

func TestBoundedReceiveQueue(t *testing.T) {
	clock := rtpaudio.NewVirtualClock(time.Unix(0, 0))
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{
		QueueCapacity: 2,
		ReorderWindow: 8,
	})
	key := rtpaudio.SourceKey("192.0.2.4:5000")
	for seq := uint16(0); seq < 2; seq++ {
		submit(t, r, key, seq, uint32(seq)*160, clock.Now())
	}
	status := r.HandlePacket(key, makeRTP(2, 320, testSSRC), clock.Now())
	if status.Reason != rtpaudio.ReasonQueueFull {
		t.Fatalf("third packet reason = %q, want queue full", status.Reason)
	}
	n, err := r.QueueLen(key)
	if err != nil || n != 2 {
		t.Fatalf("queue length = %d, %v", n, err)
	}
	capacity, err := r.QueueCapacity(key)
	if err != nil || capacity != 2 {
		t.Fatalf("queue capacity = %d, %v", capacity, err)
	}

	// Pumping drains the bounded queue and makes room again.
	r.Pump()
	n, _ = r.QueueLen(key)
	if n != 0 {
		t.Fatalf("queue after pump = %d, want 0", n)
	}
	submit(t, r, key, 2, 320, clock.Now())
}

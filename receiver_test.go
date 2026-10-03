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
	if report.Generation != 1 || len(report.LatePackets) != 0 {
		t.Fatalf("old generated report = %+v", report)
	}
	if filepath.Dir(report2) != dir {
		t.Fatal("second report was not generated in requested directory")
	}
}

func TestExactDeadlineIsOnTimeLaterArrivalIsLate(t *testing.T) {
	start := time.Date(2026, 10, 3, 15, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 8, ReorderWindow: 8})
	key := rtpaudio.SourceKey("192.0.2.10:5000")

	submit(t, r, key, 0, 0, start)
	if n := r.Pump(); n != 0 {
		t.Fatalf("pump before first deadline returned %d frames, want 0", n)
	}

	clock.Advance(80 * time.Millisecond)
	submit(t, r, key, 1, 160, clock.Now())
	// This packet has an actual arrival after sequence 2's deadline. It must
	// remain queued while the frame is zero-filled at 100 ms.
	future := start.Add(101 * time.Millisecond)
	submit(t, r, key, 2, 320, future)
	if n := r.Pump(); n != 2 {
		t.Fatalf("pump at exact deadline returned %d frames, want 2", n)
	}
	frames, _ := r.Frames(key, 0)
	if len(frames) != 2 || frames[1].Missing {
		t.Fatalf("sequence arriving exactly on deadline was not played: %+v", frames)
	}

	clock.Advance(20 * time.Millisecond)
	if n := r.Pump(); n != 1 {
		t.Fatalf("pump at missing-frame deadline returned %d frames, want 1", n)
	}
	frames, _ = r.Frames(key, 0)
	if len(frames) != 3 || !frames[2].Missing {
		t.Fatalf("future packet prevented silence: %+v", frames)
	}

	clock.Advance(1 * time.Millisecond)
	if n := r.Pump(); n != 0 {
		t.Fatalf("late packet caused %d new output frames", n)
	}
	late, err := r.LatePackets(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 1 || late[0].Sequence != 2 || late[0].Duplicate {
		t.Fatalf("late evidence = %+v", late)
	}
	if !late[0].ScheduledOutput.Equal(start.Add(100*time.Millisecond)) ||
		!late[0].Arrival.Equal(start.Add(101*time.Millisecond)) {
		t.Fatalf("late evidence timing = %+v", late[0])
	}
	frames, _ = r.Frames(key, 0)
	if !frames[2].Missing {
		t.Fatal("late packet rewrote an emitted silent frame")
	}

	clock.Advance(1 * time.Millisecond)
	submit(t, r, key, 2, 320, clock.Now())
	r.Pump()
	late, _ = r.LatePackets(key, 0)
	if len(late) != 2 || !late[1].Duplicate {
		t.Fatalf("second late copy evidence = %+v", late)
	}
}

func TestIncrementalAndBulkAdvanceProduceSameOutput(t *testing.T) {
	type event struct {
		seq uint16
		ts  uint32
		at  time.Duration
	}
	start := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC)
	events := []event{
		{0, 0, 0},
		{2, 320, 50 * time.Millisecond},
		{3, 480, 51 * time.Millisecond},
		{2, 320, 52 * time.Millisecond}, // duplicate before sequence 2's deadline
		{1, 160, 61 * time.Millisecond},
		{0, 0, 101 * time.Millisecond}, // duplicate after sequence 0's deadline
		{5, 800, 150 * time.Millisecond},
	}
	final := start.Add(160 * time.Millisecond)
	key := rtpaudio.SourceKey("192.0.2.20:5000")

	stepClock := rtpaudio.NewVirtualClock(start)
	step := rtpaudio.NewReceiver(stepClock, rtpaudio.Config{QueueCapacity: 16, ReorderWindow: 8})
	stepFrames := 0
	for _, e := range events {
		stepClock.Advance(start.Add(e.at).Sub(stepClock.Now()))
		submit(t, step, key, e.seq, e.ts, stepClock.Now())
		stepFrames += step.Pump()
	}
	stepClock.Advance(final.Sub(stepClock.Now()))
	stepFrames += step.Pump()

	bulkClock := rtpaudio.NewVirtualClock(start)
	bulk := rtpaudio.NewReceiver(bulkClock, rtpaudio.Config{QueueCapacity: 16, ReorderWindow: 8})
	for _, e := range events {
		submit(t, bulk, key, e.seq, e.ts, start.Add(e.at))
	}
	bulkClock.Advance(160 * time.Millisecond)
	bulkFrames := bulk.Pump()

	if stepFrames != bulkFrames || bulkFrames != 6 {
		t.Fatalf("emitted frame counts step=%d bulk=%d, want 6", stepFrames, bulkFrames)
	}
	sf, err := step.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	bf, err := bulk.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(rtpaudio.WAVData(sf)) != string(rtpaudio.WAVData(bf)) {
		t.Fatal("incremental and bulk WAV outputs differ")
	}
	sr, err := step.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	br, err := bulk.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	sj, _ := json.Marshal(sr)
	bj, _ := json.Marshal(br)
	if string(sj) != string(bj) {
		t.Fatalf("incremental and bulk reports differ:\n%s\n%s", sj, bj)
	}
	if sr.Frames != 6 || sr.MissingFrames != 1 || sr.Missing[0].Sequence != 4 || len(sr.LatePackets) != 1 {
		t.Fatalf("unexpected report = %+v", sr)
	}
	if sr.LatePackets[0].Sequence != 0 || !sr.LatePackets[0].Duplicate {
		t.Fatalf("unexpected late evidence = %+v", sr.LatePackets)
	}
}

func TestRestartRejectsPreStartAndInvalidEvidence(t *testing.T) {
	start := time.Date(2026, 10, 3, 17, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 8, ReorderWindow: 8})
	key := rtpaudio.SourceKey("192.0.2.30:5000")

	submit(t, r, key, 0, 0, start)
	clock.Advance(80 * time.Millisecond)
	r.Pump()
	if _, err := r.RestartSource(key, clock.Now()); err != nil {
		t.Fatal(err)
	}

	old := r.HandlePacket(key, makeRTP(1, 160, testSSRC), clock.Now().Add(-time.Nanosecond))
	if old.Reason != rtpaudio.ReasonBeforeStart {
		t.Fatalf("pre-start packet reason = %q", old.Reason)
	}
	submit(t, r, key, 100, 10000, clock.Now())
	clock.Advance(time.Nanosecond)
	bad := r.HandlePacket(key, makeRTP(1, 999, testSSRC), clock.Now())
	if !bad.Accepted {
		t.Fatalf("invalid packet was rejected at enqueue: %q", bad.Reason)
	}
	r.Pump()

	clock.Advance(60 * time.Millisecond)
	r.Pump()
	report, err := r.MissingReportFor(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.Frames != 1 || report.MissingFrames != 0 || len(report.LatePackets) != 0 {
		t.Fatalf("new generation was contaminated: %+v", report)
	}
	info, err := r.Info(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if info.InvalidPackets != 2 {
		t.Fatalf("invalid packets = %d, want 2", info.InvalidPackets)
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

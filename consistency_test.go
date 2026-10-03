package rtpaudio_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"rtpaudio"
)

// scriptedPacket is one RTP packet and the arrival time stamped on it.
type scriptedPacket struct {
	seq     uint16
	ts      uint32
	arrival time.Duration
}

// pumpScript feeds the same arrival-stamped packets two ways: incrementally
// (handle each packet at its arrival time and pump) and in bulk (handle all
// packets up front, then advance and pump once). Output must be identical
// because disposition is decided by arrival time and the playout schedule.
func pumpScript(t *testing.T, key rtpaudio.SourceKey, start time.Time, script []scriptedPacket, final time.Duration, incremental bool) (*rtpaudio.Receiver, int) {
	t.Helper()
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 32, ReorderWindow: 8})
	produced := 0
	if incremental {
		var elapsed time.Duration
		for _, p := range script {
			clock.Advance(p.arrival - elapsed)
			elapsed = p.arrival
			if st := r.HandlePacket(key, makeRTP(p.seq, p.ts, testSSRC), clock.Now()); !st.Accepted {
				t.Fatalf("packet seq %d rejected: %s", p.seq, st.Reason)
			}
			produced += r.Pump()
		}
		clock.Advance(final - elapsed)
		produced += r.Pump()
		return r, produced
	}
	for _, p := range script {
		if st := r.HandlePacket(key, makeRTP(p.seq, p.ts, testSSRC), start.Add(p.arrival)); !st.Accepted {
			t.Fatalf("packet seq %d rejected: %s", p.seq, st.Reason)
		}
	}
	clock.Advance(final)
	produced += r.Pump()
	return r, produced
}

func TestIncrementalAndBulkPumpingProduceIdenticalOutput(t *testing.T) {
	start := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC)
	key := rtpaudio.SourceKey("203.0.113.7:6000")

	// Frame i has extended sequence 65533+i and extended timestamp
	// 0xFFFFFE00+160i; its playout deadline is start+60ms+i*20ms. The script
	// crosses both the 16-bit sequence and 32-bit timestamp wrap boundaries
	// and mixes reordering, duplicates, an invalid timestamp, a packet
	// predating the capture, and genuinely late packets.
	script := []scriptedPacket{
		{65533, 0xFFFFFE00, 0},
		{65534, 0xFFFFFEA0, 15 * time.Millisecond},
		{65535, 0xFFFFFF40, 50 * time.Millisecond},
		{0, 0xFFFFFFE0, 70 * time.Millisecond},
		{1, 0x00000080, 90 * time.Millisecond},
		{0, 0xFFFFFFE0, 100 * time.Millisecond}, // on-time duplicate
		{3, 0x000001C0, 150 * time.Millisecond},
		{4, 0xDEADBEEF, 160 * time.Millisecond}, // invalid timestamp
		{4, 0x00000260, 165 * time.Millisecond},
		{65532, 0xFFFFFDA0, 170 * time.Millisecond}, // predates capture start
		{5, 0x00000300, 185 * time.Millisecond},
		{2, 0x00000120, 200 * time.Millisecond}, // late: deadline was 160ms
		{1, 0x00000080, 250 * time.Millisecond}, // late duplicate
	}
	const final = 260 * time.Millisecond

	inc, incProduced := pumpScript(t, key, start, script, final, true)
	bulk, bulkProduced := pumpScript(t, key, start, script, final, false)

	incFrames, err := inc.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	bulkFrames, err := bulk.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(incFrames, bulkFrames) {
		t.Fatalf("incremental and bulk frames differ:\nincremental: %+v\nbulk: %+v", incFrames, bulkFrames)
	}
	incLate, err := inc.LatePackets(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	bulkLate, err := bulk.LatePackets(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(incLate, bulkLate) {
		t.Fatalf("late evidence differs:\nincremental: %+v\nbulk: %+v", incLate, bulkLate)
	}
	incReport, err := inc.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	bulkReport, err := bulk.MissingReportFor(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(incReport, bulkReport) {
		t.Fatalf("reports differ:\nincremental: %+v\nbulk: %+v", incReport, bulkReport)
	}
	if !bytes.Equal(rtpaudio.WAVData(incFrames), rtpaudio.WAVData(bulkFrames)) {
		t.Fatal("WAV bytes differ between pumping strategies")
	}
	if incProduced != bulkProduced || incProduced != len(incFrames) {
		t.Fatalf("pump produced %d incrementally, %d in bulk, %d frames recorded",
			incProduced, bulkProduced, len(incFrames))
	}

	// Explicit expectations for the shared outcome: 11 frames due by 260ms;
	// frame 5 arrived past its 160ms deadline and frames 9,10 never arrived.
	if len(incFrames) != 11 {
		t.Fatalf("frames = %d, want 11", len(incFrames))
	}
	for i, frame := range incFrames {
		wantSeq := uint64(65533 + i)
		wantTS := uint64(0xFFFFFE00) + uint64(i)*rtpaudio.SamplesPerPacket
		wantAt := start.Add(60*time.Millisecond + time.Duration(i)*20*time.Millisecond)
		if frame.Sequence != wantSeq || frame.Timestamp != wantTS || !frame.At.Equal(wantAt) {
			t.Fatalf("frame %d = seq %d ts %d at %s, want seq %d ts %d at %s",
				i, frame.Sequence, frame.Timestamp, frame.At, wantSeq, wantTS, wantAt)
		}
		wantMissing := i == 5 || i == 9 || i == 10
		if frame.Missing != wantMissing {
			t.Fatalf("frame %d missing = %v, want %v", i, frame.Missing, wantMissing)
		}
		if frame.Missing {
			for j, v := range frame.Samples {
				if v != 0 {
					t.Fatalf("missing frame %d had nonzero sample %d at %d", i, v, j)
				}
			}
		}
	}
	// The late packet for frame 5 must not have rewritten the emitted
	// silence, and the on-time packets must have been played.
	for i, v := range incFrames[0].Samples {
		if want := expectedSample(65533, i); v != want {
			t.Fatalf("frame 0 sample %d = %d, want %d", i, v, want)
		}
	}

	// Late evidence holds exactly the two overdue packets, with validated
	// timestamps; the invalid-timestamp and pre-capture packets are absent.
	wantLate := []rtpaudio.LateEvidence{
		{Generation: 1, Sequence: 65538, Timestamp: 0x100000120,
			Arrival: start.Add(200 * time.Millisecond), ScheduledOutput: start.Add(160 * time.Millisecond)},
		{Generation: 1, Sequence: 65537, Timestamp: 0x100000080,
			Arrival: start.Add(250 * time.Millisecond), ScheduledOutput: start.Add(140 * time.Millisecond),
			Duplicate: true},
	}
	if !reflect.DeepEqual(incLate, wantLate) {
		t.Fatalf("late evidence = %+v, want %+v", incLate, wantLate)
	}

	info, err := inc.Info(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if info.Frames != 11 || info.MissingFrames != 3 || info.InvalidPackets != 2 ||
		info.DuplicatePackets != 2 || info.LatePackets != 2 {
		t.Fatalf("counters = %+v", info)
	}

	if incReport.MissingFrames != 3 || len(incReport.Missing) != 3 {
		t.Fatalf("report missing = %+v", incReport)
	}
	wantMissingSeq := []uint64{65538, 65542, 65543}
	wantMissingTS := []uint64{0x100000120, 0x1000003A0, 0x100000440}
	for i, mf := range incReport.Missing {
		if mf.Sequence != wantMissingSeq[i] || mf.Timestamp != wantMissingTS[i] {
			t.Fatalf("missing[%d] = seq %d ts %d, want seq %d ts %d",
				i, mf.Sequence, mf.Timestamp, wantMissingSeq[i], wantMissingTS[i])
		}
	}
}

func TestArrivalExactlyAtDeadlinePlays(t *testing.T) {
	start := time.Date(2026, 10, 3, 17, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 8, ReorderWindow: 8})
	key := rtpaudio.SourceKey("203.0.113.8:6000")

	// Frame deadlines: seq 100 at 60ms, seq 101 at 80ms, seq 102 at 100ms.
	r.HandlePacket(key, makeRTP(100, 5000, testSSRC), start)
	r.HandlePacket(key, makeRTP(101, 5160, testSSRC), start.Add(80*time.Millisecond))  // exactly at deadline
	r.HandlePacket(key, makeRTP(102, 5320, testSSRC), start.Add(101*time.Millisecond)) // 1ms past deadline
	clock.Advance(100 * time.Millisecond)
	if produced := r.Pump(); produced != 3 {
		t.Fatalf("produced = %d, want 3", produced)
	}

	frames, err := r.Frames(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
	if frames[1].Missing {
		t.Fatal("packet arriving exactly at its deadline was not played")
	}
	if !frames[2].Missing {
		t.Fatal("packet arriving after its deadline rewrote the emitted frame")
	}

	late, err := r.LatePackets(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 1 || late[0].Sequence != 102 ||
		!late[0].Arrival.Equal(start.Add(101*time.Millisecond)) ||
		!late[0].ScheduledOutput.Equal(start.Add(100*time.Millisecond)) {
		t.Fatalf("late evidence = %+v", late)
	}
}

func TestStoppedSourceValidatesLateEvidence(t *testing.T) {
	start := time.Date(2026, 10, 3, 18, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 8, ReorderWindow: 8})
	key := rtpaudio.SourceKey("203.0.113.9:6000")

	r.HandlePacket(key, makeRTP(100, 1000, testSSRC), start)
	clock.Advance(100 * time.Millisecond)
	r.Pump() // frames seq 100,101,102; 101 and 102 are zero-filled
	if err := r.StopSource(key, clock.Now()); err != nil {
		t.Fatal(err)
	}

	r.HandlePacket(key, makeRTP(103, 0xDEADBEEF, testSSRC), clock.Now()) // invalid timestamp
	r.HandlePacket(key, makeRTP(99, 840, testSSRC), clock.Now())         // predates capture start
	r.HandlePacket(key, makeRTP(102, 1320, testSSRC), clock.Now())       // valid, late
	r.HandlePacket(key, makeRTP(100, 1000, testSSRC), clock.Now())       // duplicate of played frame

	info, err := r.Info(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if info.StoppedDrops != 4 || info.InvalidPackets != 2 || info.LatePackets != 2 || info.DuplicatePackets != 1 {
		t.Fatalf("counters = %+v", info)
	}
	late, err := r.LatePackets(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(late) != 2 {
		t.Fatalf("late evidence = %+v", late)
	}
	if late[0].Sequence != 102 || late[0].Timestamp != 1320 || late[0].Duplicate {
		t.Fatalf("late[0] = %+v", late[0])
	}
	if late[1].Sequence != 100 || late[1].Timestamp != 1000 || !late[1].Duplicate {
		t.Fatalf("late[1] = %+v", late[1])
	}
}

func TestExportCurrentGenerationResolvesActualID(t *testing.T) {
	start := time.Date(2026, 10, 3, 19, 0, 0, 0, time.UTC)
	clock := rtpaudio.NewVirtualClock(start)
	r := rtpaudio.NewReceiver(clock, rtpaudio.Config{QueueCapacity: 4, ReorderWindow: 4})
	key := rtpaudio.SourceKey("192.0.2.44:5000")

	r.HandlePacket(key, makeRTP(10, 1000, testSSRC), start)
	clock.Advance(60 * time.Millisecond)
	r.Pump() // generation 1: one frame

	gen, err := r.RestartSource(key, clock.Now())
	if err != nil || gen != 2 {
		t.Fatalf("restart = generation %d, %v", gen, err)
	}
	r.HandlePacket(key, makeRTP(700, 9000, 0x55555555), clock.Now())
	clock.Advance(60 * time.Millisecond)
	r.Pump() // generation 2: one frame

	dir := t.TempDir()
	wavPath, reportPath, err := r.ExportGeneration(dir, key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(filepath.Base(wavPath), ".gen2.wav") {
		t.Fatalf("WAV path %q does not name resolved generation 2", wavPath)
	}
	if !strings.HasSuffix(filepath.Base(reportPath), ".gen2.missing.json") {
		t.Fatalf("report path %q does not name resolved generation 2", reportPath)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report rtpaudio.MissingReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Generation != 2 || report.Frames != 1 {
		t.Fatalf("report content = generation %d, %d frames", report.Generation, report.Frames)
	}
	wav, err := os.ReadFile(wavPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(wav) != 44+rtpaudio.SamplesPerPacket*2 {
		t.Fatalf("exported WAV size = %d", len(wav))
	}

	// An explicit generation export still names that generation.
	wav1, report1, err := r.ExportGeneration(dir, key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(filepath.Base(wav1), ".gen1.wav") ||
		!strings.HasSuffix(filepath.Base(report1), ".gen1.missing.json") {
		t.Fatalf("generation 1 paths = %q, %q", wav1, report1)
	}
	data, err = os.ReadFile(report1)
	if err != nil {
		t.Fatal(err)
	}
	var reportOne rtpaudio.MissingReport
	if err := json.Unmarshal(data, &reportOne); err != nil {
		t.Fatal(err)
	}
	if reportOne.Generation != 1 {
		t.Fatalf("generation 1 report content = %d", reportOne.Generation)
	}
}

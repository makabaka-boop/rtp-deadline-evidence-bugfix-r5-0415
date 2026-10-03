package rtpaudio

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// FrameDuration is the output interval.
	FrameDuration = 20 * time.Millisecond
	// StartDelay is the controlled-clock delay from a source's first packet
	// to its first playout.
	StartDelay = 60 * time.Millisecond

	defaultQueueCapacity = 128
	defaultWindow        = 32
)

// DropReason explains why a packet never entered a generation's jitter buffer.
type DropReason string

const (
	ReasonMalformed     DropReason = "malformed"
	ReasonUnsupported   DropReason = "unsupported"
	ReasonWrongSSRC     DropReason = "wrong_ssrc"
	ReasonQueueFull     DropReason = "receive_queue_full"
	ReasonStopped       DropReason = "source_stopped"
	ReasonDuplicate     DropReason = "duplicate"
	ReasonLateDuplicate DropReason = "late_duplicate"
	ReasonLate          DropReason = "late"
	ReasonOutsideWindow DropReason = "outside_reorder_window"
	ReasonBadTimestamp  DropReason = "bad_timestamp"
)

// Config controls receiver buffering. All zero values are replaced with safe
// defaults.
type Config struct {
	// QueueCapacity is the hard, per-source inbound UDP packet limit.
	QueueCapacity int
	// ReorderWindow is the number of frames from the playout position in
	// which an out-of-order packet is accepted.
	ReorderWindow uint64
}

func (c Config) withDefaults() Config {
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = defaultQueueCapacity
	}
	if c.ReorderWindow == 0 {
		c.ReorderWindow = defaultWindow
	}
	return c
}

// PacketStatus reports the disposition of one UDP payload.
type PacketStatus struct {
	Accepted   bool
	Source     SourceKey
	Generation uint64
	Sequence   uint16
	Reason     DropReason
	Err        error
}

// SourceKey identifies a source. In UDP reception it is the remote
// address. RTP SSRC is validated within a capture generation but is not used
// to merge different UDP endpoints.
type SourceKey string

// OutputFrame is one immutable record produced by the controlled playout
// clock. These records are the sole source of WAV and missing-report data.
type OutputFrame struct {
	Generation uint64
	Index      uint64
	Sequence   uint64
	Timestamp  uint64
	At         time.Time
	Missing    bool
	Samples    [SamplesPerPacket]int16
}

// LateEvidence records a packet that arrived after its frame had already been
// emitted. It is evidence only and never causes previously emitted samples to
// be rewritten.
type LateEvidence struct {
	Generation      uint64
	Sequence        uint64
	Timestamp       uint64
	Arrival         time.Time
	ScheduledOutput time.Time
	Duplicate       bool
}

// MissingFrame identifies a zero-filled frame in generation output order.
type MissingFrame struct {
	Index     uint64    `json:"index"`
	Sequence  uint64    `json:"sequence"`
	Timestamp uint64    `json:"timestamp"`
	Scheduled time.Time `json:"scheduled_at"`
}

// MissingReport is produced from OutputFrame records, not from the live jitter
// buffer.
type MissingReport struct {
	Source           SourceKey      `json:"source"`
	Generation       uint64         `json:"generation"`
	ClockRate        int            `json:"clock_rate"`
	SamplesPerPacket int            `json:"samples_per_packet"`
	FrameDurationMS  int64          `json:"frame_duration_ms"`
	Frames           int            `json:"frames"`
	MissingFrames    int            `json:"missing_frames"`
	Missing          []MissingFrame `json:"missing"`
	LatePackets      []LateEvidence `json:"late_packets"`
}

// GenerationInfo describes one capture generation.
type GenerationInfo struct {
	Generation       uint64
	Active           bool
	SSRC             uint32
	FirstArrival     time.Time
	FirstSequence    uint64
	FirstTimestamp   uint64
	Frames           int
	MissingFrames    int
	QueueFullDrops   uint64
	StoppedDrops     uint64
	InvalidPackets   uint64
	DuplicatePackets uint64
	LatePackets      uint64
}

type bufferedPacket struct {
	samples [SamplesPerPacket]int16
	ts      uint64
	arrival time.Time
}

type generation struct {
	id     uint64
	active bool
	ssrc   uint32

	started bool
	ssrcSet bool

	firstArrival   time.Time
	firstSequence  uint64
	firstTimestamp uint64

	// nextSequence/nextTimestamp identify the next frame to emit. Timestamp
	// values are extended to uint64; sequence values are extended modulo
	// 2^16.
	nextSequence  uint64
	nextTimestamp uint64
	highestSeq    uint64
	highestTS     uint64

	buffer            map[uint64]*bufferedPacket
	receivedSequences map[uint64]bool
	frames            []OutputFrame
	late              []LateEvidence

	queueFullDrops   uint64
	stoppedDrops     uint64
	invalidPackets   uint64
	duplicatePackets uint64
	latePackets      uint64
}

type queuedPacket struct {
	packet  *Packet
	arrival time.Time
}

type source struct {
	key     SourceKey
	queue   chan *queuedPacket
	config  Config
	current *generation
	past    []*generation
}

// Receiver is concurrency-safe. It contains no receive or timer goroutines by
// itself; use Run for real time and Pump with VirtualClock for tests.
type Receiver struct {
	clock Clock
	cfg   Config

	mu      sync.Mutex
	sources map[SourceKey]*source
	wake    chan struct{}

	invalidPackets uint64
}

// NewReceiver creates a receiver. A nil clock uses the wall clock.
func NewReceiver(clock Clock, cfg Config) *Receiver {
	cfg = cfg.withDefaults()
	if clock == nil {
		clock = NewRealClock()
	}
	return &Receiver{
		clock:   clock,
		cfg:     cfg,
		sources: make(map[SourceKey]*source),
		wake:    make(chan struct{}, 1),
	}
}

// HandlePacket validates and enqueues one UDP payload. The receiver copies the
// RTP payload; callers retain ownership of data.
func (r *Receiver) HandlePacket(key SourceKey, data []byte, arrival time.Time) PacketStatus {
	pkt, err := ParseRTP(data)
	if err != nil {
		reason := ReasonMalformed
		if errors.Is(err, ErrUnsupported) {
			reason = ReasonUnsupported
		}
		r.mu.Lock()
		r.invalidPackets++
		if s := r.sources[key]; s != nil {
			s.current.invalidPackets++
		}
		r.mu.Unlock()
		return PacketStatus{Source: key, Reason: reason, Err: err}
	}

	// Copy because UDP read buffers are commonly reused by the caller.
	payload := make([]byte, len(pkt.Payload))
	copy(payload, pkt.Payload)
	pkt.Payload = payload
	if arrival.IsZero() {
		arrival = r.clock.Now()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	s := r.sources[key]
	if s == nil {
		s = newSource(key, r.cfg)
		r.sources[key] = s
		s.current.ssrc = pkt.SSRC
		s.current.ssrcSet = true
		// A long-lived Run loop may be sleeping because there were no
		// active sources.
		select {
		case r.wake <- struct{}{}:
		default:
		}
	} else if s.current.ssrcSet && s.current.ssrc != pkt.SSRC {
		s.current.invalidPackets++
		return PacketStatus{
			Source:     key,
			Generation: s.current.id,
			Sequence:   pkt.Sequence,
			Reason:     ReasonWrongSSRC,
			Err:        fmt.Errorf("%w: got SSRC %d, generation uses %d", ErrWrongSSRC, pkt.SSRC, s.current.ssrc),
		}
	}

	if !s.current.active {
		s.current.stoppedDrops++
		s.recordStoppedLate(pkt, arrival)
		return PacketStatus{
			Source:     key,
			Generation: s.current.id,
			Sequence:   pkt.Sequence,
			Reason:     ReasonStopped,
			Err:        ErrSourceStopped,
		}
	}

	select {
	case s.queue <- &queuedPacket{packet: pkt, arrival: arrival}:
		return PacketStatus{
			Accepted:   true,
			Source:     key,
			Generation: s.current.id,
			Sequence:   pkt.Sequence,
		}
	default:
		s.current.queueFullDrops++
		return PacketStatus{
			Source:     key,
			Generation: s.current.id,
			Sequence:   pkt.Sequence,
			Reason:     ReasonQueueFull,
			Err:        ErrQueueFull,
		}
	}
}

func newSource(key SourceKey, cfg Config) *source {
	return &source{
		key:    key,
		config: cfg,
		queue:  make(chan *queuedPacket, cfg.QueueCapacity),
		current: &generation{
			id:                1,
			active:            true,
			buffer:            make(map[uint64]*bufferedPacket),
			receivedSequences: make(map[uint64]bool),
		},
	}
}

// Pump drains bounded inbound queues and emits all frames due at the current
// controlled time. It is safe to call concurrently but is normally driven by
// Run or a virtual-clock test.
func (r *Receiver) Pump() int {
	now := r.clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()

	produced := 0
	for _, s := range r.sources {
		produced += r.drainSource(s, now)
	}
	return produced
}

func (r *Receiver) drainSource(s *source, now time.Time) int {
	for {
		select {
		case qp := <-s.queue:
			r.processPacket(s, qp.packet, qp.arrival)
		default:
			return s.current.emitDue(now)
		}
	}
}

func (r *Receiver) processPacket(s *source, pkt *Packet, arrival time.Time) {
	g := s.current
	// Restart/stop operations drain the queue, so an unexpected generation
	// cannot reach here. Defensive handling keeps a future direct caller from
	// mixing captures.
	if !g.active {
		g.stoppedDrops++
		s.recordStoppedLate(pkt, arrival)
		return
	}

	if !g.started {
		g.started = true
		g.firstArrival = arrival
		g.firstSequence = uint64(pkt.Sequence)
		g.firstTimestamp = uint64(pkt.Timestamp)
		g.nextSequence = uint64(pkt.Sequence)
		g.nextTimestamp = uint64(pkt.Timestamp)
		g.highestSeq = uint64(pkt.Sequence)
		g.highestTS = uint64(pkt.Timestamp)
		g.ssrc = pkt.SSRC
		g.ssrcSet = true
		insertPCM(g, pkt, arrival)
		return
	}

	extendedSeq, err := extendWrapped(g.highestSeq, uint64(pkt.Sequence), 16)
	if err != nil {
		g.invalidPackets++
		return
	}
	if extendedSeq < g.nextSequence {
		g.latePackets++
		// It is a content duplicate only when that sequence was received
		// before output. A frame emitted as zero-silence can still have a
		// late copy arrive afterwards.
		duplicate := g.receivedSequences[extendedSeq]
		if duplicate {
			g.duplicatePackets++
		}
		g.late = append(g.late, LateEvidence{
			Generation:      g.id,
			Sequence:        extendedSeq,
			Timestamp:       expectedTimestamp(g, extendedSeq),
			Arrival:         arrival,
			ScheduledOutput: g.scheduledOutput(extendedSeq),
			Duplicate:       duplicate,
		})
		return
	}
	if _, exists := g.buffer[extendedSeq]; exists {
		g.duplicatePackets++
		return
	}
	if extendedSeq > g.nextSequence+s.config.ReorderWindow-1 {
		// The packet is too far ahead to fit the explicitly bounded reorder
		// window. It will naturally become a missing frame if playback
		// advances before a nearer copy arrives.
		return
	}

	extendedTS, err := extendWrapped(g.highestTS, uint64(pkt.Timestamp), 32)
	if err != nil {
		g.invalidPackets++
		return
	}
	wantTS := g.nextTimestamp + (extendedSeq-g.nextSequence)*TimestampStep
	if extendedTS != wantTS {
		g.invalidPackets++
		return
	}

	g.buffer[extendedSeq] = decodeBufferedPacket(pkt, arrival, extendedTS)
	g.receivedSequences[extendedSeq] = true
	if extendedSeq > g.highestSeq {
		g.highestSeq = extendedSeq
	}
	if extendedTS > g.highestTS {
		g.highestTS = extendedTS
	}
}

func insertPCM(g *generation, pkt *Packet, arrival time.Time) {
	g.buffer[uint64(pkt.Sequence)] = decodeBufferedPacket(pkt, arrival, uint64(pkt.Timestamp))
	g.receivedSequences[uint64(pkt.Sequence)] = true
}

func decodeBufferedPacket(pkt *Packet, arrival time.Time, ts uint64) *bufferedPacket {
	bp := &bufferedPacket{ts: ts, arrival: arrival}
	_ = DecodePCM16BE(pkt.Payload, bp.samples[:])
	return bp
}

func (g *generation) emitDue(now time.Time) int {
	if !g.active || !g.started {
		return 0
	}
	firstOutput := g.firstArrival.Add(StartDelay)
	if now.Before(firstOutput) {
		return 0
	}
	elapsed := now.Sub(firstOutput)
	framesDue := uint64(elapsed/FrameDuration) + 1
	produced := 0
	for g.outputIndex() < framesDue {
		g.emitOne()
		produced++
	}
	return produced
}

func (g *generation) outputIndex() uint64 {
	return g.nextSequence - g.firstSequence
}

func (g *generation) emitOne() {
	index := g.outputIndex()
	at := g.firstArrival.Add(StartDelay).Add(time.Duration(index) * FrameDuration)
	frame := OutputFrame{
		Generation: g.id,
		Index:      index,
		Sequence:   g.nextSequence,
		Timestamp:  g.nextTimestamp,
		At:         at,
	}
	if bp := g.buffer[g.nextSequence]; bp != nil {
		frame.Samples = bp.samples
		delete(g.buffer, g.nextSequence)
	} else {
		frame.Missing = true
	}
	g.frames = append(g.frames, frame)
	g.nextSequence++
	g.nextTimestamp += TimestampStep
}

func expectedTimestamp(g *generation, seq uint64) uint64 {
	return g.firstTimestamp + (seq-g.firstSequence)*TimestampStep
}

func (g *generation) scheduledOutput(seq uint64) time.Time {
	index := seq - g.firstSequence
	return g.firstArrival.Add(StartDelay).Add(time.Duration(index) * FrameDuration)
}

func (s *source) recordStoppedLate(pkt *Packet, arrival time.Time) {
	g := s.current
	if !g.started {
		return
	}
	seq, err := extendWrapped(g.highestSeq, uint64(pkt.Sequence), 16)
	if err != nil {
		seq = uint64(pkt.Sequence)
	}
	g.latePackets++
	duplicate := seq < g.nextSequence
	if duplicate {
		g.duplicatePackets++
	}
	g.late = append(g.late, LateEvidence{
		Generation:      g.id,
		Sequence:        seq,
		Timestamp:       expectedTimestamp(g, seq),
		Arrival:         arrival,
		ScheduledOutput: g.scheduledOutput(seq),
		Duplicate:       duplicate,
	})
}

// StopSource emits all frames due at now, closes the active capture
// generation, and causes later packets to be rejected without modifying it.
func (r *Receiver) StopSource(key SourceKey, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return err
	}
	r.finalizeSource(s, now)
	s.current.active = false
	return nil
}

// RestartSource closes the current generation at now and opens a new capture
// generation. Queued packets from the old generation are discarded before the
// new first packet can initialize state.
func (r *Receiver) RestartSource(key SourceKey, now time.Time) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return 0, err
	}
	r.drainQueue(s)
	r.finalizeSource(s, now)
	newID := uint64(1)
	if s.current != nil {
		newID = s.current.id + 1
	}
	s.past = append(s.past, s.current)
	s.current = &generation{
		id:                newID,
		active:            true,
		buffer:            make(map[uint64]*bufferedPacket),
		receivedSequences: make(map[uint64]bool),
	}
	return newID, nil
}

func (r *Receiver) finalizeSource(s *source, now time.Time) {
	if now.IsZero() {
		now = r.clock.Now()
	}
	r.drainSource(s, now)
}

func (r *Receiver) drainQueue(s *source) {
	for {
		select {
		case <-s.queue:
		default:
			return
		}
	}
}

func (r *Receiver) requireSource(key SourceKey) (*source, error) {
	s := r.sources[key]
	if s == nil {
		return nil, fmt.Errorf("source %q: %w", key, ErrUnknownSource)
	}
	return s, nil
}

// QueueLen returns the number of packets currently waiting in a source's
// bounded inbound queue.
func (r *Receiver) QueueLen(key SourceKey) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return 0, err
	}
	return len(s.queue), nil
}

// QueueCapacity returns the hard per-source receive queue limit.
func (r *Receiver) QueueCapacity(key SourceKey) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return 0, err
	}
	return cap(s.queue), nil
}

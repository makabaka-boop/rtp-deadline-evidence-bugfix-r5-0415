package rtpaudio

import (
	"time"
)

// Generations returns all capture generation IDs for a source, oldest first.
func (r *Receiver) Generations(key SourceKey) ([]uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(s.past)+1)
	for _, g := range s.past {
		ids = append(ids, g.id)
	}
	ids = append(ids, s.current.id)
	return ids, nil
}

// Frames returns copies of the records that were actually emitted for a
// generation. Generation 0 means the source's current (or most recently
// stopped) generation.
func (r *Receiver) Frames(key SourceKey, generation uint64) ([]OutputFrame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, err := r.findGeneration(key, generation)
	if err != nil {
		return nil, err
	}
	out := make([]OutputFrame, len(g.frames))
	copy(out, g.frames)
	return out, nil
}

// LatePackets returns late-packet evidence collected by a generation.
func (r *Receiver) LatePackets(key SourceKey, generation uint64) ([]LateEvidence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, err := r.findGeneration(key, generation)
	if err != nil {
		return nil, err
	}
	out := make([]LateEvidence, len(g.late))
	copy(out, g.late)
	return out, nil
}

// Info returns counters and state for a generation.
func (r *Receiver) Info(key SourceKey, generation uint64) (GenerationInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.requireSource(key)
	if err != nil {
		return GenerationInfo{}, err
	}
	g, err := generationOf(s, generation)
	if err != nil {
		return GenerationInfo{}, err
	}
	missing := 0
	for _, f := range g.frames {
		if f.Missing {
			missing++
		}
	}
	return GenerationInfo{
		Generation:       g.id,
		Active:           g.active,
		SSRC:             g.ssrc,
		FirstArrival:     g.firstArrival,
		FirstSequence:    g.firstSequence,
		FirstTimestamp:   g.firstTimestamp,
		Frames:           len(g.frames),
		MissingFrames:    missing,
		QueueFullDrops:   g.queueFullDrops,
		StoppedDrops:     g.stoppedDrops,
		InvalidPackets:   g.invalidPackets,
		DuplicatePackets: g.duplicatePackets,
		LatePackets:      g.latePackets,
	}, nil
}

// ClockNow returns the controlled or wall clock time used by the receiver.
func (r *Receiver) ClockNow() time.Time { return r.clock.Now() }

// Sources returns all known source identifiers in a stable-enough order for
// report generation.
func (r *Receiver) Sources() []SourceKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SourceKey, 0, len(r.sources))
	for key := range r.sources {
		out = append(out, key)
	}
	return out
}

func (r *Receiver) findGeneration(key SourceKey, id uint64) (*generation, error) {
	s, err := r.requireSource(key)
	if err != nil {
		return nil, err
	}
	return generationOf(s, id)
}

func generationOf(s *source, id uint64) (*generation, error) {
	if id == 0 {
		return s.current, nil
	}
	if s.current.id == id {
		return s.current, nil
	}
	for _, g := range s.past {
		if g.id == id {
			return g, nil
		}
	}
	return nil, &GenerationNotFoundError{Source: s.key, Generation: id}
}

// GenerationNotFoundError indicates that no capture generation matched an ID.
type GenerationNotFoundError struct {
	Source     SourceKey
	Generation uint64
}

func (e *GenerationNotFoundError) Error() string {
	return "generation " + uintToString(e.Generation) + " not found for source " + string(e.Source)
}

// MissingReportFor builds a report directly from emitted OutputFrame records.
// No live jitter-buffer state is used to infer missing audio.
func (r *Receiver) MissingReportFor(key SourceKey, generation uint64) (MissingReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, err := r.findGeneration(key, generation)
	if err != nil {
		return MissingReport{}, err
	}
	return buildMissingReport(key, g), nil
}

// exportSnapshot gathers the immutable records for one export while holding the
// receiver lock. Resolving generation 0 here ensures the WAV file name and its
// JSON report always name the same actual generation.
func (r *Receiver) exportSnapshot(key SourceKey, generation uint64) (uint64, []OutputFrame, MissingReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, err := r.findGeneration(key, generation)
	if err != nil {
		return 0, nil, MissingReport{}, err
	}
	frames := make([]OutputFrame, len(g.frames))
	copy(frames, g.frames)
	report := buildMissingReport(key, g)
	return g.id, frames, report, nil
}

func buildMissingReport(key SourceKey, g *generation) MissingReport {
	missing := make([]MissingFrame, 0)
	for _, f := range g.frames {
		if !f.Missing {
			continue
		}
		missing = append(missing, MissingFrame{
			Index:     f.Index,
			Sequence:  f.Sequence,
			Timestamp: f.Timestamp,
			Scheduled: f.At,
		})
	}
	late := make([]LateEvidence, len(g.late))
	copy(late, g.late)
	return MissingReport{
		Source:           key,
		Generation:       g.id,
		ClockRate:        ClockRate,
		SamplesPerPacket: SamplesPerPacket,
		FrameDurationMS:  int64(FrameDuration / time.Millisecond),
		Frames:           len(g.frames),
		MissingFrames:    len(missing),
		Missing:          missing,
		LatePackets:      late,
	}
}

func uintToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

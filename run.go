package rtpaudio

import (
	"context"
	"time"
)

// Run is the real-time pump loop. It schedules the next frame deadline based
// on active generations so playout remains controlled by the injected Clock
// abstraction. Packet arrival wakes the loop for source creation but cannot
// force frames before their 60 ms start deadline.
func (r *Receiver) Run(ctx context.Context) {
	timer := r.clock.AfterFunc(r.nextDeadline(), func() { r.signalWake() })
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		}
		timer.Stop()
		r.Pump()
		timer.Reset(r.nextDeadline())
	}
}

func (r *Receiver) signalWake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Receiver) nextDeadline() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clock.Now()
	earliest := now.Add(FrameDuration)
	for _, s := range r.sources {
		g := s.current
		if !g.active || !g.started {
			continue
		}
		index := g.outputIndex()
		at := g.firstArrival.Add(StartDelay).Add(time.Duration(index) * FrameDuration)
		if at.Before(now) {
			at = now
		}
		if at.Before(earliest) {
			earliest = at
		}
	}
	return earliest
}

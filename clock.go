package rtpaudio

import (
	"sort"
	"sync"
	"time"
)

// Clock allows playback timing to be controlled in tests and driven by the
// wall clock in production.
type Clock interface {
	Now() time.Time
	AfterFunc(deadline time.Time, fn func()) Timer
}

// Timer is a one-shot timer tied to an absolute deadline.
type Timer interface {
	Stop() bool
	Reset(deadline time.Time) bool
	C() <-chan time.Time
}

// RealClock schedules timers with the standard time package.
type RealClock struct{}

// NewRealClock returns a wall-clock Clock.
func NewRealClock() RealClock { return RealClock{} }

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// AfterFunc schedules fn at deadline.
func (RealClock) AfterFunc(deadline time.Time, fn func()) Timer {
	d := time.Until(deadline)
	if d < 0 {
		d = 0
	}
	return &realTimer{timer: time.AfterFunc(d, fn), deadline: deadline}
}

type realTimer struct {
	timer    *time.Timer
	deadline time.Time
	mu       sync.Mutex
}

func (t *realTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.timer.Stop()
}

func (t *realTimer) Reset(deadline time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	stopped := t.timer.Stop()
	t.deadline = deadline
	d := time.Until(deadline)
	if d < 0 {
		d = 0
	}
	t.timer.Reset(d)
	return stopped
}

func (t *realTimer) C() <-chan time.Time { return nil }

// VirtualClock is a deterministic clock for tests. No goroutine runs until a
// test explicitly calls Advance.
type VirtualClock struct {
	mu     sync.Mutex
	now    time.Time
	nextID uint64
	timers map[uint64]*virtualTimer
}

// NewVirtualClock creates a clock at t.
func NewVirtualClock(t time.Time) *VirtualClock {
	return &VirtualClock{now: t, timers: make(map[uint64]*virtualTimer)}
}

// Now returns the virtual time.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc creates a one-shot virtual timer.
func (c *VirtualClock) AfterFunc(deadline time.Time, fn func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	t := &virtualTimer{
		clock:    c,
		id:       c.nextID,
		deadline: deadline,
		fn:       fn,
		ch:       make(chan time.Time, 1),
	}
	c.timers[t.id] = t
	return t
}

// Advance moves virtual time forward and synchronously fires every timer whose
// deadline is reached. Durations must be non-negative.
func (c *VirtualClock) Advance(d time.Duration) {
	if d < 0 {
		panic("virtual clock cannot move backwards")
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()

	for {
		due := c.takeDue()
		if len(due) == 0 {
			return
		}
		// Invoke timers without holding clock.mu so callbacks may create or
		// reset other timers. A callback scheduling an already-due timer is
		// drained on the next iteration.
		for _, t := range due {
			t.mu.Lock()
			fn := t.fn
			ch := t.ch
			deadline := t.deadline
			t.mu.Unlock()
			if fn != nil {
				fn()
			} else {
				select {
				case ch <- deadline:
				default:
				}
			}
		}
	}
}

// takeDue atomically marks the next due batch as fired and returns it sorted
// by deadline.
func (c *VirtualClock) takeDue() []*virtualTimer {
	c.mu.Lock()
	defer c.mu.Unlock()

	type dueTimer struct {
		timer    *virtualTimer
		deadline time.Time
	}
	var due []dueTimer
	for _, t := range c.timers {
		t.mu.Lock()
		if !t.stopped && !t.fired && !t.deadline.After(c.now) {
			due = append(due, dueTimer{timer: t, deadline: t.deadline})
		}
		t.mu.Unlock()
	}
	if len(due) == 0 {
		return nil
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].deadline.Before(due[j].deadline)
	})
	for _, item := range due {
		t := item.timer
		t.mu.Lock()
		if !t.stopped && !t.fired && !t.deadline.After(c.now) {
			t.fired = true
			delete(c.timers, t.id)
		}
		t.mu.Unlock()
	}
	out := make([]*virtualTimer, len(due))
	for i, item := range due {
		out[i] = item.timer
	}
	return out
}

type virtualTimer struct {
	clock    *VirtualClock
	id       uint64
	deadline time.Time
	fn       func()
	ch       chan time.Time
	stopped  bool
	fired    bool
	mu       sync.Mutex
}

func (t *virtualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	delete(t.clock.timers, t.id)
	return true
}

func (t *virtualTimer) Reset(deadline time.Time) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	active := !t.fired && !t.stopped
	t.stopped = false
	t.fired = false
	t.deadline = deadline
	t.clock.timers[t.id] = t
	return active
}

func (t *virtualTimer) C() <-chan time.Time { return t.ch }

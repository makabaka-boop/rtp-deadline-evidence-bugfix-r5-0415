package rtpaudio

import "fmt"

// extendWrapped expands a wrapping unsigned RTP counter into the monotonic
// uint64 number line. width is 16 for sequence numbers and 32 for timestamps.
//
// The nearest value on the wrapping circle is selected. A jump of at least
// half of the modulus is rejected as a discontinuity rather than guessed.
// Repeated wraps remain valid because the returned value is used as the next
// base.
func extendWrapped(base, raw uint64, width uint) (uint64, error) {
	if width == 0 || width > 63 {
		return 0, fmt.Errorf("invalid RTP counter width %d", width)
	}
	mod := uint64(1) << width
	half := int64(mod / 2)
	if raw >= mod {
		return 0, fmt.Errorf("raw RTP value %d is wider than %d bits", raw, width)
	}

	delta := int64((raw - (base % mod) + mod) % mod)
	if delta >= half {
		delta -= int64(mod)
	}
	if delta <= -half {
		return 0, fmt.Errorf("%w: jump from %d to raw %d is at least half of 2^%d",
			ErrCounterJump, base, raw, width)
	}

	extended := int64(base) + delta
	if extended < 0 {
		return 0, fmt.Errorf("%w: sequence extension from %d to raw %d predates zero",
			ErrCounterJump, base, raw)
	}
	return uint64(extended), nil
}

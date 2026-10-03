package rtpaudio

import "testing"

func TestExtendWrapped16DoubleWrap(t *testing.T) {
	base, raw := uint64(0), uint64(0)
	for i := 0; i < 131072; i++ {
		raw++
		if raw == 65536 {
			raw = 0
		}
		got, err := extendWrapped(base, raw, 16)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		base++
		if got != base {
			t.Fatalf("step %d: got %d, want %d", i, got, base)
		}
	}
}

func TestExtendWrapped32DoubleWrap(t *testing.T) {
	tests := []struct {
		base uint64
		raw  uint64
		want uint64
	}{
		{1<<32 - 8, 8, 1<<32 + 8},
		{1<<33 - 8, 8, 1<<33 + 8},
		{1<<33 + 10, 0xfffffff0, 1<<33 - 16},
	}
	for _, tc := range tests {
		got, err := extendWrapped(tc.base, tc.raw, 32)
		if err != nil {
			t.Fatalf("extend %d->%d: %v", tc.base, tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("extend %d->%d = %d, want %d", tc.base, tc.raw, got, tc.want)
		}
	}
}

func TestExtendWrappedRejectsHalfRangeJump(t *testing.T) {
	if _, err := extendWrapped(0, 32768, 16); err == nil {
		t.Fatal("accepted ambiguous half-range sequence jump")
	}
	if _, err := extendWrapped(0, 1<<31, 32); err == nil {
		t.Fatal("accepted ambiguous half-range timestamp jump")
	}
}

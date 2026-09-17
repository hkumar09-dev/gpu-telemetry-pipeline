package clock

import (
	"testing"
	"time"
)

func TestClocks(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := FixedClock{T: fixed}
	if !clk.Now().Equal(fixed) {
		t.Fatal("fixed")
	}
	sys := SystemClock{}
	if sys.Now().IsZero() {
		t.Fatal("system")
	}
}

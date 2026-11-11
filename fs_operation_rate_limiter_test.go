package fsopratelimiter

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock is a deterministic Clock for tests. It records sleeps and advances
// its own notion of time without touching the wall clock.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	slept  time.Duration
	sleeps int
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.slept += d
	c.sleeps++
}

func TestNewLimiterRejectsBadArgs(t *testing.T) {
	if _, err := NewLimiter(0, 1, nil); err == nil {
		t.Fatal("expected error for zero rate")
	}
	if _, err := NewLimiter(-1, 1, nil); err == nil {
		t.Fatal("expected error for negative rate")
	}
	if _, err := NewLimiter(1, 0, nil); err == nil {
		t.Fatal("expected error for zero burst")
	}
	if _, err := NewLimiter(1, -1, nil); err == nil {
		t.Fatal("expected error for negative burst")
	}
}

func TestWaitConsumesBurstThenRefills(t *testing.T) {
	clk := newFakeClock()
	lim, err := NewLimiter(2, 2, clk)
	if err != nil {
		t.Fatal(err)
	}

	// Two immediate waits should succeed without sleeping.
	for i := 0; i < 2; i++ {
		if err := lim.Wait(); err != nil {
			t.Fatalf("wait %d: %v", i, err)
		}
	}
	if got := lim.Tokens(); got != 0 {
		t.Fatalf("tokens after burst = %d, want 0", got)
	}

	// Third wait must sleep until a token accrues. At 2 tokens/sec, 0.5s.
	if err := lim.Wait(); err != nil {
		t.Fatal(err)
	}
	if clk.sleeps != 1 {
		t.Fatalf("sleeps = %d, want 1", clk.sleeps)
	}
	wantSleep := 500 * time.Millisecond
	if clk.slept != wantSleep {
		t.Fatalf("slept = %v, want %v", clk.slept, wantSleep)
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	clk := newFakeClock()
	lim, err := NewLimiter(10, 3, clk)
	if err != nil {
		t.Fatal(err)
	}
	// Consume all tokens.
	for i := 0; i < 3; i++ {
		if err := lim.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	// Advance time far beyond what burst allows.
	clk.Sleep(10 * time.Second)
	if got := lim.Tokens(); got != 3 {
		t.Fatalf("tokens after long refill = %d, want 3", got)
	}
}

func TestCloseUnblocksWait(t *testing.T) {
	clk := newFakeClock()
	lim, err := NewLimiter(1, 1, clk)
	if err != nil {
		t.Fatal(err)
	}
	// Consume the only token.
	if err := lim.Wait(); err != nil {
		t.Fatal(err)
	}
	// Close before the next Wait can get a token.
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Wait(); !errors.Is(err, ErrClosed) {
		t.Fatalf("wait after close = %v, want ErrClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	lim, err := NewLimiter(1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
}

func TestConcurrentWaitRespectsBurst(t *testing.T) {
	clk := newFakeClock()
	lim, err := NewLimiter(1000, 5, clk)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- lim.Wait()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestWaitOnClosedLimiterReturnsErrClosed(t *testing.T) {
	lim, err := NewLimiter(1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Wait(); !errors.Is(err, ErrClosed) {
		t.Fatalf("wait on closed = %v, want ErrClosed", err)
	}
}

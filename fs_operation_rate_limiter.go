// Package fsopratelimiter provides a token bucket rate limiter for filesystem
// operations. It is designed to prevent I/O saturation on slow volumes by
// limiting the rate at which operations are started.
//
// The limiter is safe for concurrent use. Time is injected via a Clock
// interface so that tests can be deterministic without sleeping.
package fsopratelimiter

import (
	"errors"
	"sync"
	"time"
)

// ErrClosed is returned by Wait when the limiter has been closed.
var ErrClosed = errors.New("fs operation rate limiter: closed")

// Clock abstracts time so tests can advance it manually. Implementations must
// be safe for concurrent use if the limiter is used concurrently.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// realClock is the default Clock backed by the standard library.
type realClock struct{}

func (realClock) Now() time.Time        { return time.Now() }
func (realClock) Sleep(d time.Duration) { time.Sleep(d) }

// Limiter limits the rate of filesystem operations using a token bucket.
//
// Tokens are replenished at a constant rate up to a burst capacity. Each call
// to Wait consumes one token, blocking until a token is available or the
// limiter is closed.
//
// A zero value is not usable; construct with NewLimiter.
type Limiter struct {
	mu         sync.Mutex
	tokens     float64
	burst      float64
	ratePerSec float64
	last       time.Time
	closed     bool
	clock      Clock
}

// NewLimiter returns a Limiter that permits up to burst operations
// immediately and then replenishes tokens at ratePerSec operations per second.
//
// ratePerSec must be positive and burst must be at least 1. If clock is nil,
// the real system clock is used.
func NewLimiter(ratePerSec float64, burst int, clock Clock) (*Limiter, error) {
	if ratePerSec <= 0 {
		return nil, errors.New("fs operation rate limiter: ratePerSec must be positive")
	}
	if burst < 1 {
		return nil, errors.New("fs operation rate limiter: burst must be at least 1")
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Limiter{
		tokens:     float64(burst),
		burst:      float64(burst),
		ratePerSec: ratePerSec,
		last:       clock.Now(),
		clock:      clock,
	}, nil
}

// Wait blocks until a token is available, then consumes it. It returns
// ErrClosed if the limiter is closed before a token becomes available.
func (l *Limiter) Wait() error {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return ErrClosed
		}
		now := l.clock.Now()
		l.refillLocked(now)
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		// Compute how long until one token is available.
		missing := 1 - l.tokens
		wait := time.Duration(missing / l.ratePerSec * float64(time.Second))
		if wait <= 0 {
			// Extremely small wait; avoid busy loop by using a minimum step.
			wait = time.Nanosecond
		}
		l.mu.Unlock()
		// Sleep outside the lock so other goroutines can proceed.
		l.clock.Sleep(wait)
	}
}

// refillLocked adds tokens accrued since the last refill. Callers must hold l.mu.
func (l *Limiter) refillLocked(now time.Time) {
	elapsed := now.Sub(l.last).Seconds()
	if elapsed <= 0 {
		return
	}
	l.tokens += elapsed * l.ratePerSec
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.last = now
}

// Close marks the limiter as closed. Any goroutine blocked in Wait will
// return ErrClosed. Close is idempotent.
func (l *Limiter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}

// Tokens returns the current number of available tokens, rounded down to an
// integer. It is primarily useful for diagnostics and tests.
func (l *Limiter) Tokens() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(l.clock.Now())
	if l.tokens < 0 {
		return 0
	}
	return int(l.tokens)
}

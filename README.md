# fs-operation-rate-limiter

Wraps filesystem operations with token bucket rate limiting to prevent I/O saturation on slow volumes.

```go
package main

import (
	"fmt"
	"os"

	fsopratelimiter "github.com/example/fs-operation-rate-limiter"
)

func main() {
	lim, err := fsopratelimiter.NewLimiter(100, 10, nil)
	if err != nil {
		panic(err)
	}
	defer lim.Close()

	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := lim.Wait(); err != nil {
			fmt.Println("limiter closed:", err)
			return
		}
		if _, err := os.ReadFile(name); err != nil {
			fmt.Println("read:", err)
		}
	}
}
```

## Why this exists

A busy filesystem can be saturated by a burst of reads or writes, especially on
slow volumes such as network mounts or spinning disks. This library lets you
place a single limiter in front of those operations so that they start at a
controlled rate. The trade-off is simple: throughput is capped, but latency and
contention stay predictable. Tokens are refilled continuously rather than in
fixed windows, so there is no thundering herd at window boundaries.

## The awkward edge

`Wait` blocks until a token is available. If you close the limiter while a
goroutine is blocked, that goroutine returns `ErrClosed` rather than hanging.
The limiter does not try to drain in-flight operations; it only stops new ones
from starting. A zero value `Limiter` is not usable and will panic; always
construct with `NewLimiter`.

## API

- `NewLimiter(ratePerSec float64, burst int, clock Clock) (*Limiter, error)`
- `(*Limiter).Wait() error`
- `(*Limiter).Close() error`
- `(*Limiter).Tokens() int`
- `ErrClosed`
- `Clock` interface with `Now() time.Time` and `Sleep(time.Duration)`

Pass a custom `Clock` to make tests deterministic; pass `nil` to use the real
system clock.

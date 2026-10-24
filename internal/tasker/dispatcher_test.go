package tasker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type slowClaimStorage struct {
	Storage
	claims int
}

func (s *slowClaimStorage) ClaimDue(context.Context, time.Duration) (Task, error) {
	s.claims++
	if s.claims == 1 {
		time.Sleep(150 * time.Millisecond)
	}
	if s.claims > 2 {
		return Task{}, ErrNoTaskDue
	}
	return Task{}, nil
}

func TestDispatcherLimitsStartsAfterSlowClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		config := DefaultConfig
		config.Storage = &slowClaimStorage{}
		config.MaxDispatchesPerSecond = 10
		d := newDispatcher(config)
		starts := make(chan time.Time, 2)
		d.execute = func(context.Context, Task) {
			starts <- time.Now()
		}
		done := make(chan error, 1)
		go func() { done <- d.run(ctx) }()

		first, second := <-starts, <-starts
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("run returned %v, want cancellation", err)
		}
		if gap := second.Sub(first); gap < 100*time.Millisecond {
			t.Fatalf("execution starts only %s apart, want at least 100ms", gap)
		}
		if len(d.slots) != 0 {
			t.Fatal("dispatcher leaked concurrency slots")
		}
	})
}

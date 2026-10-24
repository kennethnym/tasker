package tasker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type cleanupStorage struct {
	Storage
	delete func(context.Context, time.Time, int) (int, error)
	claim  func(context.Context, time.Duration) (Task, error)
}

func (s cleanupStorage) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	return s.delete(ctx, cutoff, limit)
}

func (s cleanupStorage) ClaimDue(ctx context.Context, duration time.Duration) (Task, error) {
	return s.claim(ctx, duration)
}

func TestCleanupBatches(t *testing.T) {
	now := time.Now().UTC()
	retention := 24 * time.Hour
	calls := 0
	c := cleanupReconciler{retention: retention, storage: cleanupStorage{
		delete: func(_ context.Context, cutoff time.Time, limit int) (int, error) {
			calls++
			if !cutoff.Equal(now.Add(-retention)) || limit != cleanupBatchSize {
				t.Fatalf("unexpected cutoff or limit: %v, %d", cutoff, limit)
			}
			if calls < 3 {
				return limit, nil
			}
			return 1, nil
		},
	}}
	if err := c.reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("got %d calls, want 3", calls)
	}
}

func TestCleanupRetriesNextSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		var mu sync.Mutex
		var cutoffs []time.Time
		c := cleanupReconciler{retention: time.Hour, storage: cleanupStorage{
			delete: func(_ context.Context, cutoff time.Time, _ int) (int, error) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				cutoffs = append(cutoffs, cutoff)
				if calls == 1 {
					return 0, errors.New("storage unavailable")
				}
				return 0, nil
			},
		}}
		done := make(chan struct{})
		go func() { defer close(done); c.run(ctx) }()
		synctest.Wait()
		mu.Lock()
		if calls != 1 {
			t.Fatalf("startup calls = %d", calls)
		}
		mu.Unlock()
		time.Sleep(cleanupInterval)
		synctest.Wait()
		mu.Lock()
		if calls != 2 {
			t.Fatalf("calls after retry = %d", calls)
		}
		if cutoffs[1].Sub(cutoffs[0]) != cleanupInterval {
			t.Fatal("cutoff did not advance")
		}
		mu.Unlock()
		cancel()
		<-done
	})
}

func TestCleanupCancellationBetweenBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	c := cleanupReconciler{retention: time.Hour, storage: cleanupStorage{
		delete: func(context.Context, time.Time, int) (int, error) {
			calls++
			cancel()
			return cleanupBatchSize, nil
		},
	}}
	if err := c.reconcile(ctx, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
	if err := c.reconcile(ctx, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if calls != 1 {
		t.Fatal("called storage after cancellation")
	}
}

func TestTaskRetentionValidation(t *testing.T) {
	for _, retention := range []time.Duration{-time.Second, 0, time.Hour} {
		service, err := New(Config{Storage: cleanupStorage{}, TaskRetention: retention})
		if retention < 0 {
			if err == nil {
				t.Fatal("negative retention accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		expected := retention
		if expected == 0 {
			expected = DefaultConfig.TaskRetention
		}
		if service.cleanup.retention != expected {
			t.Fatalf("retention = %v", service.cleanup.retention)
		}
	}
}

func TestRunStopsCleanupWhenDispatcherExits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		exited := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		service, err := New(Config{Storage: cleanupStorage{
			delete: func(ctx context.Context, _ time.Time, _ int) (int, error) {
				close(entered)
				<-ctx.Done()
				close(exited)
				return 0, ctx.Err()
			},
			claim: func(context.Context, time.Duration) (Task, error) {
				<-entered
				cancel()
				return Task{}, ctx.Err()
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		service.dispatcher.execute = func(context.Context, Task) {}
		if err := service.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		select {
		case <-exited:
		default:
			t.Fatal("Run returned before cleanup stopped")
		}
	})
}

func TestRunPreservesUnimplementedDispatcherError(t *testing.T) {
	service, err := New(Config{Storage: cleanupStorage{delete: func(context.Context, time.Time, int) (int, error) { return 0, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Run(context.Background()); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Run = %v", err)
	}
}

package e2e_test

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyLimit(t *testing.T) {
	t.Parallel()
	const limit, count = 2, 6
	permits := make(chan struct{}, count)
	var active, peak atomic.Int32
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		select {
		case <-permits:
			_, _ = io.WriteString(w, `{"ok":true}`)
		case <-r.Context().Done():
		}
	})
	p := startTasker(t, "", map[string]string{"max_concurrent_dispatches": "2", "dispatch_deadline": "30s"})
	tasks := make(map[string]task)
	for range count {
		created := p.create(t, e.server.URL, `{}`, nil)
		tasks[created.ID] = created
	}
	seen := make(map[string]bool)
	for batch := 0; batch < count/limit; batch++ {
		for range limit {
			d := e.next(t, waitTimeout)
			id := d.Header.Get("X-Task-Id")
			created, exists := tasks[id]
			if !exists || seen[id] {
				t.Fatalf("unexpected or duplicate task %q", id)
			}
			seen[id] = true
			assertDelivery(t, d, created, 1, "/")
		}
		e.quiet(t, 1200*time.Millisecond)
		for id := range tasks {
			if !seen[id] {
				queued := p.get(t, id)
				if queued.Status != "queued" || queued.Attempt != 0 {
					t.Errorf("task waiting for a slot was already claimed: %+v", queued)
				}
			}
		}
		for range limit {
			permits <- struct{}{}
		}
	}
	for id := range tasks {
		assertTerminal(t, p.awaitStatus(t, id, "succeeded", waitTimeout), "succeeded", 1)
	}
	if got := peak.Load(); got != limit {
		t.Errorf("peak active deliveries = %d, want %d", got, limit)
	}
}

func TestRateLimitIncludesRetries(t *testing.T) {
	t.Parallel()
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Task-Attempt") == "1" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	p := startTasker(t, "", map[string]string{"max_dispatches_per_second": "2", "max_attempts": "2"})
	tasks := make(map[string]task)
	// Make a backlog before any task is eligible, then let polling discover it.
	due := time.Now().Add(2 * time.Second)
	for range 6 {
		created := p.create(t, e.server.URL, `{}`, map[string]any{"scheduleTime": due})
		tasks[created.ID] = created
	}
	seen := make(map[string]int)
	var first time.Time
	for i := range 12 {
		d := e.next(t, waitTimeout)
		id := d.Header.Get("X-Task-Id")
		created, exists := tasks[id]
		if !exists {
			t.Fatalf("unexpected task %q", id)
		}
		seen[id]++
		assertDelivery(t, d, created, seen[id], "/")
		if i == 0 {
			first = d.At
		}
		// Test cumulative throughput with jitter allowance, not exact spacing:
		// the current limiter gates claims rather than HTTP request starts.
		minimum := time.Duration(i)*500*time.Millisecond - 200*time.Millisecond
		if elapsed := d.At.Sub(first); elapsed < minimum {
			t.Errorf("%d deliveries in %s; rate 2/s requires at least %s with tolerance", i+1, elapsed, minimum)
		}
	}
	for id := range tasks {
		if seen[id] != 2 {
			t.Errorf("task %s delivered %d times, want 2", id, seen[id])
		}
		assertTerminal(t, p.awaitStatus(t, id, "succeeded", waitTimeout), "succeeded", 2)
	}
	e.quiet(t, 1200*time.Millisecond)
}

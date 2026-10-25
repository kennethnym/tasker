package e2e_test

import (
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRestartPreservesTasks(t *testing.T) {
	t.Parallel()
	e := newExecutor(t, nil)
	database := filepath.Join(t.TempDir(), "tasks.db")
	p := startTasker(t, database, nil)
	completed := p.create(t, e.server.URL, `{"state":"completed"}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), completed, 1, "/")
	before := p.awaitStatus(t, completed.ID, "succeeded", waitTimeout)
	queued := p.create(t, e.server.URL, `{"state":"queued"}`, map[string]any{"scheduleTime": time.Now().Add(2 * time.Second)})
	p.stop(t, true)
	p = startTasker(t, database, nil)
	after := p.get(t, completed.ID)
	assertTerminal(t, after, "succeeded", 1)
	assertJSON(t, after.Result, string(before.Result))
	if !after.FinishedAt.Equal(*before.FinishedAt) {
		t.Error("completion timestamp changed after restart")
	}
	assertDelivery(t, e.next(t, waitTimeout), queued, 1, "/")
	assertTerminal(t, p.awaitStatus(t, queued.ID, "succeeded", waitTimeout), "succeeded", 1)
	e.quiet(t, 1200*time.Millisecond)
}

func TestInterruptedDeliveryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the real 30s lease to expire")
	}
	t.Parallel()
	for _, tc := range []struct {
		name     string
		force    bool
		attempts int
	}{
		{"sigterm", false, 3},
		{"kill", true, 3},
		{"exhausted_after_kill", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			canceled := make(chan struct{}, 1)
			e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Task-Attempt") == "1" {
					<-r.Context().Done()
					select {
					case canceled <- struct{}{}:
					default:
					}
					return
				}
				_, _ = io.WriteString(w, `{"recovered":true}`)
			})
			database := filepath.Join(t.TempDir(), "tasks.db")
			config := map[string]string{"dispatch_deadline": "2m", "max_attempts": strconv.Itoa(tc.attempts)}
			p := startTasker(t, database, config)
			created := p.create(t, e.server.URL, `{"work":"recover"}`, nil)
			first := e.next(t, waitTimeout)
			assertDelivery(t, first, created, 1, "/")
			running := p.get(t, created.ID)
			if running.Status != "running" || running.StartedAt == nil {
				t.Fatalf("expected active delivery: %+v", running)
			}
			p.stop(t, tc.force)
			select {
			case <-canceled:
			case <-time.After(waitTimeout):
				t.Fatal("executor connection was not canceled after process exit")
			}
			p = startTasker(t, database, config)
			abandoned := p.get(t, created.ID)
			if abandoned.Status != "running" || abandoned.Attempt != 1 || abandoned.FinishedAt != nil {
				t.Fatalf("interrupted claim should remain recoverable: %+v", abandoned)
			}
			var result task
			if tc.attempts == 1 {
				result = p.awaitStatus(t, created.ID, "failed", 45*time.Second)
				assertTerminal(t, result, "failed", 1)
				if result.FinishedAt.Sub(first.At) < 28*time.Second {
					t.Error("abandoned final attempt failed before lease expiry")
				}
				if !strings.Contains(result.LastError, "lease expired") {
					t.Errorf("recovery error = %q", result.LastError)
				}
			} else {
				second := e.next(t, 45*time.Second)
				assertDelivery(t, second, created, 2, "/")
				// A generous lower bound detects immediate reclaim while allowing
				// the small gap between claiming and observing the first request.
				if second.At.Sub(first.At) < 28*time.Second {
					t.Error("abandoned task was reclaimed before lease expiry")
				}
				result = p.awaitStatus(t, created.ID, "succeeded", waitTimeout)
				assertTerminal(t, result, "succeeded", 2)
				assertJSON(t, result.Result, `{"recovered":true}`)
			}
			if !result.StartedAt.Equal(*running.StartedAt) {
				t.Error("recovery changed startedAt")
			}
			e.quiet(t, 1200*time.Millisecond)
		})
	}
}

func TestLeaseRenewalWithCompetingProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("holds a request beyond the real 30s lease")
	}
	t.Parallel()
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/long" {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	database := filepath.Join(t.TempDir(), "tasks.db")
	config := map[string]string{"dispatch_deadline": "2m", "max_concurrent_dispatches": "1"}
	first := startTasker(t, database, config)
	created := first.create(t, e.server.URL+"/long", `{}`, nil)
	d := e.next(t, waitTimeout)
	assertDelivery(t, d, created, 1, "/long")
	second := startTasker(t, database, config)
	// The first process's only slot is occupied. These tasks prove that the
	// competing process is polling and can claim work from the shared database.
	probe := func() {
		other := second.create(t, e.server.URL+"/probe", `{}`, nil)
		assertDelivery(t, e.next(t, waitTimeout), other, 1, "/probe")
		assertTerminal(t, second.awaitStatus(t, other.ID, "succeeded", waitTimeout), "succeeded", 1)
	}
	probe()
	e.quiet(t, time.Until(d.At.Add(33*time.Second)))
	probe()
	running := second.get(t, created.ID)
	if running.Status != "running" || running.Attempt != 1 {
		t.Fatalf("long delivery lost its lease: %+v", running)
	}
	finish()
	assertTerminal(t, second.awaitStatus(t, created.ID, "succeeded", waitTimeout), "succeeded", 1)
	e.quiet(t, 1200*time.Millisecond)
}

func TestRetentionOnStartup(t *testing.T) {
	t.Parallel()
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fail":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/hold":
			<-r.Context().Done()
		default:
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	})
	database := filepath.Join(t.TempDir(), "tasks.db")
	config := map[string]string{"max_attempts": "1", "dispatch_deadline": "2m"}
	p := startTasker(t, database, config)
	success := p.create(t, e.server.URL+"/success", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), success, 1, "/success")
	p.awaitStatus(t, success.ID, "succeeded", waitTimeout)
	failure := p.create(t, e.server.URL+"/fail", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), failure, 1, "/fail")
	p.awaitStatus(t, failure.ID, "failed", waitTimeout)
	queued := p.create(t, e.server.URL+"/future", `{}`, map[string]any{"scheduleTime": time.Now().Add(time.Hour)})
	running := p.create(t, e.server.URL+"/hold", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), running, 1, "/hold")
	p.stop(t, false)
	// All four tasks now predate this retention period; only terminal ones
	// should be deleted. Restart invokes the actual startup cleanup sweep.
	config["task_retention"] = "1us"
	p = startTasker(t, database, config)
	for _, id := range []string{success.ID, failure.ID} {
		awaitDeleted(t, p, id, waitTimeout)
	}
	if task := p.get(t, queued.ID); task.Status != "queued" || task.Attempt != 0 {
		t.Errorf("cleanup affected queued task: %+v", task)
	}
	if task := p.get(t, running.ID); task.Status != "running" || task.Attempt != 1 {
		t.Errorf("cleanup affected running task: %+v", task)
	}
	e.quiet(t, 1200*time.Millisecond)
}

func TestPeriodicRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the real one-minute cleanup interval")
	}
	t.Parallel()
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hold":
			<-r.Context().Done()
		case "/fail":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	})
	p := startTasker(t, "", map[string]string{"task_retention": "5s", "dispatch_deadline": "2m", "max_attempts": "1"})
	success := p.create(t, e.server.URL+"/success", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), success, 1, "/success")
	p.awaitStatus(t, success.ID, "succeeded", waitTimeout)
	failure := p.create(t, e.server.URL+"/fail", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), failure, 1, "/fail")
	p.awaitStatus(t, failure.ID, "failed", waitTimeout)
	queued := p.create(t, e.server.URL+"/future", `{}`, map[string]any{"scheduleTime": time.Now().Add(time.Hour)})
	running := p.create(t, e.server.URL+"/hold", `{}`, nil)
	assertDelivery(t, e.next(t, waitTimeout), running, 1, "/hold")
	awaitDeleted(t, p, success.ID, 75*time.Second)
	awaitDeleted(t, p, failure.ID, waitTimeout)
	if task := p.get(t, queued.ID); task.Status != "queued" || task.Attempt != 0 {
		t.Errorf("periodic cleanup affected queued task: %+v", task)
	}
	if task := p.get(t, running.ID); task.Status != "running" || task.Attempt != 1 {
		t.Errorf("periodic cleanup affected running task: %+v", task)
	}
	e.quiet(t, 1200*time.Millisecond)
}

func awaitDeleted(t *testing.T, p *process, id string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, id+" is deleted", func() bool {
		status, _, body := p.request(t, http.MethodGet, "/tasks/"+id, nil)
		if status != http.StatusOK && status != http.StatusNotFound {
			t.Fatalf("get during cleanup: HTTP %d: %s", status, body)
		}
		return status == http.StatusNotFound
	})
}

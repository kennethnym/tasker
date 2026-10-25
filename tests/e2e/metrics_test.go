package e2e_test

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var enableMetrics = map[string]any{"metrics": map[string]any{"enabled": true}}

func scrapeMetrics(t *testing.T, p *process) string {
	t.Helper()

	status, headers, body := p.request(t, http.MethodGet, "/metrics", nil)
	if status != 200 || !strings.Contains(headers.Get("Content-Type"), "text/plain") {
		t.Fatalf("scrape: %d %s %s", status, headers.Get("Content-Type"), body)
	}

	return string(body)
}

func sample(t *testing.T, body, name string) float64 {
	t.Helper()

	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, name+" ") {
			value, err := strconv.ParseFloat(strings.TrimPrefix(line, name+" "), 64)
			if err != nil {
				t.Fatal(err)
			}

			return value
		}
	}

	t.Fatalf("missing metric %s", name)

	return 0
}

func TestMetricsLifecycle(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)

	worker := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hold":
			select {
			case <-release:

			case <-r.Context().Done():
				return
			}

		case "/retry":
			if r.Header.Get("X-Task-Attempt") == "1" {
				w.WriteHeader(503)
				return
			}

		case "/fail":
			w.WriteHeader(503)
			return

		case "/timeout":
			<-r.Context().Done()
			return
		}

		w.WriteHeader(204)
	})

	p := startTasker(t, "", map[string]string{"max_concurrent_dispatches": "1", "max_attempts": "2"}, enableMetrics)
	initial := scrapeMetrics(t, p)
	for _, name := range []string{"go_goroutines", "go_memstats_alloc_bytes", "process_cpu_seconds_total", "process_resident_memory_bytes"} {
		_ = sample(t, initial, name)
	}

	hold := p.create(t, worker.server.URL+"/hold", `{}`, nil)
	assertDelivery(t, worker.next(t, waitTimeout), hold, 1, "/hold")

	future := p.create(t, worker.server.URL+"/future", `{}`, map[string]any{"scheduleTime": time.Now().Add(time.Hour)})
	past := p.create(t, worker.server.URL+"/past", `{}`, map[string]any{"scheduleTime": time.Now().Add(-30 * time.Second), "kind": "private-kind"})

	queued := scrapeMetrics(t, p)
	for _, state := range []string{"due", "scheduled", "running"} {
		if got := sample(t, queued, `tasker_queue_tasks{state="`+state+`"}`); got != 1 {
			t.Fatalf("%s = %v", state, got)
		}
	}

	if got := sample(t, queued, "tasker_deliveries_in_flight"); got != 1 {
		t.Fatalf("in flight = %v", got)
	}

	if age := sample(t, queued, "tasker_queue_oldest_due_age_seconds"); age < 30 || age > 40 {
		t.Fatalf("oldest due age = %v", age)
	}

	finish()
	assertDelivery(t, worker.next(t, waitTimeout), past, 1, "/past")
	p.awaitStatus(t, hold.ID, "succeeded", waitTimeout)
	p.awaitStatus(t, past.ID, "succeeded", waitTimeout)

	for _, scenario := range []struct{ path, status string }{{"/retry", "succeeded"}, {"/fail", "failed"}, {"/timeout", "failed"}} {
		overrides := map[string]any{}
		if scenario.path == "/timeout" {
			overrides["dispatchDeadline"] = "50ms"
		}

		created := p.create(t, worker.server.URL+scenario.path, `{}`, overrides)
		for attempt := 1; attempt <= 2; attempt++ {
			assertDelivery(t, worker.next(t, waitTimeout), created, attempt, scenario.path)
		}

		p.awaitStatus(t, created.ID, scenario.status, waitTimeout)
	}

	// A database commit can become visible just before the counter increments.
	var body string
	eventually(t, waitTimeout, "metrics reflect all persisted outcomes", func() bool {
		body = scrapeMetrics(t, p)

		return sample(t, body, `tasker_tasks_finished_total{outcome="failed"}`) == 2
	})

	for name, want := range map[string]float64{
		"tasker_tasks_enqueued_total":                                           6,
		`tasker_tasks_finished_total{outcome="succeeded"}`:                      3,
		`tasker_delivery_attempts_total{outcome="success"}`:                     3,
		`tasker_delivery_attempts_total{outcome="http_error"}`:                  3,
		`tasker_delivery_attempts_total{outcome="timeout"}`:                     2,
		"tasker_retries_total":                                                  3,
		"tasker_delivery_duration_seconds_count":                                8,
		"tasker_queue_delay_seconds_count":                                      8,
		"tasker_deliveries_in_flight":                                           0,
		`tasker_queue_tasks{state="due"}`:                                       0,
		`tasker_queue_tasks{state="scheduled"}`:                                 1,
		`tasker_queue_tasks{state="running"}`:                                   0,
		`tasker_queue_tasks{state="expired"}`:                                   0,
		"tasker_queue_oldest_due_age_seconds":                                   0,
		"tasker_queue_stats_up":                                                 1,
		`tasker_http_requests_total{method="POST",route="/tasks",status="201"}`: 6,
	} {
		if got := sample(t, body, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	for _, private := range []string{hold.ID, future.ID, past.ID, worker.server.URL, "private-kind", `route="/metrics"`} {
		if strings.Contains(body, private) {
			t.Errorf("metric contains %q", private)
		}
	}
}

func TestMetricsOptInAndRestart(t *testing.T) {
	t.Parallel()

	database := filepath.Join(t.TempDir(), "tasks.db")
	p := startTasker(t, database, nil)
	if status, _, _ := p.request(t, "GET", "/metrics", nil); status != 404 {
		t.Fatalf("metrics enabled by default: %d", status)
	}

	p.stop(t, false)

	p = startTasker(t, database, nil, enableMetrics)
	p.create(t, "http://localhost:1/future", `{}`, map[string]any{"scheduleTime": time.Now().Add(time.Hour)})

	before := scrapeMetrics(t, p)
	if got := sample(t, before, "tasker_tasks_enqueued_total"); got != 1 {
		t.Fatalf("enqueued = %v", got)
	}

	p.stop(t, true)

	p = startTasker(t, database, nil, enableMetrics)

	after := scrapeMetrics(t, p)
	if got := sample(t, after, "tasker_tasks_enqueued_total"); got != 0 {
		t.Fatalf("process counter did not reset: %v", got)
	}

	if got := sample(t, after, `tasker_queue_tasks{state="scheduled"}`); got != 1 {
		t.Fatalf("persisted queue gauge did not survive restart: %v", got)
	}
}

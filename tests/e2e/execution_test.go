package e2e_test

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecutionLifecycle(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			_, _ = io.WriteString(w, `{"report":"ready","rows":42}`)
		case <-r.Context().Done():
		}
	})
	p := startTasker(t, "", nil)
	created := p.create(t, e.server.URL+"/reports?format=json", `{"account":"abc","month":10}`, map[string]any{"kind": "report"})
	if created.Kind != "report" || created.DispatchDeadline != "5s" || created.MaxAttempts != 3 {
		t.Fatalf("creation metadata/defaults: %+v", created)
	}
	assertDelivery(t, e.next(t, waitTimeout), created, 1, "/reports?format=json")
	running := p.get(t, created.ID)
	if running.Status != "running" || running.Attempt != 1 || running.StartedAt == nil || running.FinishedAt != nil {
		t.Fatalf("expected running task while executor is blocked: %+v", running)
	}
	finish()
	result := p.awaitStatus(t, created.ID, "succeeded", waitTimeout)
	assertTerminal(t, result, "succeeded", 1)
	assertJSON(t, result.Result, `{"report":"ready","rows":42}`)
	assertJSON(t, result.Input, string(created.Input))
	if !result.StartedAt.Equal(*running.StartedAt) {
		t.Error("startedAt changed at completion")
	}
	e.quiet(t, 1200*time.Millisecond)
}

func TestPayloadsAndResults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, input, response, result string
		status                        int
	}{
		{"object", `{"nested":[1,true,null]}`, `{"answer":42}`, `{"answer":42}`, 200},
		{"array", `[1,"two"]`, `[false,3]`, `[false,3]`, 201},
		{"string", `"hello"`, `"done"`, `"done"`, 200},
		{"number", `9007199254740993`, `9007199254740993`, `9007199254740993`, 200},
		{"boolean", `false`, `true`, `true`, 200},
		{"null", `null`, `null`, `null`, 200},
		{"whitespace", `{}`, " \n {\"ok\":true} \t", `{"ok":true}`, 200},
		{"empty", `{}`, "", `null`, 200},
		{"no_content", `{}`, "", `null`, 204},
		{"malformed_success", `{}`, "not JSON", `null`, 200},
		{"at_result_limit", `{}`, `"` + strings.Repeat("x", (1<<20)-2) + `"`, `"` + strings.Repeat("x", (1<<20)-2) + `"`, 200},
		{"oversized_success", `{}`, `"` + strings.Repeat("x", (1<<20)-1) + `"`, `null`, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newExecutor(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.response)
			})
			p := startTasker(t, "", nil)
			created := p.create(t, e.server.URL, tc.input, nil)
			assertDelivery(t, e.next(t, waitTimeout), created, 1, "/")
			result := p.awaitStatus(t, created.ID, "succeeded", waitTimeout)
			assertTerminal(t, result, "succeeded", 1)
			assertJSON(t, result.Result, tc.result)
			// Also observe a poll cycle: malformed/oversized success must not retry.
			e.quiet(t, 1200*time.Millisecond)
		})
	}
}

func TestScheduling(t *testing.T) {
	t.Parallel()
	t.Run("future_discovered_without_another_enqueue", func(t *testing.T) {
		e := newExecutor(t, nil)
		p := startTasker(t, "", nil)
		due := time.Now().Add(1500 * time.Millisecond).Truncate(time.Microsecond)
		created := p.create(t, e.server.URL, `{}`, map[string]any{"scheduleTime": due})
		if !created.ScheduleTime.Equal(due) {
			t.Fatalf("schedule = %s, want %s", created.ScheduleTime, due)
		}
		queued := p.get(t, created.ID)
		if queued.Status != "queued" || queued.Attempt != 0 {
			t.Fatalf("future task was claimed early: %+v", queued)
		}
		d := e.next(t, waitTimeout)
		assertDelivery(t, d, created, 1, "/")
		if d.At.Before(due) {
			t.Errorf("delivered at %s before schedule %s", d.At, due)
		}
		assertTerminal(t, p.awaitStatus(t, created.ID, "succeeded", waitTimeout), "succeeded", 1)
	})
	t.Run("past_is_immediately_eligible", func(t *testing.T) {
		e := newExecutor(t, nil)
		p := startTasker(t, "", nil)
		created := p.create(t, e.server.URL, `{}`, map[string]any{"scheduleTime": time.Now().Add(-time.Hour)})
		assertDelivery(t, e.next(t, waitTimeout), created, 1, "/")
		assertTerminal(t, p.awaitStatus(t, created.ID, "succeeded", waitTimeout), "succeeded", 1)
	})
}

func TestRetryThenSuccess(t *testing.T) {
	t.Parallel()
	replies := make(chan int)
	e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case code := <-replies:
			w.WriteHeader(code)
			_, _ = io.WriteString(w, `{"recovered":true}`)
		case <-r.Context().Done():
		}
	})
	p := startTasker(t, "", map[string]string{"min_backoff": "500ms", "max_backoff": "750ms", "max_attempts": "4"})
	created := p.create(t, e.server.URL, `{"work":"retry"}`, nil)
	var firstStarted time.Time
	nextEligible := created.ScheduleTime
	for attempt := 1; attempt <= 4; attempt++ {
		d := e.next(t, waitTimeout)
		assertDelivery(t, d, created, attempt, "/")
		if d.At.Before(nextEligible) {
			t.Errorf("attempt %d delivered at %s before schedule %s", attempt, d.At, nextEligible)
		}
		running := p.get(t, created.ID)
		if running.Status != "running" || running.Attempt != attempt || running.StartedAt == nil {
			t.Fatalf("attempt %d is not running: %+v", attempt, running)
		}
		if attempt == 1 {
			firstStarted = *running.StartedAt
		} else if !running.StartedAt.Equal(firstStarted) {
			t.Error("startedAt changed across retries")
		}
		code := http.StatusServiceUnavailable
		if attempt == 4 {
			code = http.StatusOK
		}
		select {
		case replies <- code:
		case <-time.After(waitTimeout):
			t.Fatal("executor did not accept response instruction")
		}
		if attempt < 4 {
			queued := p.awaitStatus(t, created.ID, "queued", waitTimeout)
			if queued.Attempt != attempt || !strings.Contains(queued.LastError, "503") || queued.FinishedAt != nil {
				t.Fatalf("incorrect retry state: %+v", queued)
			}
			// scheduleTime is computed just before the persistence timestamp.
			delay := queued.ScheduleTime.Sub(queued.UpdatedAt)
			upper := min(time.Duration(1<<(attempt-1))*500*time.Millisecond, 750*time.Millisecond)
			if delay < 400*time.Millisecond || delay > upper+50*time.Millisecond {
				t.Errorf("retry delay %s outside expected 500ms..%s (with persistence tolerance)", delay, upper)
			}
			nextEligible = queued.ScheduleTime
		}
	}
	result := p.awaitStatus(t, created.ID, "succeeded", waitTimeout)
	assertTerminal(t, result, "succeeded", 4)
	assertJSON(t, result.Result, `{"recovered":true}`)
	if !result.StartedAt.Equal(firstStarted) {
		t.Error("startedAt changed after final success")
	}
	e.quiet(t, 1200*time.Millisecond)
}

func TestDeliveryFailures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		attempts int
		error    string
		respond  func(http.ResponseWriter, *http.Request)
	}{
		{"server_error", 3, "HTTP 503", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }},
		{"client_error_is_retried", 2, "HTTP 400", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400) }},
		{"one_attempt", 1, "HTTP 500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }},
		{"truncated_response", 2, "read executor response", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, `{`)
		}},
		{"deadline", 2, "context deadline exceeded", func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}},
		{"transport_disconnect", 2, "executor request", func(w http.ResponseWriter, _ *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cancellations := make(chan time.Time, tc.attempts)
			e := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
				tc.respond(w, r)
				if tc.name == "deadline" {
					select {
					case cancellations <- time.Now():
					default:
					}
				}
			})
			p := startTasker(t, "", map[string]string{"max_attempts": strconv.Itoa(tc.attempts)})
			fields := map[string]any{}
			if tc.name == "deadline" {
				fields["dispatchDeadline"] = "250ms"
			}
			created := p.create(t, e.server.URL, `{"work":"fail"}`, fields)
			for attempt := 1; attempt <= tc.attempts; attempt++ {
				d := e.next(t, waitTimeout)
				assertDelivery(t, d, created, attempt, "/")
				if tc.name == "deadline" {
					select {
					case canceledAt := <-cancellations:
						if canceledAt.Sub(d.At) > 2*time.Second {
							t.Error("250ms per-task deadline did not override the 5s default")
						}
					case <-time.After(2 * time.Second):
						t.Fatal("executor request was not canceled by the per-task deadline")
					}
				}
			}
			result := p.awaitStatus(t, created.ID, "failed", waitTimeout)
			assertTerminal(t, result, "failed", tc.attempts)
			assertJSON(t, result.Result, `null`)
			if !strings.Contains(result.LastError, tc.error) {
				t.Errorf("lastError = %q, want %q", result.LastError, tc.error)
			}
			if tc.name == "deadline" && (result.DispatchDeadline != "250ms" || created.DispatchDeadline != "250ms") {
				t.Error("per-task deadline was not preserved")
			}
			e.quiet(t, 1200*time.Millisecond)
		})
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	t.Parallel()
	destination := newExecutor(t, nil)
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			e := newExecutor(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", destination.server.URL+"/redirected")
				w.WriteHeader(code)
			})
			p := startTasker(t, "", map[string]string{"max_attempts": "2"})
			created := p.create(t, e.server.URL, `{}`, nil)
			for attempt := 1; attempt <= 2; attempt++ {
				assertDelivery(t, e.next(t, waitTimeout), created, attempt, "/")
			}
			result := p.awaitStatus(t, created.ID, "failed", waitTimeout)
			assertTerminal(t, result, "failed", 2)
			if !strings.Contains(result.LastError, fmt.Sprintf("HTTP %d", code)) {
				t.Errorf("redirect error = %q", result.LastError)
			}
		})
	}
	destination.quiet(t, 1200*time.Millisecond)
}

func TestValidation(t *testing.T) {
	t.Parallel()
	e := newExecutor(t, nil)
	p := startTasker(t, "", nil)
	valid := fmt.Sprintf(`{"url":%q,"input":{}}`, e.server.URL)
	cases := []struct {
		name, body string
		status     int
	}{
		{"empty", "", 400},
		{"malformed", `{`, 400},
		{"array", `[]`, 400},
		{"null_request", `null`, 400},
		{"missing_url", `{"input":{}}`, 400},
		{"relative_url", `{"url":"/relative","input":{}}`, 400},
		{"unsupported_scheme", `{"url":"ftp://example.com/task","input":{}}`, 400},
		{"missing_host", `{"url":"http:///task","input":{}}`, 400},
		{"missing_input", fmt.Sprintf(`{"url":%q}`, e.server.URL), 400},
		{"unknown_field", strings.TrimSuffix(valid, "}") + `,"maxAttempts":1}`, 400},
		{"multiple_values", valid + ` {}`, 400},
		{"invalid_schedule", strings.TrimSuffix(valid, "}") + `,"scheduleTime":"tomorrow"}`, 400},
		{"oversized", fmt.Sprintf(`{"url":%q,"input":"%s"}`, e.server.URL, strings.Repeat("x", 1<<20)), 413},
	}
	for _, deadline := range []string{`""`, `"0s"`, `"-1s"`, `"soon"`, `null`, `42`} {
		cases = append(cases, struct {
			name, body string
			status     int
		}{"deadline_" + deadline, strings.TrimSuffix(valid, "}") + `,"dispatchDeadline":` + deadline + `}`, 400})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, body := p.request(t, http.MethodPost, "/tasks", []byte(tc.body))
			if status != tc.status {
				t.Errorf("HTTP %d, want %d: %s", status, tc.status, body)
			}
		})
	}
	status, _, _ := p.request(t, http.MethodGet, "/tasks/does-not-exist", nil)
	if status != http.StatusNotFound {
		t.Errorf("unknown task: HTTP %d, want 404", status)
	}
	e.quiet(t, 1200*time.Millisecond)
	// A valid request after the rejected requests proves the dispatcher is alive.
	created := p.create(t, e.server.URL, `null`, nil)
	assertDelivery(t, e.next(t, waitTimeout), created, 1, "/")
	assertTerminal(t, p.awaitStatus(t, created.ID, "succeeded", waitTimeout), "succeeded", 1)
}

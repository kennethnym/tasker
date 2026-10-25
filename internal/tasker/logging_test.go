package tasker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type logStorage struct {
	Storage
	writeErr error
}

func (s logStorage) RenewLease(context.Context, Lease, time.Duration) error { return nil }

func (s logStorage) Complete(context.Context, Lease, json.RawMessage) error { return s.writeErr }
func (s logStorage) Retry(context.Context, Lease, time.Time, string) error  { return s.writeErr }

func (s logStorage) Fail(context.Context, Lease, string) error { return s.writeErr }
func (s logStorage) Get(context.Context, string) (Task, error) {
	return Task{}, errors.New("database unavailable")
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOutcomeLogsFollowPersistence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		attempt     int
		deliveryErr error
		writeErr    error
		want        string
	}{
		{name: "success", attempt: 1, want: "task succeeded"},
		{name: "retry", attempt: 1, deliveryErr: errors.New("connection refused"), want: "task retry scheduled"},
		{name: "exhausted", attempt: 3, deliveryErr: errors.New("connection refused"), want: "task attempts exhausted"},
		{name: "write failure", attempt: 1, writeErr: errors.New("disk full"), want: "failed to persist task outcome"},
		{name: "lease lost", attempt: 1, writeErr: ErrLeaseLost, want: "task outcome discarded after lease lost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			e := newExecutor(Config{Storage: logStorage{writeErr: tc.writeErr}, MinBackoff: time.Second, MaxBackoff: time.Second}, time.Minute)
			e.logger = slog.New(slog.NewJSONHandler(&output, nil))
			e.client.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
				if tc.deliveryErr != nil {
					return nil, tc.deliveryErr
				}

				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
			})

			e.execute(context.Background(), Task{ID: "task-1", Attempt: tc.attempt, MaxAttempts: 3, URL: "https://user:password@example.com/work?token=secret", DispatchDeadline: "1s"})

			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatalf("expected exactly one JSON outcome: %s: %v", output.String(), err)
			}

			if record["msg"] != tc.want || record["task_id"] != "task-1" || record["attempt"] != float64(tc.attempt) {
				t.Fatalf("unexpected record: %v", record)
			}

			if tc.name == "retry" && record["retry_at"] == nil {
				t.Fatal("missing retry time")
			}

			for _, secret := range []string{"password", "secret", "example.com"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("target leaked: %s", output.String())
				}
			}
		})
	}
}

func TestAccessLogsAndErrorCorrelation(t *testing.T) {
	var output bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(old)
	for _, enabled := range []bool{false, true} {
		output.Reset()
		service, err := New(Config{Storage: logStorage{}, AccessLog: enabled})
		if err != nil {
			t.Fatal(err)
		}

		request := httptest.NewRequest("GET", "/tasks/task-1?token=secret", nil)
		request.Header.Set("Authorization", "Bearer password")

		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, request)
		if response.Code != 500 || response.Body.String() != "failed to find task\n" {
			t.Fatalf("response changed: %v", response)
		}

		id := response.Header().Get("X-Request-Id")
		for _, secret := range []string{"secret", "password"} {
			if strings.Contains(output.String(), secret) {
				t.Fatalf("request data leaked: %s", output.String())
			}
		}

		decoder := json.NewDecoder(&output)
		count := 0
		for decoder.More() {

			var record map[string]any
			if err := decoder.Decode(&record); err != nil {
				t.Fatal(err)
			}

			if id == "" || record["request_id"] != id {
				t.Fatalf("missing correlation: %v", record)
			}

			if record["component"] == "http.access" && (record["status"] != float64(500) || record["route"] != "GET /tasks/{id}") {
				t.Fatalf("incorrect access record: %v", record)
			}
			count++
		}

		want := 1
		if enabled {
			want = 2
		}

		if count != want {
			t.Fatalf("got %d logs, want %d", count, want)
		}
	}
}

func TestMalformedRedirectDoesNotLeakIntoLogs(t *testing.T) {
	const location = "https://user:password@example.com/%zz?token=secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	for _, attempt := range []int{1, 3} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			var output bytes.Buffer
			e := newExecutor(Config{
				Storage:    logStorage{},
				MinBackoff: time.Second,
				MaxBackoff: time.Second,
			}, time.Minute)
			e.logger = slog.New(slog.NewJSONHandler(&output, nil))

			e.execute(context.Background(), Task{
				ID:               "redirect-task",
				Attempt:          attempt,
				MaxAttempts:      3,
				URL:              server.URL,
				DispatchDeadline: "1s",
			})

			for _, secret := range []string{"user", "password", "example.com", "%zz", "token", "secret"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("redirect leaked %q into logs: %s", secret, output.String())
				}
			}

			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}

			if record["error"] != "request_failed" {
				t.Fatalf("expected safe request failure category, got %v", record)
			}
		})
	}
}

func TestDeliveryLogErrorCategories(t *testing.T) {
	const secret = "https://user:password@example.com/?token=secret"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"unknown", errors.New(secret), "delivery_failed"},
		{"response read", fmt.Errorf("read executor response: %w", errors.New(secret)), "delivery_failed"},
		{"nested URL", &url.Error{Op: secret, URL: secret, Err: &url.Error{Op: secret, URL: secret, Err: errors.New(secret)}}, "request_failed"},
		{"deadline", fmt.Errorf("%s: %w", secret, context.DeadlineExceeded), "deadline_exceeded"},
		{"canceled", fmt.Errorf("%s: %w", secret, context.Canceled), "canceled"},
		{"invalid deadline", ErrInvalidDispatchDeadline, "invalid_dispatch_deadline"},
		{"HTTP status", deliveryStatusError(http.StatusServiceUnavailable), "http_status_503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deliveryLogError(tc.err); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

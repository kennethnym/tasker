package tasker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type metricsTestStorage struct {
	Storage
	stats                                              QueueStats
	statsErr, renewErr, completeErr, retryErr, failErr error
	claim                                              ClaimResult
	claimErr                                           error
}

func (s *metricsTestStorage) QueueStats(context.Context, time.Time) (QueueStats, error) {
	return s.stats, s.statsErr
}

func (s *metricsTestStorage) RenewLease(context.Context, Lease, time.Duration) error {
	return s.renewErr
}

func (s *metricsTestStorage) Complete(context.Context, Lease, json.RawMessage) error {
	return s.completeErr
}

func (s *metricsTestStorage) Retry(context.Context, Lease, time.Time, string) error {
	return s.retryErr
}

func (s *metricsTestStorage) Fail(context.Context, Lease, string) error {
	return s.failErr
}

func (s *metricsTestStorage) ClaimDue(context.Context, time.Duration) (ClaimResult, error) {
	return s.claim, s.claimErr
}

func gatherMetrics(t *testing.T, m *metrics) map[string]*dto.MetricFamily {
	t.Helper()

	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}

	return result
}

func metricValue(t *testing.T, families map[string]*dto.MetricFamily, name, label, value string) float64 {
	t.Helper()

	for _, metric := range families[name].GetMetric() {
		matched := label == ""
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == label && pair.GetValue() == value {
				matched = true
			}
		}

		if matched {
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}

			return metric.Gauge.GetValue()
		}
	}

	t.Fatalf("missing metric %s{%s=%q}", name, label, value)

	return 0
}

func TestDeliveryAndPersistenceMetrics(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		status, attempt                 int
		renewErr, completeErr, retryErr error
		outcome                         string
		finished, retries, delivered    float64
	}{
		{name: "success", status: 204, attempt: 1, outcome: "success", finished: 1, delivered: 1},
		{name: "success not persisted", status: 204, attempt: 1, completeErr: errors.New("disk failure"), outcome: "success", delivered: 1},
		{name: "success fenced", status: 204, attempt: 1, completeErr: ErrLeaseLost, outcome: "success", delivered: 1},
		{name: "retry", status: 503, attempt: 1, outcome: "http_error", retries: 1, delivered: 1},
		{name: "retry not persisted", status: 503, attempt: 1, retryErr: errors.New("disk failure"), outcome: "http_error", delivered: 1},
		{name: "exhausted", status: 503, attempt: 2, outcome: "http_error", finished: 1, delivered: 1},
		{name: "ownership lost", renewErr: ErrLeaseLost, outcome: "success"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer worker.Close()

			storage := &metricsTestStorage{renewErr: tc.renewErr, completeErr: tc.completeErr, retryErr: tc.retryErr}
			m := newMetrics(storage)
			config := DefaultConfig.Merge(Config{Storage: &instrumentedStorage{Storage: storage, metrics: m}})
			executor := newExecutor(config, 30*time.Second, m)

			executor.execute(context.Background(), ClaimResult{
				Task: Task{ID: "task", URL: worker.URL, Input: []byte(`{}`), DispatchDeadline: "1s", Attempt: tc.attempt, MaxAttempts: 2,
					CreatedAt: time.Now().Add(-time.Hour), ScheduleTime: time.Now().Add(-time.Hour)},
				ReadyAt: time.Now().Add(-2 * time.Second), Recovered: true,
			})

			families := gatherMetrics(t, m)
			finishedOutcome := "succeeded"
			if tc.status == 503 {
				finishedOutcome = "failed"
			}

			for _, check := range []struct {
				name, label, value string
				want               float64
			}{
				{"tasker_delivery_attempts_total", "outcome", tc.outcome, tc.delivered},
				{"tasker_tasks_finished_total", "outcome", finishedOutcome, tc.finished},
				{"tasker_retries_total", "", "", tc.retries},
				{"tasker_deliveries_in_flight", "", "", 0},
			} {
				if got := metricValue(t, families, check.name, check.label, check.value); got != check.want {
					t.Errorf("%s = %v, want %v", check.name, got, check.want)
				}
			}

			for operation, err := range map[string]error{"renew": tc.renewErr, "complete": tc.completeErr, "retry": tc.retryErr} {
				var wantErrors, wantLost float64
				if errors.Is(err, ErrLeaseLost) {
					wantLost = 1
				} else if err != nil {
					wantErrors = 1
				}

				if got := metricValue(t, families, "tasker_storage_errors_total", "operation", operation); got != wantErrors {
					t.Errorf("%s errors = %v", operation, got)
				}

				if got := metricValue(t, families, "tasker_lease_lost_total", "operation", operation); got != wantLost {
					t.Errorf("%s lost leases = %v", operation, got)
				}
			}

			queue := families["tasker_queue_delay_seconds"].Metric[0].GetHistogram()
			if float64(queue.GetSampleCount()) != tc.delivered {
				t.Fatalf("queue observations = %d", queue.GetSampleCount())
			}

			if tc.delivered > 0 && (queue.GetSampleSum() < 2 || queue.GetSampleSum() > 5) {
				t.Fatalf("queue delay includes prior execution/backoff: %v", queue.GetSampleSum())
			}
		})
	}
}

func TestRecoveryMetricsWithoutClaim(t *testing.T) {
	storage := &metricsTestStorage{claim: ClaimResult{Exhausted: 2}, claimErr: ErrNoTaskDue}
	m := newMetrics(storage)
	observed := &instrumentedStorage{Storage: storage, metrics: m}

	_, _ = observed.ClaimDue(context.Background(), time.Minute)

	families := gatherMetrics(t, m)
	if got := metricValue(t, families, "tasker_tasks_finished_total", "outcome", "failed"); got != 2 {
		t.Fatalf("recovered failures = %v", got)
	}

	if got := metricValue(t, families, "tasker_storage_errors_total", "operation", "claim"); got != 0 {
		t.Fatalf("empty queue counted as error: %v", got)
	}

	storage.claimErr = errors.New("commit failed")
	_, _ = observed.ClaimDue(context.Background(), time.Minute)

	families = gatherMetrics(t, m)
	if got := metricValue(t, families, "tasker_tasks_finished_total", "outcome", "failed"); got != 2 {
		t.Fatalf("uncommitted recovery counted: %v", got)
	}
}

func TestQueueMetricsFailureOmitsSnapshot(t *testing.T) {
	due := time.Now().Add(-time.Minute)
	storage := &metricsTestStorage{stats: QueueStats{Due: 2, OldestDueAt: &due}}
	m := newMetrics(storage)

	if got := metricValue(t, gatherMetrics(t, m), "tasker_queue_tasks", "state", "due"); got != 2 {
		t.Fatalf("due = %v", got)
	}

	storage.statsErr = errors.New("unavailable")

	families := gatherMetrics(t, m)
	if got := metricValue(t, families, "tasker_queue_stats_up", "", ""); got != 0 {
		t.Fatalf("snapshot health = %v", got)
	}

	if families["tasker_queue_tasks"] != nil || families["tasker_queue_oldest_due_age_seconds"] != nil {
		t.Fatal("failed scrape exposed stale or zero queue snapshot")
	}

	// A new service's registry must not inherit the old service's measurements.
	fresh := newMetrics(&metricsTestStorage{})
	if got := metricValue(t, gatherMetrics(t, fresh), "tasker_queue_tasks", "state", "due"); got != 0 {
		t.Fatalf("registry leaked state: %v", got)
	}
}

func TestHTTPMetricsBoundLabelsAndIncludeAuthFailures(t *testing.T) {
	service, err := New(Config{Storage: &metricsTestStorage{}, MetricsEnabled: true, Auth: &BasicAuth{Username: "user", Password: "secret"}})
	if err != nil {
		t.Fatal(err)
	}

	handler := service.Handler()
	for _, path := range []string{"/tasks/private-id-a?token=secret", "/tasks/private-id-b", "/private-path"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("PRIVATE-METHOD", "/private-path", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.SetBasicAuth("user", "secret")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != 200 {
		t.Fatalf("scrape: %d %s", response.Code, response.Body.String())
	}

	for _, private := range []string{"private-id", "private-path", "PRIVATE-METHOD", "secret", `route="/metrics"`} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("metric contains %q", private)
		}
	}

	if !strings.Contains(response.Body.String(), `tasker_http_requests_total{method="GET",route="/tasks/{id}",status="401"} 2`) {
		t.Fatal("missing authenticated route metrics")
	}

	if !strings.Contains(response.Body.String(), `tasker_http_requests_total{method="other",route="unmatched",status="401"} 1`) {
		t.Fatal("unbounded method label")
	}
}

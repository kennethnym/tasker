package tasker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Each Tasker owns its registry; constructing services never registers globals.
type metrics struct {
	registry         *prometheus.Registry
	handler          http.Handler
	enqueued         prometheus.Counter
	finished         *prometheus.CounterVec
	retries          prometheus.Counter
	recoveries       prometheus.Counter
	deliveries       *prometheus.CounterVec
	deliveryDuration prometheus.Histogram
	queueDelay       prometheus.Histogram
	inFlight         prometheus.Gauge
	storageErrors    *prometheus.CounterVec
	leaseLost        *prometheus.CounterVec
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
}

func newMetrics(storage Storage) *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		enqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasker_tasks_enqueued_total",
			Help: "Tasks successfully persisted by this process.",
		}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasker_tasks_finished_total",
			Help: "Terminal task outcomes persisted by this process, including expired final leases.",
		}, []string{"outcome"}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasker_retries_total",
			Help: "Retry schedules successfully persisted by this process.",
		}),
		recoveries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tasker_lease_recoveries_total",
			Help: "Expired leases successfully reclaimed for another attempt by this process.",
		}),
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasker_delivery_attempts_total",
			Help: "Delivery attempts that returned in this process, independent of outcome persistence.",
		}, []string{"outcome"}),
		deliveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tasker_delivery_duration_seconds",
			Help:    "Time spent delivering an attempt, including reading its response, excluding persistence.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}),
		queueDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tasker_queue_delay_seconds",
			Help:    "Time from schedule time (previous lease expiry for recovery) to delivery start; excludes intentional scheduling and retry backoff.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tasker_deliveries_in_flight",
			Help: "Delivery attempts currently executing in this process; excludes claims awaiting ownership confirmation or persistence.",
		}),
		storageErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasker_storage_errors_total",
			Help: "Storage operation errors, excluding expected empty results, lost leases, and cancellation.",
		}, []string{"operation"}),
		leaseLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasker_lease_lost_total",
			Help: "Storage operations rejected because lease ownership was lost.",
		}, []string{"operation"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasker_http_requests_total",
			Help: "API requests, including authentication failures and unmatched routes; excludes /metrics.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tasker_http_request_duration_seconds",
			Help:    "API request duration; excludes /metrics.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}

	m.registry.MustRegister(m.enqueued, m.finished, m.retries, m.recoveries,
		m.deliveries, m.deliveryDuration, m.queueDelay, m.inFlight,
		m.storageErrors, m.leaseLost, m.httpRequests, m.httpDuration,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	for _, outcome := range []string{"succeeded", "failed"} {
		m.finished.WithLabelValues(outcome)
	}

	for _, outcome := range []string{"success", "http_error", "timeout", "canceled", "invalid_task", "network_error", "response_error"} {
		m.deliveries.WithLabelValues(outcome)
	}

	for _, operation := range []string{"create", "get", "claim", "renew", "complete", "retry", "fail", "cleanup", "queue_stats"} {
		m.storageErrors.WithLabelValues(operation)
	}

	for _, operation := range []string{"renew", "complete", "retry", "fail"} {
		m.leaseLost.WithLabelValues(operation)
	}

	m.registry.MustRegister(&queueCollector{
		storage: storage,
		metrics: m,
		tasks:   prometheus.NewDesc("tasker_queue_tasks", "Persisted tasks by queue state: due, scheduled, running with a valid lease, or expired awaiting recovery.", []string{"state"}, nil),
		oldest:  prometheus.NewDesc("tasker_queue_oldest_due_age_seconds", "Age past schedule time of the oldest due queued task, or zero when none are due; excludes running and expired leases.", nil, nil),
		up:      prometheus.NewDesc("tasker_queue_stats_up", "Whether the database queue snapshot succeeded in this scrape (1 or 0).", nil, nil),
	})

	m.handler = promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics:   true,
		MaxRequestsInFlight: 1,
	})

	return m
}

type queueCollector struct {
	storage           Storage
	metrics           *metrics
	tasks, oldest, up *prometheus.Desc
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.oldest
	ch <- c.up
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	// Bound contention with the single SQLite connection. Scrapes never change
	// queue state, and a failed snapshot never emits misleading zero counts.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	now := time.Now().UTC()
	stats, err := c.storage.QueueStats(ctx, now)
	if err != nil {
		c.metrics.storageResult("queue_stats", err)
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)

	for _, state := range []struct {
		name  string
		count int64
	}{
		{"due", stats.Due},
		{"scheduled", stats.Scheduled},
		{"running", stats.Running},
		{"expired", stats.Expired},
	} {
		ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(state.count), state.name)
	}

	var age float64
	if stats.OldestDueAt != nil {
		age = max(0, now.Sub(*stats.OldestDueAt).Seconds())
	}

	ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, age)
}

func deliveryOutcome(err error) string {
	if err == nil {
		return "success"
	}

	if errors.Is(err, context.Canceled) {
		return "canceled"
	}

	var network net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &network) && network.Timeout()) {
		return "timeout"
	}

	var status deliveryStatusError
	if errors.As(err, &status) {
		return "http_error"
	}

	if errors.Is(err, ErrInvalidDispatchDeadline) || errors.Is(err, ErrInvalidTaskURL) {
		return "invalid_task"
	}

	var request *url.Error
	if errors.As(err, &request) {
		return "network_error"
	}

	return "response_error"
}

func (m *metrics) storageResult(operation string, err error) {
	switch {
	case err == nil, errors.Is(err, ErrNoTaskDue), errors.Is(err, ErrTaskNotFound), errors.Is(err, context.Canceled):

	case errors.Is(err, ErrLeaseLost):
		m.leaseLost.WithLabelValues(operation).Inc()

	default:
		m.storageErrors.WithLabelValues(operation).Inc()
	}
}

// Count persisted transitions only after the adapter confirms its commit.
type instrumentedStorage struct {
	Storage
	metrics *metrics
}

func (s *instrumentedStorage) Create(ctx context.Context, task Task) error {
	err := s.Storage.Create(ctx, task)
	s.metrics.storageResult("create", err)
	if err == nil {
		s.metrics.enqueued.Inc()
	}

	return err
}

func (s *instrumentedStorage) Get(ctx context.Context, id string) (Task, error) {
	task, err := s.Storage.Get(ctx, id)
	s.metrics.storageResult("get", err)

	return task, err
}

func (s *instrumentedStorage) ClaimDue(ctx context.Context, duration time.Duration) (ClaimResult, error) {
	claim, err := s.Storage.ClaimDue(ctx, duration)
	s.metrics.storageResult("claim", err)
	if err == nil || errors.Is(err, ErrNoTaskDue) {
		s.metrics.finished.WithLabelValues("failed").Add(float64(claim.Exhausted))
	}

	if err == nil && claim.Recovered {
		s.metrics.recoveries.Inc()
	}

	return claim, err
}

func (s *instrumentedStorage) RenewLease(ctx context.Context, lease Lease, duration time.Duration) error {
	err := s.Storage.RenewLease(ctx, lease, duration)
	s.metrics.storageResult("renew", err)

	return err
}

func (s *instrumentedStorage) Complete(ctx context.Context, lease Lease, result json.RawMessage) error {
	err := s.Storage.Complete(ctx, lease, result)
	s.metrics.storageResult("complete", err)
	if err == nil {
		s.metrics.finished.WithLabelValues("succeeded").Inc()
	}

	return err
}

func (s *instrumentedStorage) Retry(ctx context.Context, lease Lease, at time.Time, message string) error {
	err := s.Storage.Retry(ctx, lease, at, message)
	s.metrics.storageResult("retry", err)
	if err == nil {
		s.metrics.retries.Inc()
	}

	return err
}

func (s *instrumentedStorage) Fail(ctx context.Context, lease Lease, message string) error {
	err := s.Storage.Fail(ctx, lease, message)
	s.metrics.storageResult("fail", err)
	if err == nil {
		s.metrics.finished.WithLabelValues("failed").Inc()
	}

	return err
}

func (s *instrumentedStorage) DeleteFinishedBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	count, err := s.Storage.DeleteFinishedBefore(ctx, cutoff, limit)
	s.metrics.storageResult("cleanup", err)

	return count, err
}

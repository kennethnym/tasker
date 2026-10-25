// Package tasker defines a durable HTTP task service with at-least-once delivery.
// Executors are responsible for idempotency.
package tasker

import (
	"context"
	"encoding/json"
	"net/url"
	"time"
)

type Tasker struct {
	config     Config
	dispatcher *dispatcher
	cleanup    *cleanupReconciler
}

func New(config Config) (*Tasker, error) {
	config, err := validateAndNormalizeConfig(config)
	if err != nil {
		return nil, err
	}

	return &Tasker{
		config:     config,
		dispatcher: newDispatcher(config),
		cleanup:    &cleanupReconciler{storage: config.Storage, retention: config.TaskRetention},
	}, nil
}

func (t *Tasker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.cleanup.run(ctx)
	}()
	err := t.dispatcher.run(ctx)
	cancel()
	<-done
	return err
}

func (t *Tasker) CreateTask(ctx context.Context, config CreateTaskConfig) (Task, error) {
	u, err := url.Parse(config.URL)
	if err != nil {
		return Task{}, ErrInvalidTaskURL
	}

	if _, err := Origin(u); err != nil {
		return Task{}, ErrInvalidTaskURL
	}

	if !json.Valid(config.Input) {
		return Task{}, ErrInvalidTaskInput
	}

	deadline := t.config.DispatchDeadline
	if config.DispatchDeadline != "" {
		deadline, err = time.ParseDuration(config.DispatchDeadline)
		if err != nil || deadline <= 0 {
			return Task{}, ErrInvalidDispatchDeadline
		}
	}

	now := time.Now().UTC()
	scheduleTime := now
	if config.ScheduleTime != nil {
		scheduleTime = config.ScheduleTime.UTC()
	}

	task := Task{
		ID:               NewTaskID(),
		Kind:             config.Kind,
		URL:              config.URL,
		Input:            append(json.RawMessage(nil), config.Input...),
		Status:           StatusQueued,
		ScheduleTime:     scheduleTime,
		DispatchDeadline: deadline.String(),
		MaxAttempts:      t.config.MaxAttempts,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if err := t.config.Storage.Create(ctx, task); err != nil {
		return Task{}, err
	}

	t.dispatcher.notify()

	return task, nil
}

func (t *Tasker) FindTask(ctx context.Context, id string) (Task, error) {
	return t.config.Storage.Get(ctx, id)
}

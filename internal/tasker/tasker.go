// Package tasker defines a durable HTTP task service with at-least-once delivery.
// Executors are responsible for idempotency. This package is currently a skeleton.
package tasker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"time"
)

type Tasker struct {
	config     Config
	dispatcher *dispatcher
}

func New(config Config) (*Tasker, error) {
	config = DefaultConfig.Merge(config)

	if config.Storage == nil {
		return nil, fmt.Errorf("tasker: storage is required")
	}

	if config.DispatchDeadline <= 0 || config.MaxAttempts <= 0 {
		return nil, fmt.Errorf("tasker: dispatch deadline and max attempts must be positive")
	}

	if config.MaxConcurrentDispatches <= 0 || config.MaxDispatchesPerSecond <= 0 ||
		math.IsNaN(config.MaxDispatchesPerSecond) || math.IsInf(config.MaxDispatchesPerSecond, 0) {
		return nil, fmt.Errorf("tasker: dispatch rate and concurrency must be positive and finite")
	}

	return &Tasker{config: config, dispatcher: newDispatcher(config)}, nil
}

func (t *Tasker) Run(ctx context.Context) error {
	return t.dispatcher.run(ctx)
}

func (t *Tasker) CreateTask(ctx context.Context, config CreateTaskConfig) (Task, error) {
	u, err := url.Parse(config.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
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

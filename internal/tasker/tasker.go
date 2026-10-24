// Package tasker defines a durable HTTP task service with at-least-once delivery.
// Executors are responsible for idempotency. This package is currently a skeleton.
package tasker

import (
	"context"
)

type Tasker struct {
	config Config
}

func New(config Config) (*Tasker, error) {
	return &Tasker{config: DefaultConfig.Merge(config)}, nil
}

func (t *Tasker) Run(ctx context.Context) error {
	return ErrNotImplemented
}

func (t *Tasker) CreateTask(ctx context.Context, config CreateTaskConfig) (Task, error) {
	return Task{}, ErrNotImplemented
}

func (t *Tasker) FindTask(ctx context.Context, id string) (Task, error) {
	return Task{}, ErrNotImplemented
}

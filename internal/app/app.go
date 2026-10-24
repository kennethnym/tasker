package app

import (
	"time"

	"tasker/internal/tasker"
)

func Run() error {
	service, err := tasker.New(tasker.Config{
		Storage:                 nil,
		MaxDispatchesPerSecond:  10,
		MaxConcurrentDispatches: 20,
		DispatchDeadline:        5 * time.Minute,
		MaxAttempts:             5,
		MinBackoff:              time.Second,
		MaxBackoff:              time.Minute,
		TaskRetention:           7 * 24 * time.Hour,
	})
	if err != nil {
		return err
	}

	_ = service
	return tasker.ErrNotImplemented
}

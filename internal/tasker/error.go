package tasker

import (
	"errors"
	"fmt"
)

var (
	ErrNotImplemented          = errors.New("tasker: not implemented")
	ErrTaskNotFound            = errors.New("tasker: task not found")
	ErrNoTaskDue               = errors.New("tasker: no task due")
	ErrLeaseLost               = errors.New("tasker: lease lost")
	ErrInvalidTaskURL          = errors.New("tasker: url must be an absolute HTTP or HTTPS URL")
	ErrInvalidTaskInput        = errors.New("tasker: input must be a JSON value")
	ErrInvalidDispatchDeadline = errors.New("tasker: dispatchDeadline must be a positive duration")
)

type TaskNotFoundError struct {
	ID string
}

func (e *TaskNotFoundError) Error() string {
	return fmt.Sprintf("%s: %q", ErrTaskNotFound, e.ID)
}

func (e *TaskNotFoundError) Unwrap() error {
	return ErrTaskNotFound
}

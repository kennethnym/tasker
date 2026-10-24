package tasker

import (
	"encoding/json"
	"time"

	"github.com/lucsky/cuid"
)

func NewTaskID() string {
	return cuid.New()
}

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type CreateTaskConfig struct {
	Kind             string          `json:"kind,omitempty"`
	URL              string          `json:"url"`
	Input            json.RawMessage `json:"input"`
	ScheduleTime     *time.Time      `json:"scheduleTime,omitempty"`
	DispatchDeadline string          `json:"dispatchDeadline,omitempty"`
}

type Task struct {
	ID               string          `json:"id"`
	Kind             string          `json:"kind,omitempty"`
	URL              string          `json:"url"`
	Input            json.RawMessage `json:"input"`
	Status           Status          `json:"status"`
	ScheduleTime     time.Time       `json:"scheduleTime"`
	DispatchDeadline string          `json:"dispatchDeadline"`
	Attempt          int             `json:"attempt"`
	MaxAttempts      int             `json:"maxAttempts"`
	Result           json.RawMessage `json:"result"`
	LastError        string          `json:"lastError,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
	StartedAt        *time.Time      `json:"startedAt,omitempty"`
	FinishedAt       *time.Time      `json:"finishedAt,omitempty"`
	LeaseToken       string          `json:"-"`
	LeaseExpiresAt   *time.Time      `json:"-"`
}

package tasker

import "time"

type Config struct {
	Storage Storage

	MaxDispatchesPerSecond  float64
	MaxConcurrentDispatches int
	DispatchDeadline        time.Duration
	MaxAttempts             int
	MinBackoff              time.Duration
	MaxBackoff              time.Duration
	TaskRetention           time.Duration
}

var DefaultConfig = Config{
	MaxDispatchesPerSecond:  10,
	MaxConcurrentDispatches: 20,
	DispatchDeadline:        5 * time.Minute,
	MaxAttempts:             5,
	MinBackoff:              time.Second,
	MaxBackoff:              time.Minute,
	TaskRetention:           7 * 24 * time.Hour,
}

func (c Config) Merge(other Config) Config {
	if other.Storage != nil {
		c.Storage = other.Storage
	}
	if other.MaxDispatchesPerSecond != 0 {
		c.MaxDispatchesPerSecond = other.MaxDispatchesPerSecond
	}
	if other.MaxConcurrentDispatches != 0 {
		c.MaxConcurrentDispatches = other.MaxConcurrentDispatches
	}
	if other.DispatchDeadline != 0 {
		c.DispatchDeadline = other.DispatchDeadline
	}
	if other.MaxAttempts != 0 {
		c.MaxAttempts = other.MaxAttempts
	}
	if other.MinBackoff != 0 {
		c.MinBackoff = other.MinBackoff
	}
	if other.MaxBackoff != 0 {
		c.MaxBackoff = other.MaxBackoff
	}
	if other.TaskRetention != 0 {
		c.TaskRetention = other.TaskRetention
	}
	return c
}

package tasker

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Storage      Storage
	Auth         *BasicAuth
	OutboundAuth map[string]BasicAuth

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
	if other.Auth != nil {
		c.Auth = other.Auth
	}

	if other.OutboundAuth != nil {
		c.OutboundAuth = other.OutboundAuth
	}

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

// validateAndNormalizeConfig applies defaults, validates service settings, and
// copies credentials into an owned configuration with normalized origin keys.
func validateAndNormalizeConfig(config Config) (Config, error) {
	config = DefaultConfig.Merge(config)

	if config.Auth != nil {
		if err := config.Auth.Validate(); err != nil {
			return Config{}, err
		}

		auth := *config.Auth
		config.Auth = &auth
	}

	outbound := make(map[string]BasicAuth, len(config.OutboundAuth))
	for raw, auth := range config.OutboundAuth {
		u, err := url.Parse(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid outbound auth origin")
		}

		origin, err := Origin(u)
		if err != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
			return Config{}, fmt.Errorf("invalid outbound auth origin")
		}

		if _, exists := outbound[origin]; exists {
			return Config{}, fmt.Errorf("duplicate outbound auth origin")
		}

		if err := auth.Validate(); err != nil {
			return Config{}, err
		}

		outbound[origin] = auth
	}

	config.OutboundAuth = outbound

	if config.Storage == nil {
		return Config{}, fmt.Errorf("tasker: storage is required")
	}

	if config.DispatchDeadline <= 0 || config.MaxAttempts <= 0 {
		return Config{}, fmt.Errorf("tasker: dispatch deadline and max attempts must be positive")
	}

	if config.MaxConcurrentDispatches <= 0 || config.MaxDispatchesPerSecond <= 0 ||
		math.IsNaN(config.MaxDispatchesPerSecond) || math.IsInf(config.MaxDispatchesPerSecond, 0) {
		return Config{}, fmt.Errorf("tasker: dispatch rate and concurrency must be positive and finite")
	}

	if config.MinBackoff <= 0 || config.MaxBackoff < config.MinBackoff {
		return Config{}, fmt.Errorf("tasker: backoff bounds must be positive and max must be at least min")
	}

	if config.TaskRetention <= 0 {
		return Config{}, fmt.Errorf("tasker: task retention must be positive")
	}

	return config, nil
}

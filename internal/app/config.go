package app

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type loggingConfig struct {
	Format string `yaml:"format"`
	Level  string `yaml:"level"`
	Access bool   `yaml:"access"`
}

type appConfig struct {
	Logging  loggingConfig  `yaml:"logging"`
	HTTP     httpConfig     `yaml:"http"`
	Database databaseConfig `yaml:"database"`
	Queue    queueConfig    `yaml:"queue"`
}

type databaseConfig struct {
	Path string `yaml:"path"`
}

type httpConfig struct {
	ListenAddress   string        `yaml:"listen_address"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type queueConfig struct {
	MaxDispatchesPerSecond  float64       `yaml:"max_dispatches_per_second"`
	MaxConcurrentDispatches int           `yaml:"max_concurrent_dispatches"`
	DispatchDeadline        time.Duration `yaml:"dispatch_deadline"`
	MaxAttempts             int           `yaml:"max_attempts"`
	MinBackoff              time.Duration `yaml:"min_backoff"`
	MaxBackoff              time.Duration `yaml:"max_backoff"`
	TaskRetention           time.Duration `yaml:"task_retention"`
}

var defaultConfig = appConfig{
	Logging: loggingConfig{Format: "json", Level: "info"},
	HTTP: httpConfig{
		ListenAddress:   ":8080",
		ShutdownTimeout: 10 * time.Second,
	},
	Database: databaseConfig{
		Path: "tasker.db",
	},
	Queue: queueConfig{
		MaxDispatchesPerSecond:  10,
		MaxConcurrentDispatches: 20,
		DispatchDeadline:        5 * time.Minute,
		MaxAttempts:             5,
		MinBackoff:              time.Second,
		MaxBackoff:              time.Minute,
		TaskRetention:           7 * 24 * time.Hour,
	},
}

// loadConfig reads a YAML mapping over defaults, or returns defaults if the file
// does not exist. Validation is a separate step.
func loadConfig(path string) (appConfig, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return defaultConfig, nil
	}

	if err != nil {
		return appConfig{}, fmt.Errorf("read config %q: %w", path, err)
	}

	config := defaultConfig
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return appConfig{}, fmt.Errorf("decode config %q: %w", path, err)
	}

	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return appConfig{}, fmt.Errorf("decode config %q: %w", path, err)
		}

		return appConfig{}, fmt.Errorf("config %q must contain a single YAML document", path)
	}

	return config, nil
}

func validateConfig(config appConfig) error {
	if config.Logging.Format != "json" && config.Logging.Format != "text" {
		return fmt.Errorf("logging.format must be json or text")
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(config.Logging.Level)); err != nil {
		return fmt.Errorf("logging.level: %w", err)
	}

	for _, field := range []struct{ name, value string }{
		{"http.listen_address", config.HTTP.ListenAddress},
		{"database.path", config.Database.Path},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s must not be empty", field.name)
		}
	}

	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{"http.shutdown_timeout", config.HTTP.ShutdownTimeout},
		{"queue.dispatch_deadline", config.Queue.DispatchDeadline},
		{"queue.min_backoff", config.Queue.MinBackoff},
		{"queue.max_backoff", config.Queue.MaxBackoff},
		{"queue.task_retention", config.Queue.TaskRetention},
	} {
		if field.value <= 0 {
			return fmt.Errorf("%s must be positive", field.name)
		}
	}

	if config.Queue.MaxAttempts <= 0 {
		return fmt.Errorf("queue.max_attempts must be positive")
	}

	if config.Queue.MaxConcurrentDispatches <= 0 {
		return fmt.Errorf("queue.max_concurrent_dispatches must be positive")
	}

	if config.Queue.MaxDispatchesPerSecond <= 0 || math.IsNaN(config.Queue.MaxDispatchesPerSecond) || math.IsInf(config.Queue.MaxDispatchesPerSecond, 0) {
		return fmt.Errorf("queue.max_dispatches_per_second must be positive and finite")
	}

	if config.Queue.MaxBackoff < config.Queue.MinBackoff {
		return fmt.Errorf("queue.max_backoff must be at least queue.min_backoff")
	}

	return nil
}

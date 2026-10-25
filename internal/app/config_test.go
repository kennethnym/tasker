package app

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{"defaults", "{}", ""},
		{"logging", "logging:\n  format: text\n  level: debug\n  access: true", ""},
		{"metrics", "metrics:\n  enabled: true", ""},
		{"invalid metrics", "metrics:\n  enabled: invalid", "decode config"},
		{"invalid log format", "logging:\n  format: xml", "logging.format"},
		{"invalid log level", "logging:\n  level: loud", "logging.level"},
		{"override", "queue:\n  max_attempts: 8\n  dispatch_deadline: \"30s\"", ""},
		{"alias", "queue:\n  min_backoff: &backoff 2s\n  max_backoff: *backoff", ""},
		{"list field", "queue:\n  max_attempts: [3]", "decode config"},
		{"mapping field", "http:\n  listen_address: {port: 8080}", "decode config"},
		{"empty sections", "http: {}\nqueue: {}\ndatabase: {}", ""},
		{"null section", "http: null", ""},
		{"database override", "database:\n  path: custom.db", ""},
		{"old database key", "database_path: custom.db", "not found"},
		{"null database", "database: null", ""},
		{"scalar database", "database: custom.db", "decode config"},
		{"scalar section", "queue: 3", "decode config"},
		{"nested unknown", "http:\n  typo: 3", "not found"},
		{"old flat key", "max_attempts: 3", "not found"},
		{"old kebab key", "queue:\n  max-attempts: 3", "not found"},
		{"empty", "", "decode config"},
		{"null", "null", ""},
		{"array", "[]", "decode config"},
		{"unknown", "max-atempt: 3", "not found"},
		{"trailing", "{} {}", "decode config"},
		{"multiple documents", "{}\n---\n{}", "single YAML document"},
		{"duplicate", "queue:\n  max_attempts: 3\n  max_attempts: 4", "already defined"},
		{"nan rate", "queue:\n  max_dispatches_per_second: .nan", "positive and finite"},
		{"infinite rate", "queue:\n  max_dispatches_per_second: .inf", "positive and finite"},
		{"implicit null", "queue:\n  max_attempts:", ""},
		{"null field", "queue:\n  max_attempts: null", ""},
		{"zero attempts", "queue:\n  max_attempts: 0", "max_attempts must be positive"},
		{"negative concurrency", "queue:\n  max_concurrent_dispatches: -1", "must be positive"},
		{"zero rate", "queue:\n  max_dispatches_per_second: 0", "must be positive"},
		{"overflow rate", "queue:\n  max_dispatches_per_second: 1e999", "decode config"},
		{"blank address", "http:\n  listen_address: \" \"", "must not be empty"},
		{"blank database", "database:\n  path: \"\"", "must not be empty"},
		{"bad duration", "queue:\n  dispatch_deadline: \"7d\"", "decode config"},
		{"numeric duration", "http:\n  shutdown_timeout: 10", "decode config"},
		{"zero shutdown", "http:\n  shutdown_timeout: \"0s\"", "must be positive"},
		{"zero deadline", "queue:\n  dispatch_deadline: \"0s\"", "must be positive"},
		{"negative retention", "queue:\n  task_retention: \"-1h\"", "must be positive"},
		{"zero min", "queue:\n  min_backoff: \"0s\"", "must be positive"},
		{"zero max", "queue:\n  max_backoff: \"0s\"", "must be positive"},
		{"backoff order", "queue:\n  min_backoff: \"2m\"", "max_backoff must be at least"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			config, err := loadConfig(path)
			if err == nil {
				err = validateConfig(config)
			}

			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v, want %q", err, tc.want)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if (tc.name == "defaults" || tc.name == "empty sections" || strings.Contains(tc.name, "null")) && !reflect.DeepEqual(config, defaultConfig) {
				t.Fatalf("unexpected defaults: %+v", config)
			}

			if tc.name == "logging" && (config.Logging.Format != "text" || config.Logging.Level != "debug" || !config.Logging.Access) {
				t.Fatalf("unexpected logging config: %+v", config.Logging)
			}

			if tc.name == "metrics" && !config.Metrics.Enabled {
				t.Fatal("metrics was not enabled")
			}

			if tc.name == "database override" && config.Database.Path != "custom.db" {
				t.Fatalf("database path = %q, want custom.db", config.Database.Path)
			}

			if tc.name == "alias" && (config.Queue.MinBackoff != 2*time.Second || config.Queue.MaxBackoff != 2*time.Second) {
				t.Fatalf("unexpected alias values: %+v", config.Queue)
			}

			if tc.name == "override" && (config.Queue.MaxAttempts != 8 || config.Queue.DispatchDeadline != 30*time.Second || config.HTTP.ListenAddress != defaultConfig.HTTP.ListenAddress) {
				t.Fatalf("unexpected overrides: %+v", config)
			}
		})
	}
}

func TestMissingConfig(t *testing.T) {
	config, err := loadConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(config, defaultConfig) {
		t.Fatalf("got %+v, want defaults", config)
	}
}

func TestConfigReadError(t *testing.T) {
	if _, err := loadConfig(t.TempDir()); err == nil {
		t.Fatal("expected an error when reading a directory")
	}
}

func TestConfigIgnoresEnvironment(t *testing.T) {
	t.Setenv("TASKER_MAX_ATTEMPTS", "99")
	config, err := loadConfig("../../tasker.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if config.Queue.MaxAttempts != defaultConfig.Queue.MaxAttempts {
		t.Fatal("environment changed config")
	}
}

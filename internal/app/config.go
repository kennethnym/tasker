package app

import (
	"flag"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"tasker/internal/tasker"
)

func validateConfig(config tasker.Config) error {
	if config.MaxBackoff < config.MinBackoff {
		return fmt.Errorf("max-backoff must be at least min-backoff")
	}

	return nil
}

func loadConfigFromArgs(args []string, output io.Writer) (tasker.Config, error) {
	var c tasker.Config
	flags := configFlags(&c)
	flags.SetOutput(output)

	if err := flags.Parse(args); err != nil {
		return tasker.Config{}, err
	}

	if flags.NArg() != 0 {
		return tasker.Config{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}

	if err := validateProvidedFlags(flags, func(name string) string { return "--" + name }); err != nil {
		return tasker.Config{}, err
	}

	return c, nil
}

func loadConfigFromEnv(lookupEnv func(string) (string, bool)) (tasker.Config, error) {
	var c tasker.Config
	flags := configFlags(&c)

	var parseErr error
	flags.VisitAll(func(f *flag.Flag) {
		if parseErr != nil {
			return
		}

		if value, ok := lookupEnv(envName(f.Name)); ok {
			if err := flags.Set(f.Name, value); err != nil {
				parseErr = fmt.Errorf("invalid %s: %w", envName(f.Name), err)
			}
		}
	})

	if parseErr != nil {
		return tasker.Config{}, parseErr
	}

	if err := validateProvidedFlags(flags, envName); err != nil {
		return tasker.Config{}, err
	}

	return c, nil
}

func configFlags(c *tasker.Config) *flag.FlagSet {
	flags := flag.NewFlagSet("tasker", flag.ContinueOnError)
	flags.Float64Var(&c.MaxDispatchesPerSecond, "max-dispatches-per-second", c.MaxDispatchesPerSecond, "Maximum dispatches per second")
	flags.IntVar(&c.MaxConcurrentDispatches, "max-concurrent-dispatches", c.MaxConcurrentDispatches, "Maximum concurrent dispatches")
	flags.DurationVar(&c.DispatchDeadline, "dispatch-deadline", c.DispatchDeadline, "Dispatch timeout")
	flags.IntVar(&c.MaxAttempts, "max-attempts", c.MaxAttempts, "Maximum delivery attempts")
	flags.DurationVar(&c.MinBackoff, "min-backoff", c.MinBackoff, "Minimum retry backoff")
	flags.DurationVar(&c.MaxBackoff, "max-backoff", c.MaxBackoff, "Maximum retry backoff")
	flags.DurationVar(&c.TaskRetention, "task-retention", c.TaskRetention, "Retention for terminal tasks")

	flags.VisitAll(func(f *flag.Flag) { f.Usage += " (" + envName(f.Name) + ")" })

	return flags
}

// Reject explicit zero values before Merge treats them as absent. Validate
// individual values here; relationships between fields depend on the final merge.
func validateProvidedFlags(flags *flag.FlagSet, source func(string) string) error {
	var err error
	flags.Visit(func(f *flag.Flag) {
		var valid bool
		switch value := f.Value.(flag.Getter).Get().(type) {
		case int:
			valid = value > 0
		case float64:
			valid = value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
		case time.Duration:
			valid = value > 0
		}

		if !valid && err == nil {
			err = fmt.Errorf("%s must be positive and finite", source(f.Name))
		}
	})

	return err
}

func envName(name string) string {
	return "TASKER_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

package app

import (
	"errors"
	"flag"
	"os"

	"tasker/internal/tasker"
)

func Run() error {
	argsConfig, err := loadConfigFromArgs(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	envConfig, err := loadConfigFromEnv(os.LookupEnv)
	if err != nil {
		return err
	}

	config := tasker.DefaultConfig.Merge(argsConfig).Merge(envConfig)
	if err := validateConfig(config); err != nil {
		return err
	}

	service, err := tasker.New(config)
	if err != nil {
		return err
	}

	_ = service
	return tasker.ErrNotImplemented
}

package main

import (
	"flag"
	"log/slog"
	"os"

	"tasker/internal/app"
)

func main() {
	path := flag.String("config", "tasker.yaml", "Path to the YAML configuration file")
	flag.Parse()

	if flag.NArg() != 0 {
		slog.Error("usage: tasker --config path/to/tasker.yaml")
		os.Exit(1)
	}

	if err := app.Run(*path); err != nil {
		slog.With("component", "app").Error("tasker failed", "error", err)
		os.Exit(1)
	}
}

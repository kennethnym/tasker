package main

import (
	"flag"
	"log"

	"tasker/internal/app"
)

func main() {
	path := flag.String("config", "tasker.yaml", "Path to the YAML configuration file")
	flag.Parse()

	if flag.NArg() != 0 {
		log.Fatal("usage: tasker --config path/to/tasker.yaml")
	}

	if err := app.Run(*path); err != nil {
		log.Fatal(err)
	}
}

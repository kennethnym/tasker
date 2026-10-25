package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tasker/internal/storage/sqlite"
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

	config := defaultConfig.Merge(argsConfig).Merge(envConfig)
	if err := validateConfig(config); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Restore default signal handling so a second signal can force exit.
	go func() {
		<-ctx.Done()
		stop()
	}()

	return run(ctx, config)
}

func run(ctx context.Context, config appConfig) (err error) {
	storage, err := sqlite.Open(ctx, config.DatabasePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}

	defer func() {
		err = errors.Join(err, storage.Close())
	}()

	config.Storage = storage

	service, err := tasker.New(config.Config)
	if err != nil {
		return err
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	slog.Info("tasker listening", "address", listener.Addr().String())

	return serve(ctx, listener, service, config.ShutdownTimeout)
}

// serve owns the listener and waits for background work before returning.
func serve(ctx context.Context, listener net.Listener, service *tasker.Tasker, shutdownTimeout time.Duration) error {
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Keep incoming requests alive during the grace period, independently of signals.
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()

	server := &http.Server{
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       time.Minute,
		BaseContext:       func(net.Listener) context.Context { return requestCtx },
	}

	workerDone := make(chan error, 1)
	httpDone := make(chan error, 1)

	go func() {
		workerDone <- service.Run(workerCtx)
	}()

	go func() {
		httpDone <- server.Serve(listener)
	}()

	var workerErr, httpErr error
	var workerStopped, httpStopped bool

	select {
	case <-ctx.Done():

	case workerErr = <-workerDone:
		workerStopped = true

	case httpErr = <-httpDone:
		httpStopped = true
	}

	cancelWorkers()

	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownTimeout)
	defer stop()

	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		cancelRequests()
		shutdownErr = errors.Join(fmt.Errorf("HTTP shutdown: %w", shutdownErr), server.Close())
	}

	if !httpStopped {
		httpErr = <-httpDone
	}

	if !workerStopped {
		workerErr = <-workerDone
	}

	if errors.Is(httpErr, http.ErrServerClosed) {
		httpErr = nil
	}

	if errors.Is(workerErr, context.Canceled) && workerCtx.Err() != nil {
		workerErr = nil
	}

	return errors.Join(httpErr, workerErr, shutdownErr)
}

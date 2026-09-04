// Command metrics-aggregator consumes raw events from Kafka, folds them into
// Prometheus metrics and exposes them over HTTP for scraping.
//
// It is the composition root of the Clean Architecture layers: the only place
// where the application, delivery and infrastructure packages are wired
// together.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eventpulse/telemetry"

	"github.com/eventpulse/metrics-aggregator/internal/application"
	"github.com/eventpulse/metrics-aggregator/internal/config"
	httpdelivery "github.com/eventpulse/metrics-aggregator/internal/delivery/http"
	"github.com/eventpulse/metrics-aggregator/internal/infrastructure/kafka"
	"github.com/eventpulse/metrics-aggregator/internal/metrics"
)

const (
	serviceName = "metrics-aggregator"

	// shutdownTimeout bounds draining the HTTP server and closing Kafka.
	shutdownTimeout = 15 * time.Second

	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("service stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}

// run holds the whole lifecycle so that every deferred cleanup executes before
// the process exits; main only translates a failure into a non-zero status.
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With(slog.String("service", serviceName))
	slog.SetDefault(logger)

	if err := telemetry.Init(telemetry.Config{
		ServiceName:  serviceName,
		Environment:  os.Getenv("ENVIRONMENT"),
		OTLPEndpoint: os.Getenv("OTLP_ENDPOINT"),
	}); err != nil {
		return fmt.Errorf("telemetry init: %w", err)
	}

	cfg := config.Load()

	// Wire the layers: metrics collectors <- aggregator <- Kafka consumer,
	// plus the HTTP server serving the same collectors.
	m := metrics.New()
	aggregator := application.New(m)
	consumer := kafka.NewConsumer(kafka.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	}, aggregator, m, logger)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpdelivery.NewRouter(m),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// Cancelled on SIGINT/SIGTERM, which is what `docker stop` and Kubernetes
	// send when they drain a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Run(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening",
			slog.String("addr", cfg.HTTPAddr),
			slog.Any("kafka_brokers", cfg.KafkaBrokers),
			slog.String("kafka_topic", cfg.KafkaTopic),
			slog.String("kafka_group_id", cfg.KafkaGroupID),
		)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("http server: %w", err)
		}
	}()

	var fatalErr error

	select {
	case err := <-serverErr:
		fatalErr = err
	case err := <-consumerErr:
		fatalErr = err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	}

	// Stop trapping signals so a second Ctrl-C aborts a stuck shutdown.
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	var errs []error

	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	} else {
		logger.Info("http server stopped")
	}

	// Closed after the server so that /metrics keeps serving while the last
	// events drain.
	if err := consumer.Close(); err != nil {
		errs = append(errs, err)
	} else {
		logger.Info("kafka consumer closed")
	}

	if fatalErr != nil {
		return errors.Join(append([]error{fatalErr}, errs...)...)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	logger.Info("shutdown complete")

	return nil
}

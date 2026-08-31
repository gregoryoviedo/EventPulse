// Command ingestion-gateway is the HTTP entry point of the eventpulse
// platform: it accepts events over REST and publishes them to Kafka.
//
// It is the composition root of the Clean Architecture layers: this file is the
// only place where the domain, delivery and infrastructure packages are wired
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
	"strings"
	"syscall"
	"time"

	"github.com/eventpulse/telemetry"

	httpdelivery "github.com/eventpulse/ingestion-gateway/internal/delivery/http"
	"github.com/eventpulse/ingestion-gateway/internal/infrastructure/kafka"
)

const (
	serviceName = "ingestion-gateway"

	// defaultHTTPAddr is the listen address required by docker-compose and the
	// Helm chart (containerPort 8080).
	defaultHTTPAddr = ":8080"
	// defaultKafkaBrokers targets a Kafka listening directly on the host and is
	// only used when KAFKA_BROKERS is not exported. Against docker-compose use
	// the external listener instead: KAFKA_BROKERS=localhost:29092.
	defaultKafkaBrokers = "localhost:9092"

	// shutdownTimeout bounds draining in-flight requests and flushing Kafka.
	shutdownTimeout = 15 * time.Second
	// topicSetupTimeout bounds the best-effort topic provisioning at start-up.
	topicSetupTimeout = 10 * time.Second

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
		Environment:  getenv("ENVIRONMENT", "development"),
		OTLPEndpoint: os.Getenv("OTLP_ENDPOINT"),
	}); err != nil {
		return fmt.Errorf("telemetry init: %w", err)
	}

	// Infrastructure layer: KAFKA_BROKERS is provided by docker-compose.yml
	// and by the Helm deployment ("kafka:9092").
	brokers := kafkaBrokers()
	producer := kafka.NewProducer(kafka.Config{
		Brokers: brokers,
		Topic:   os.Getenv("KAFKA_TOPIC"),
	})

	ensureTopic(producer, logger)

	// Delivery layer, depending on the domain port implemented above.
	handler := httpdelivery.NewEventHandler(producer, logger)

	addr := getenv("HTTP_ADDR", defaultHTTPAddr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpdelivery.NewRouter(handler),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// Cancelled on SIGINT/SIGTERM, which is what `docker stop` and Kubernetes
	// send when they drain a container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)

	go func() {
		logger.Info("http server listening",
			slog.String("addr", addr),
			slog.Any("kafka_brokers", brokers),
			slog.String("kafka_topic", producer.Topic()),
		)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("http server: %w", err)
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	}

	// Stop trapping signals so a second Ctrl-C aborts a stuck shutdown.
	stop()

	return shutdown(srv, producer, logger)
}

// ensureTopic provisions the destination topic before the server starts
// accepting traffic.
//
// Failure is deliberately non-fatal: the broker may still be booting (compose
// only orders start-up, it does not wait for readiness), and the writer will
// keep retrying on the first publish.
func ensureTopic(producer *kafka.Producer, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), topicSetupTimeout)
	defer cancel()

	if err := producer.EnsureTopic(ctx); err != nil {
		logger.Warn("could not provision kafka topic, falling back to auto-creation",
			slog.String("kafka_topic", producer.Topic()),
			slog.Any("error", err),
		)

		return
	}

	logger.Info("kafka topic ready", slog.String("kafka_topic", producer.Topic()))
}

// shutdown stops accepting new requests, waits for in-flight ones and then
// flushes the Kafka writer, all bounded by shutdownTimeout.
func shutdown(srv *http.Server, producer *kafka.Producer, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	var errs []error

	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	} else {
		logger.Info("http server stopped")
	}

	// Closed after the server so that handlers still publishing are not cut
	// off mid-request; Close flushes anything buffered.
	if err := producer.Close(); err != nil {
		errs = append(errs, err)
	} else {
		logger.Info("kafka producer closed")
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	logger.Info("shutdown complete")

	return nil
}

// kafkaBrokers reads the comma-separated KAFKA_BROKERS variable.
func kafkaBrokers() []string {
	raw := getenv("KAFKA_BROKERS", defaultKafkaBrokers)

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	if len(brokers) == 0 {
		return []string{defaultKafkaBrokers}
	}

	return brokers
}

// getenv returns the environment variable value or fallback when unset/empty.
func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}

	return fallback
}

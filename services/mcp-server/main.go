// Command mcp-server exposes the RAG capabilities of the eventpulse platform
// as a Model Context Protocol server over Streamable HTTP: LLM clients query
// the pgvector index through the rag_search tool.
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

	"github.com/joho/godotenv"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/eventpulse/mcp-server/internal/application"
	"github.com/eventpulse/mcp-server/internal/config"
	"github.com/eventpulse/mcp-server/internal/delivery/mcp"
	"github.com/eventpulse/mcp-server/internal/infrastructure/embeddings"
	pgvectorstore "github.com/eventpulse/mcp-server/internal/infrastructure/pgvector"
	"github.com/eventpulse/telemetry"
)

const (
	serviceName = "mcp-server"

	// version matches the Helm chart image tag so clients can identify the
	// release they are talking to.
	version = "0.1.0"

	// shutdownTimeout bounds draining the HTTP server and flushing telemetry.
	shutdownTimeout = 15 * time.Second

	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
)

func main() {
	_ = godotenv.Load()
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

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if err := telemetry.Init(telemetry.Config{
		ServiceName:  serviceName,
		Environment:  os.Getenv("ENVIRONMENT"),
		OTLPEndpoint: os.Getenv("OTLP_ENDPOINT"),
	}); err != nil {
		return fmt.Errorf("telemetry init: %w", err)
	}

	// Fail fast on the dependencies: an unreachable database or a misconfigured
	// embedding provider should stop the process, not every tool call.
	embedder, err := embeddings.New(embeddings.Config{
		Provider:   cfg.EmbeddingsProvider,
		Model:      cfg.EmbeddingModel,
		Dimensions: cfg.EmbeddingDimensions,
		APIKey:     cfg.OpenAIAPIKey,
		BaseURL:    cfg.OpenAIBaseURL,
		Token:      cfg.HuggingFaceHubAPIToken,
	})
	if err != nil {
		return fmt.Errorf("embeddings: %w", err)
	}

	store, err := pgvectorstore.New(context.Background(), cfg.PostgresURI, cfg.EmbeddingsTable)
	if err != nil {
		return fmt.Errorf("pgvector: %w", err)
	}
	defer store.Close()

	searcher := application.NewSearcher(embedder, store)

	// The MCP endpoint and the Kubernetes probes share one listener.
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpserver.NewStreamableHTTPServer(mcp.NewServer(searcher, version)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Ping(r.Context()); err != nil {
			logger.Warn("readiness probe failed", slog.Any("error", err))
			http.Error(w, `{"status":"unavailable"}`, http.StatusServiceUnavailable)

			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
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
		logger.Info("mcp server listening",
			slog.String("addr", cfg.HTTPAddr),
			slog.String("endpoint", "/mcp"),
			slog.String("embeddings_provider", cfg.EmbeddingsProvider),
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

	return shutdown(srv, logger)
}

// shutdown stops accepting new requests, waits for in-flight ones and then
// flushes the telemetry exporters, all bounded by shutdownTimeout.
func shutdown(srv *http.Server, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	var errs []error

	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	} else {
		logger.Info("http server stopped")
	}

	if err := telemetry.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("telemetry shutdown: %w", err))
	} else {
		logger.Info("telemetry flushed")
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	logger.Info("shutdown complete")

	return nil
}
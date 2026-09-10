// Package telemetry provides shared observability initialization for all
// eventpulse services: OpenTelemetry traces and metrics exported over OTLP,
// alongside the structured logging each service already sets up on its own.
//
// When no OTLP endpoint is configured the package installs nothing, so a
// service without a collector keeps working with the default no-op providers.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config holds the observability configuration for a service.
type Config struct {
	// ServiceName is reported through the service.name resource attribute.
	ServiceName string
	// Environment is reported through the deployment.environment attribute.
	Environment string
	// OTLPEndpoint is the gRPC collector endpoint (e.g. "localhost:4317").
	// Empty disables the SDK entirely.
	OTLPEndpoint string
}

// closer flushes and closes a provider created by Init. Shutdown runs them in
// reverse creation order, once per call.
type closer func(context.Context) error

var closers []closer

// Init wires the global OpenTelemetry tracer and meter providers.
//
// With an empty OTLPEndpoint the default no-op providers stay in place and
// Shutdown has nothing to flush. With an endpoint, OTLP/gRPC exporters for
// traces and metrics are installed; the caller must call Shutdown when the
// service stops so pending spans and metrics are exported.
//
// The connection is plaintext (WithInsecure): this targets the local dev
// collector and in-cluster TLS termination; production deployments terminate
// TLS in front of the collector.
func Init(cfg Config) error {
	if strings.TrimSpace(cfg.OTLPEndpoint) == "" {
		return nil
	}

	res, err := newResource(cfg)
	if err != nil {
		return fmt.Errorf("otel resource: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("trace exporter: %w", err)
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)
	closers = append(closers, func(ctx context.Context) error {
		return errors.Join(tracerProvider.Shutdown(ctx), traceExporter.Shutdown(ctx))
	})

	metricExporter, err := otlpmetricgrpc.New(
		context.Background(),
		otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)
	closers = append(closers, func(ctx context.Context) error {
		return errors.Join(meterProvider.Shutdown(ctx), metricExporter.Shutdown(ctx))
	})

	return nil
}

// Shutdown flushes and closes every provider created by Init. It is safe to
// call when Init installed nothing, and it clears the registered closers so a
// second call is a no-op.
func Shutdown(ctx context.Context) error {
	var errs []error
	for i := len(closers) - 1; i >= 0; i-- {
		if err := closers[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	closers = nil

	return errors.Join(errs...)
}

// newResource attaches the service identity attributes every signal carries.
func newResource(cfg Config) (*resource.Resource, error) {
	attributes := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion("0.1.0"),
	}
	if env := strings.TrimSpace(cfg.Environment); env != "" {
		attributes = append(attributes, semconv.DeploymentEnvironment(env))
	}

	return resource.New(context.Background(),
		resource.WithAttributes(attributes...),
		resource.WithTelemetrySDK(),
	)
}
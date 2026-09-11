package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// attrValue returns the value for key from a resource attribute slice.
func attrValue(attrs []attribute.KeyValue, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range attrs {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestInitEmptyEndpointIsNoop(t *testing.T) {
	if err := Init(Config{ServiceName: "test", OTLPEndpoint: "  "}); err != nil {
		t.Fatalf("Init with empty endpoint = %v, want nil", err)
	}

	if len(closers) != 0 {
		t.Fatalf("closers = %d, want 0 for no-op init", len(closers))
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	// Shutdown must be safe when Init installed nothing, and a second call
	// must also succeed after the first has cleared the closers.
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown = %v, want nil", err)
	}
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown = %v, want nil", err)
	}
}

func TestNewResourceAttributes(t *testing.T) {
	res, err := newResource(Config{ServiceName: "checkout-api", Environment: "dev"})
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}

	attrs := res.Attributes()

	if v, ok := attrValue(attrs, semconv.ServiceNameKey); !ok || v.AsString() != "checkout-api" {
		t.Fatalf("service.name = %v (ok=%v), want checkout-api", v, ok)
	}
	if v, ok := attrValue(attrs, semconv.DeploymentEnvironmentKey); !ok || v.AsString() != "dev" {
		t.Fatalf("deployment.environment = %v (ok=%v), want dev", v, ok)
	}
}

func TestNewResourceOmitsEmptyEnvironment(t *testing.T) {
	res, err := newResource(Config{ServiceName: "worker"})
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}

	if _, ok := attrValue(res.Attributes(), semconv.DeploymentEnvironmentKey); ok {
		t.Fatalf("deployment.environment should be omitted when empty")
	}
}
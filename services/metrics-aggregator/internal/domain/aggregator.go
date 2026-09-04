// Package domain holds the ports of the metrics aggregator: the abstractions
// the delivery and infrastructure layers depend on. It must not import any
// broker or transport package.
package domain

import (
	"context"

	"github.com/eventpulse/events"
)

// EventAggregator is the port the Kafka delivery layer hands each consumed
// event to. The concrete implementation folds the event into the aggregated
// metrics.
type EventAggregator interface {
	// Handle aggregates a single raw event, honouring cancellation via ctx.
	Handle(ctx context.Context, event events.Event) error
}

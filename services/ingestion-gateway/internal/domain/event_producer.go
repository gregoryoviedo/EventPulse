package domain

import "context"

// EventProducer is the output port used to hand an accepted Event over to the
// event stream.
//
// The interface is declared here, in the inner layer, and implemented in
// internal/infrastructure/kafka. That inverts the dependency: the delivery
// layer talks to this abstraction and never imports the Kafka client.
type EventProducer interface {
	// Produce publishes the event, honouring cancellation via ctx. It returns
	// a non-nil error when the event could not be handed to the broker.
	Produce(ctx context.Context, event Event) error
}

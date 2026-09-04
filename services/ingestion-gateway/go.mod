module github.com/eventpulse/ingestion-gateway

go 1.25.0

require (
	github.com/eventpulse/events v0.0.0
	github.com/eventpulse/telemetry v0.0.0
	github.com/segmentio/kafka-go v0.4.51
)

require (
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/net v0.57.0 // indirect
)

replace github.com/eventpulse/events => ../../pkg/events

replace github.com/eventpulse/telemetry => ../../pkg/telemetry

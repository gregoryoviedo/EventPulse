module github.com/eventpulse/ingestion-gateway

go 1.23.0

require (
	github.com/eventpulse/events v0.0.0
	github.com/eventpulse/telemetry v0.0.0
	github.com/segmentio/kafka-go v0.4.51
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace github.com/eventpulse/events => ../../pkg/events

replace github.com/eventpulse/telemetry => ../../pkg/telemetry

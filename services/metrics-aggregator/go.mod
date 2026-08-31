module github.com/eventpulse/metrics-aggregator

go 1.23

require (
	github.com/eventpulse/events v0.0.0
	github.com/eventpulse/telemetry v0.0.0
)

replace github.com/eventpulse/events => ../../pkg/events

replace github.com/eventpulse/telemetry => ../../pkg/telemetry

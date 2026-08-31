package main

import (
	"log"

	"github.com/eventpulse/telemetry"
	"github.com/eventpulse/events"
)

func main() {
	if err := telemetry.Init(telemetry.Config{ServiceName: "mcp-server"}); err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	_ = events.TopicDocsEmbedded

	// TODO: expose RAG tools over MCP and query pgvector for semantic search.
	log.Println("mcp-server started (scaffold)")
	select {}
}

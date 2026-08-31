package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/eventpulse/events"
	"github.com/eventpulse/telemetry"
)

func main() {
	if err := telemetry.Init(telemetry.Config{ServiceName: "mcp-server"}); err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	_ = events.TopicDocsEmbedded

	// Block until the process is asked to stop. A bare `select {}` would be
	// flagged by the runtime as a deadlock ("all goroutines are asleep"), so the
	// scaffold waits on SIGINT/SIGTERM instead, which is also what `docker stop`
	// and Kubernetes send when draining the container.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// TODO: expose RAG tools over MCP and query pgvector for semantic search.
	log.Println("mcp-server started (scaffold)")

	<-ctx.Done()

	// Stop trapping signals so a second Ctrl-C aborts a stuck shutdown.
	stop()

	// TODO: close the MCP listener and the pgvector pool here.
	log.Println("mcp-server stopped")
}

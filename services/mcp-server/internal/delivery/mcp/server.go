// Package mcp exposes the RAG use case as a Model Context Protocol server,
// served over Streamable HTTP by main.go.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/eventpulse/mcp-server/internal/application"
)

// Tool names exposed by the server.
const (
	// ToolRagSearch performs semantic search over the indexed documents.
	ToolRagSearch = "rag_search"
)

// NewServer builds the MCP server and registers its tools. version follows
// the Helm chart image tag so clients can identify the release.
func NewServer(searcher *application.Searcher, version string) *mcpserver.MCPServer {
	s := mcpserver.NewMCPServer(
		"eventpulse-mcp",
		version,
		mcpserver.WithToolCapabilities(true),
	)

	s.AddTool(
		mcpgo.NewTool(
			ToolRagSearch,
			mcpgo.WithDescription(
				"Semantic search over the documents indexed in pgvector. "+
					"Returns the chunks most similar to a query, with relevance "+
					"scores and source metadata.",
			),
			mcpgo.WithString("query", mcpgo.Required(), mcpgo.Description("The text to search for")),
			mcpgo.WithNumber("top_k", mcpgo.Description("Number of chunks to return (default 3, max 10)")),
		),
		handleRagSearch(searcher),
	)

	return s
}

// handleRagSearch runs the tool: validate the arguments, delegate to the use
// case and return the hits as a JSON payload.
func handleRagSearch(searcher *application.Searcher) mcpserver.ToolHandlerFunc {
	return func(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
		query := req.GetString("query", "")
		topK := req.GetInt("top_k", 3)
		if topK < 1 || topK > 10 {
			topK = 3
		}

		hits, err := searcher.Search(ctx, query, topK)
		if err != nil {
			return nil, fmt.Errorf("rag search: %w", err)
		}

		payload, err := json.Marshal(hits)
		if err != nil {
			return nil, fmt.Errorf("encode results: %w", err)
		}

		return mcpgo.NewToolResultText(string(payload)), nil
	}
}
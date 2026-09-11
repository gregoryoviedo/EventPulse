package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/eventpulse/mcp-server/internal/application"
	"github.com/eventpulse/mcp-server/internal/domain"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// stubEmbedder returns a fixed vector unless told to fail.
type stubEmbedder struct {
	vector []float32
	err    error
}

func (s *stubEmbedder) EmbedQuery(_ context.Context, _ string) ([]float32, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.vector, nil
}

// stubSearcher records the topK and returns canned hits unless told to fail.
type stubSearcher struct {
	topK int
	hits []domain.Hit
	err  error
}

func (s *stubSearcher) Search(_ context.Context, _ []float32, topK int) ([]domain.Hit, error) {
	s.topK = topK
	if s.err != nil {
		return nil, s.err
	}
	return s.hits, nil
}

func newTestHandler(searcher *application.Searcher) func(context.Context, mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	return handleRagSearch(searcher)
}

func callToolArgs(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{Name: ToolRagSearch, Arguments: args}}
}

func TestRagSearchReturnsHits(t *testing.T) {
	searcher := application.NewSearcher(
		&stubEmbedder{vector: []float32{0.1, 0.2}},
		&stubSearcher{hits: []domain.Hit{{
			ChunkID:    "chunk-1",
			DocumentID: "doc-1",
			Source:     "docs-api",
			ChunkIndex: 0,
			Content:    "some content",
			Score:      0.9,
		}}},
	)
	handler := newTestHandler(searcher)

	result, err := handler(context.Background(), callToolArgs(map[string]any{"query": "hola", "top_k": 2}))
	if err != nil {
		t.Fatalf("call rag_search: %v", err)
	}
	if result.IsError {
		t.Fatalf("result marked as error: %+v", result)
	}
	if len(result.Content) == 0 {
		t.Fatalf("result has no content")
	}

	text, ok := result.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content[0] is %T, want TextContent", result.Content[0])
	}

	var hits []domain.Hit
	if err := json.Unmarshal([]byte(text.Text), &hits); err != nil {
		t.Fatalf("decode hits: %v", err)
	}
	if len(hits) != 1 || hits[0].DocumentID != "doc-1" {
		t.Fatalf("hits = %+v, want doc-1", hits)
	}
}

func TestRagSearchClampsTopK(t *testing.T) {
	cases := []struct {
		name string
		topK int
		want int
	}{
		{name: "default when absent", topK: 0, want: 3},
		{name: "clamped high", topK: 100, want: 3},
		{name: "clamped low", topK: 0, want: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &stubSearcher{hits: []domain.Hit{}}
			searcher := application.NewSearcher(&stubEmbedder{vector: []float32{0.1}}, store)
			handler := newTestHandler(searcher)

			args := map[string]any{"query": "q"}
			if tc.topK > 0 {
				args["top_k"] = tc.topK
			}
			if _, err := handler(context.Background(), callToolArgs(args)); err != nil {
				t.Fatalf("call rag_search: %v", err)
			}
			if store.topK != tc.want {
				t.Fatalf("topK = %d, want %d", store.topK, tc.want)
			}
		})
	}
}

func TestRagSearchEmptyQueryIsError(t *testing.T) {
	searcher := application.NewSearcher(
		&stubEmbedder{vector: []float32{0.1}},
		&stubSearcher{hits: []domain.Hit{}},
	)
	handler := newTestHandler(searcher)

	_, err := handler(context.Background(), callToolArgs(map[string]any{"query": "  "}))
	if err == nil {
		t.Fatalf("expected an error for an empty query")
	}
}

func TestRagSearchPropagatesSearcherError(t *testing.T) {
	searcher := application.NewSearcher(
		&stubEmbedder{vector: []float32{0.1}},
		&stubSearcher{err: errors.New("pgvector down")},
	)
	handler := newTestHandler(searcher)

	_, err := handler(context.Background(), callToolArgs(map[string]any{"query": "q"}))
	if err == nil || !strings.Contains(err.Error(), "pgvector down") {
		t.Fatalf("error = %v, want to mention pgvector down", err)
	}
}
// Package application implements the RAG search use case on top of the domain
// ports: embed the query, then retrieve the nearest chunks.
package application

import (
	"context"
	"fmt"
	"strings"

	"github.com/eventpulse/mcp-server/internal/domain"
)

// Searcher is the RAG use case behind the rag_search tool.
type Searcher struct {
	embeddings domain.Embedder
	store      domain.DocumentSearcher
}

// NewSearcher wires the use case over the two ports.
func NewSearcher(embeddings domain.Embedder, store domain.DocumentSearcher) *Searcher {
	return &Searcher{embeddings: embeddings, store: store}
}

// Search embeds the query and returns the topK most similar chunks.
func (s *Searcher) Search(ctx context.Context, query string, topK int) ([]domain.Hit, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	if topK < 1 {
		topK = 1
	}

	vector, err := s.embeddings.EmbedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	hits, err := s.store.Search(ctx, vector, topK)
	if err != nil {
		return nil, fmt.Errorf("search vector store: %w", err)
	}

	return hits, nil
}
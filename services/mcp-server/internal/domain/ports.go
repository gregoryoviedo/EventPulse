// Package domain holds the ports of the mcp-server: the abstractions the
// application, delivery and infrastructure layers depend on. It must not
// import any transport, database or HTTP package.
package domain

import "context"

// Embedder turns a query into the fixed-size vector used by pgvector.
type Embedder interface {
	// EmbedQuery returns the embedding vector for a text query.
	EmbedQuery(ctx context.Context, query string) ([]float32, error)
}

// Hit is a single retrieval result returned to the client.
type Hit struct {
	// ChunkID is the primary key of the chunk in the vector table.
	ChunkID string `json:"chunk_id"`
	// DocumentID is the document the chunk belongs to.
	DocumentID string `json:"document_id"`
	// Source is the origin of the document, e.g. "docs-api".
	Source string `json:"source"`
	// ChunkIndex is the position of the chunk inside its document.
	ChunkIndex int `json:"chunk_index"`
	// Content is the text of the chunk.
	Content string `json:"content"`
	// Score is the cosine similarity between the query and the chunk, in
	// (0, 1]; a higher number means a better match.
	Score float64 `json:"score"`
	// Metadata carries the langchain_metadata JSON column when present.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// DocumentSearcher finds the chunks most similar to a query vector.
type DocumentSearcher interface {
	// Search returns the topK chunks nearest to queryVector, best first.
	Search(ctx context.Context, queryVector []float32, topK int) ([]Hit, error)
}
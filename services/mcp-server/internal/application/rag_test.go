package application

import (
	"context"
	"errors"
	"testing"

	"github.com/eventpulse/mcp-server/internal/domain"
)

// stubEmbedder returns a fixed vector unless told to fail.
type stubEmbedder struct {
	vector []float32
	err    error
	called bool
}

func (s *stubEmbedder) EmbedQuery(_ context.Context, query string) ([]float32, error) {
	s.called = true
	if s.err != nil {
		return nil, s.err
	}
	return append([]float32(nil), s.vector...), nil
}

// stubSearcher records the vector it receives and returns canned hits.
type stubSearcher struct {
	vector []float32
	topK   int
	hits   []domain.Hit
	err    error
}

func (s *stubSearcher) Search(_ context.Context, queryVector []float32, topK int) ([]domain.Hit, error) {
	s.vector = queryVector
	s.topK = topK
	if s.err != nil {
		return nil, s.err
	}
	return s.hits, nil
}

func TestSearchEmbeddsQueryAndForwardsTopK(t *testing.T) {
	embedder := &stubEmbedder{vector: []float32{0.1, 0.2}}
	searcher := &stubSearcher{
		hits: []domain.Hit{{ChunkID: "c1", DocumentID: "d1", Score: 0.9}},
	}
	svc := NewSearcher(embedder, searcher)

	hits, err := svc.Search(context.Background(), "pregunta", 7)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if !embedder.called {
		t.Fatal("Search() did not embed the query")
	}
	if searcher.topK != 7 {
		t.Fatalf("topK passed to store = %d, want 7", searcher.topK)
	}
	if len(hits) != 1 || hits[0].ChunkID != "c1" {
		t.Fatalf("hits = %+v, want the stub hits", hits)
	}
}

func TestSearchClampsTopKToOne(t *testing.T) {
	embedder := &stubEmbedder{vector: []float32{1}}
	searcher := &stubSearcher{}
	svc := NewSearcher(embedder, searcher)

	if _, err := svc.Search(context.Background(), "x", 0); err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if searcher.topK != 1 {
		t.Fatalf("topK passed to store = %d, want 1", searcher.topK)
	}
}

func TestSearchRejectsEmptyQuery(t *testing.T) {
	embedder := &stubEmbedder{vector: []float32{1}}
	searcher := &stubSearcher{}
	svc := NewSearcher(embedder, searcher)

	if _, err := svc.Search(context.Background(), "  ", 3); err == nil {
		t.Fatal("Search() with a blank query: want error, got nil")
	}
}

func TestSearchPropagatesEmbeddingFailure(t *testing.T) {
	embedder := &stubEmbedder{err: errors.New("boom")}
	searcher := &stubSearcher{}
	svc := NewSearcher(embedder, searcher)

	if _, err := svc.Search(context.Background(), "x", 3); err == nil {
		t.Fatal("Search() with failing embedder: want error, got nil")
	}
}

func TestSearchPropagatesStoreFailure(t *testing.T) {
	embedder := &stubEmbedder{vector: []float32{1}}
	searcher := &stubSearcher{err: errors.New("boom")}
	svc := NewSearcher(embedder, searcher)

	if _, err := svc.Search(context.Background(), "x", 3); err == nil {
		t.Fatal("Search() with failing store: want error, got nil")
	}
}
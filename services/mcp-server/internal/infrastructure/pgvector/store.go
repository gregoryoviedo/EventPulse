// Package pgvector implements the domain.DocumentSearcher port on top of a
// pgvector database, matching the schema written by the document processor
// (services/document-processor/schema.sql).
package pgvector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"

	"github.com/eventpulse/mcp-server/internal/domain"
)

// Store searches the document_embeddings table by cosine distance. The table
// name is interpolated into the SQL and is validated as an identifier by the
// config layer before it reaches this package.
type Store struct {
	pool  *pgxpool.Pool
	table string
}

// New connects to Postgres and verifies the pool before returning. A broken
// database fails the process at startup instead of the first tool call.
func New(ctx context.Context, uri, table string) (*Store, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Store{pool: pool, table: table}, nil
}

// SearchQuery finds the chunks nearest to the query vector, returning the
// cosine distance so the caller can convert it into a similarity score.
const searchQuery = `SELECT langchain_id, content, document_id, source, chunk_index,
       langchain_metadata, embedding <=> $1::vector AS distance
FROM %s
ORDER BY embedding <=> $1::vector
LIMIT $2`

// Search returns the topK chunks nearest to queryVector, best first.
func (s *Store) Search(ctx context.Context, queryVector []float32, topK int) ([]domain.Hit, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(searchQuery, s.table), pgvector.NewVector(queryVector), topK)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", s.table, err)
	}
	defer rows.Close()

	hits := make([]domain.Hit, 0, topK)
	for rows.Next() {
		var (
			hit      domain.Hit
			distance float32
			metadata []byte
		)
		if err := rows.Scan(
			&hit.ChunkID,
			&hit.Content,
			&hit.DocumentID,
			&hit.Source,
			&hit.ChunkIndex,
			&metadata,
			&distance,
		); err != nil {
			return nil, fmt.Errorf("scan hit: %w", err)
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &hit.Metadata); err != nil {
				return nil, fmt.Errorf("decode langchain_metadata: %w", err)
			}
		}
		// Cosine distance in (0, 2] maps to similarity in [-1, 1); a higher
		// number means a better match, mirroring the document processor's
		// store.search (1 - distance).
		hit.Score = float64(1.0 - distance)

		hits = append(hits, hit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hits: %w", err)
	}

	return hits, nil
}

// Ping reports whether the pool can reach the database, backing /readyz.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}
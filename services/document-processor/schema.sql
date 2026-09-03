-- Schema of the vector store written by document-processor and read by
-- mcp-server. Placeholders are filled in by store.py through psycopg's SQL
-- composition, so the file stays valid reference documentation of the layout.
--
-- Column names and types intentionally match what langchain-postgres'
-- PGVectorStore expects (id/content/embedding plus an optional JSON metadata
-- column), which is what allows the table to be mapped without an adapter.
-- Every statement is idempotent so replicas can run it concurrently at boot.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS {table} (
    langchain_id       UUID PRIMARY KEY,
    content            TEXT NOT NULL,
    embedding          vector({dimensions}) NOT NULL,
    event_id           TEXT,
    document_id        TEXT,
    source             TEXT,
    chunk_index        INTEGER,
    langchain_metadata JSON
);

-- Supports replacing every chunk of a document on re-upload.
CREATE INDEX IF NOT EXISTS {document_index} ON {table} (document_id);

-- Approximate nearest-neighbour index for the cosine distance used by the
-- retriever. HNSW can be built on an empty table and stays valid as rows arrive.
CREATE INDEX IF NOT EXISTS {embedding_index} ON {table}
    USING hnsw (embedding vector_cosine_ops);

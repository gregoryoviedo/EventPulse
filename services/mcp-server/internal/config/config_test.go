package config

import (
	"testing"
)

func TestLoadRequiresPostgres(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	t.Setenv("POSTGRES_HOST", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() without Postgres: want error, got nil")
	}
}

func TestLoadBuildsPostgresURIParts(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_USER", "alice")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "eventpulse_db")
	t.Setenv("EMBEDDINGS_PROVIDER", "fake")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := "postgresql://alice:secret@localhost:5433/eventpulse_db"
	if cfg.PostgresURI != want {
		t.Fatalf("PostgresURI = %q, want %q", cfg.PostgresURI, want)
	}
}

func TestLoadRejectsBadTableName(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/eventpulse_db")
	t.Setenv("EMBEDDINGS_TABLE", "document_embeddings; DROP TABLE x")
	t.Setenv("EMBEDDINGS_PROVIDER", "fake")

	if _, err := Load(); err == nil {
		t.Fatal("Load() with a malicious table name: want error, got nil")
	}
}

func TestLoadRequiresTokenForHuggingFace(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/eventpulse_db")
	t.Setenv("EMBEDDINGS_PROVIDER", "huggingface")
	t.Setenv("HUGGINGFACEHUB_API_TOKEN", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() with huggingface and no token: want error, got nil")
	}
}

func TestLoadFakeNeedsNoCredentials(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/eventpulse_db")
	t.Setenv("EMBEDDINGS_PROVIDER", "fake")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("HUGGINGFACEHUB_API_TOKEN", "")

	if _, err := Load(); err != nil {
		t.Fatalf("Load() with fake provider error = %v", err)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/eventpulse_db")
	t.Setenv("EMBEDDINGS_PROVIDER", "fake")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("EMBEDDINGS_TABLE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPAddr != ":8090" {
		t.Fatalf("HTTPAddr = %q, want :8090", cfg.HTTPAddr)
	}
	if cfg.EmbeddingsTable != "document_embeddings" {
		t.Fatalf("EmbeddingsTable = %q, want document_embeddings", cfg.EmbeddingsTable)
	}
}
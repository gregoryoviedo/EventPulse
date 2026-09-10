// Package config loads the runtime configuration of the mcp-server from the
// environment, applying the same conventions as the other eventpulse services
// (POSTGRES_*, EMBEDDINGS_*, HTTP_ADDR, ...).
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// tableIdentifier only allows safe table names to be interpolated into the
// search SQL. The value comes from EMBEDDINGS_TABLE, which is not a bindable
// parameter.
var tableIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Config holds every tunable of the service.
type Config struct {
	// HTTPAddr is the listen address for the MCP endpoint and the probes.
	HTTPAddr string
	// PostgresURI is the connection string of the pgvector database.
	PostgresURI string
	// EmbeddingsTable is the table the document processor writes vectors to.
	EmbeddingsTable string

	// EmbeddingsProvider selects the provider: openai, huggingface or fake.
	EmbeddingsProvider string
	// EmbeddingModel is the model used to embed queries.
	EmbeddingModel string
	// EmbeddingDimensions is the width of the vector(n) column.
	EmbeddingDimensions int
	// OpenAIAPIKey and OpenAIBaseURL configure the openai provider.
	OpenAIAPIKey   string
	OpenAIBaseURL  string
	// HuggingFaceHubAPIToken configures the huggingface provider.
	HuggingFaceHubAPIToken string
}

// Load reads the configuration from the environment. Postgres and the
// embedding provider are required, so an unset variable is a hard error
// instead of a failure on the first tool call.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:            getenv("HTTP_ADDR", ":8090"),
		PostgresURI:         postgresURI(),
		EmbeddingsTable:     getenv("EMBEDDINGS_TABLE", "document_embeddings"),
		EmbeddingsProvider:  getenv("EMBEDDINGS_PROVIDER", "huggingface"),
		EmbeddingModel:      getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5"),
		EmbeddingDimensions: getenvInt("EMBEDDING_DIMENSIONS", 1024),
		OpenAIAPIKey:        os.Getenv("OPENAI_API_KEY"),
		OpenAIBaseURL:       getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		HuggingFaceHubAPIToken: os.Getenv("HUGGINGFACEHUB_API_TOKEN"),
	}

	if cfg.PostgresURI == "" {
		return Config{}, fmt.Errorf("POSTGRES_URI or POSTGRES_HOST is required")
	}
	if !tableIdentifier.MatchString(cfg.EmbeddingsTable) {
		return Config{}, fmt.Errorf("EMBEDDINGS_TABLE %q is not a valid table name", cfg.EmbeddingsTable)
	}
	if cfg.EmbeddingDimensions <= 0 {
		return Config{}, fmt.Errorf("EMBEDDING_DIMENSIONS must be a positive integer")
	}

	switch cfg.EmbeddingsProvider {
	case "openai":
		if cfg.OpenAIAPIKey == "" {
			return Config{}, fmt.Errorf("EMBEDDINGS_PROVIDER=openai requires OPENAI_API_KEY")
		}
	case "huggingface":
		if cfg.HuggingFaceHubAPIToken == "" {
			return Config{}, fmt.Errorf("EMBEDDINGS_PROVIDER=huggingface requires HUGGINGFACEHUB_API_TOKEN")
		}
	case "fake":
		// Deterministic vectors, no API key or network: for wiring tests only.
	default:
		return Config{}, fmt.Errorf("EMBEDDINGS_PROVIDER must be openai, huggingface or fake, got %q", cfg.EmbeddingsProvider)
	}

	return cfg, nil
}

// postgresURI builds the connection string, honouring POSTGRES_URI over the
// POSTGRES_* parts like the document processor does.
func postgresURI() string {
	if uri := strings.TrimSpace(os.Getenv("POSTGRES_URI")); uri != "" {
		return uri
	}

	host := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if host == "" {
		return ""
	}

	user := url.UserPassword(
		getenv("POSTGRES_USER", "postgres"),
		os.Getenv("POSTGRES_PASSWORD"),
	)

	u := &url.URL{
		Scheme: "postgresql",
		User:   user,
		Host:   net.JoinHostPort(host, getenv("POSTGRES_PORT", "5432")),
		Path:   getenv("POSTGRES_DB", "eventpulse_db"),
	}

	return u.String()
}

// getenv returns the environment variable value or fallback when unset/empty.
func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}

	return fallback
}

// getenvInt parses an integer environment variable, falling back when unset or
// malformed.
func getenvInt(key string, fallback int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil {
		return v
	}

	return fallback
}
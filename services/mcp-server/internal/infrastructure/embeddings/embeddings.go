// Package embeddings implements the domain.Embedder port over the providers
// used by the document processor: OpenAI-compatible gateways (openai) and
// Hugging Face serverless models (huggingface), plus a deterministic fake for
// exercising the MCP wiring without an API key.
//
// The fake is intentionally NOT vector-compatible with the document
// processor's DeterministicFakeEmbedding (different PRNGs), so a database
// indexed with EMBEDDINGS_PROVIDER=fake on the Python side will not return
// meaningful rankings from this service. Use a real provider end-to-end for
// actual RAG retrieval.
package embeddings

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/eventpulse/mcp-server/internal/domain"
)

// Provider names accepted by Config.Provider.
const (
	ProviderOpenAI     = "openai"
	ProviderHuggingFace = "huggingface"
	ProviderFake       = "fake"
)

// Config holds the connection settings of the embedding provider.
type Config struct {
	// Provider selects the implementation: openai, huggingface or fake.
	Provider string
	// Model is the model used to embed queries.
	Model string
	// Dimensions is the width of the vectors the provider must return.
	Dimensions int
	// APIKey authenticates the openai provider.
	APIKey string
	// BaseURL is the OpenAI-compatible base URL (e.g. https://api.openai.com/v1).
	BaseURL string
	// Token authenticates the huggingface provider.
	Token string
}

// New builds the embedder described by the configuration.
func New(cfg Config) (domain.Embedder, error) {
	switch cfg.Provider {
	case ProviderOpenAI:
		if cfg.APIKey == "" {
			return nil, fmt.Errorf("openai provider requires an API key")
		}
		return &httpEmbedder{
			baseURL: strings.TrimSuffix(cfg.BaseURL, "/"),
			apiKey:  cfg.APIKey,
			model:   cfg.Model,
			client: &http.Client{Timeout: 30 * time.Second},
		}, nil
	case ProviderHuggingFace:
		if cfg.Token == "" {
			return nil, fmt.Errorf("huggingface provider requires a token")
		}
		return &hfEmbedder{
			model:  cfg.Model,
			token:  cfg.Token,
			client: &http.Client{Timeout: 30 * time.Second},
		}, nil
	case ProviderFake:
		return &fakeEmbedder{size: cfg.Dimensions}, nil
	default:
		return nil, fmt.Errorf("unknown embeddings provider %q", cfg.Provider)
	}
}

// httpEmbedder talks to any OpenAI-compatible /embeddings endpoint. The
// Hugging Face serverless gateway (router.huggingface.co) speaks the same
// protocol, so both providers share this implementation.
type httpEmbedder struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func (e *httpEmbedder) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	payload, err := json.Marshal(map[string]any{
		"model": e.model,
		"input": query,
	})
	if err != nil {
		return nil, fmt.Errorf("encode embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		e.baseURL+"/embeddings",
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf("build embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.apiKey)

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed query with %s: %w", e.model, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding endpoint returned %d: %s", resp.StatusCode, truncate(body))
	}

	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	if len(out.Data) == 0 {
		return nil, fmt.Errorf("embedding endpoint returned no vectors")
	}

	return out.Data[0].Embedding, nil
}

// hfEmbedder talks to the Hugging Face serverless feature-extraction pipeline.
//
// This is the endpoint the current huggingface_hub InferenceClient resolves for
// the huggingface provider, which is what the document processor indexes with
// (HuggingFaceEndpointEmbeddings). The OpenAI-compatible /v1/embeddings route
// of router.huggingface.co does NOT expose this task, so the request must go
// through the /hf-inference/models/.../pipeline/feature-extraction route.
type hfEmbedder struct {
	model  string
	token  string
	client *http.Client
}

const hfPipelineBaseURL = "https://router.huggingface.co/hf-inference/models"

func (e *hfEmbedder) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	payload, err := json.Marshal(map[string]any{"inputs": query})
	if err != nil {
		return nil, fmt.Errorf("encode embedding request: %w", err)
	}

	url := fmt.Sprintf("%s/%s/pipeline/feature-extraction", hfPipelineBaseURL, e.model)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.token)

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed query with %s: %w", e.model, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding endpoint returned %d: %s", resp.StatusCode, truncate(body))
	}

	// A single input answers with a flat vector; a batch answers with a nested
	// array. We always send one input, but accept both shapes defensively.
	var flat []float32
	if err := json.Unmarshal(body, &flat); err == nil && len(flat) > 0 {
		return flat, nil
	}

	var batch [][]float32
	if err := json.Unmarshal(body, &batch); err == nil && len(batch) > 0 {
		return batch[0], nil
	}

	return nil, fmt.Errorf("unexpected embedding response: %s", truncate(body))
}

// fakeEmbedder derives a deterministic vector from the text hash. It never
// fails and needs no network, which makes it useful for local wiring tests.
type fakeEmbedder struct {
	size int
}

func (f *fakeEmbedder) EmbedQuery(_ context.Context, query string) ([]float32, error) {
	sum := sha256.Sum256([]byte(query))
	seed := int64(binary.BigEndian.Uint64(sum[:8]))
	rng := rand.New(rand.NewSource(seed))

	vector := make([]float32, f.size)
	for i := range vector {
		vector[i] = float32(rng.Float64()*2 - 1)
	}

	return vector, nil
}

// truncate shortens an error body to something log-friendly.
func truncate(b []byte) string {
	const max = 200
	if len(b) <= max {
		return string(b)
	}

	return string(b[:max]) + "..."
}
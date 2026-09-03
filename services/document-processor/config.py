"""Environment-driven configuration for the document-processor.

Every knob is read once at start-up and frozen, so the rest of the service can
depend on plain values instead of reaching for os.environ. The defaults target
a local run against the docker-compose stack, mirroring the Go services'
`getenv(key, fallback)` convention.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Final

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

SERVICE_NAME: Final = "document-processor"

# Kafka topic published by the ingestion-gateway (pkg/events.TopicRawEvents).
DEFAULT_KAFKA_TOPIC: Final = "raw.events"
# Points at a broker listening directly on the host. Inside docker-compose the
# service receives KAFKA_BROKERS=kafka:9092; from the host use the external
# listener instead: KAFKA_BROKERS=localhost:29092.
DEFAULT_KAFKA_BROKERS: Final = "localhost:29092"
DEFAULT_POSTGRES_URI: Final = "postgresql://postgres:postgres@localhost:5432/eventpulse_db"
DEFAULT_EMBEDDINGS_TABLE: Final = "document_embeddings"
DEFAULT_EMBEDDING_MODEL: Final = "text-embedding-3-small"
# Native width of text-embedding-3-small; it is also the vector(n) size of the
# table, so changing it requires recreating document_embeddings.
DEFAULT_EMBEDDING_DIMENSIONS: Final = 1536
# OpenAI-compatible gateway the service talks to by default; OPENAI_BASE_URL
# overrides it when set.
DEFAULT_OPENAI_BASE_URL: Final = "https://opencode.ai/zen/go/v1"
DEFAULT_DOCUMENT_EVENT_TYPES: Final = "document_uploaded"

# Postgres identifiers cannot be bound as query parameters, so the table name
# is validated instead of escaped.
_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(RuntimeError):
    """Raised when the environment does not describe a runnable service."""


@dataclass(frozen=True)
class Settings:
    """Resolved configuration of a single process."""

    kafka_brokers: str
    kafka_topic: str
    kafka_group_id: str
    kafka_auto_offset_reset: str

    postgres_uri: str
    embeddings_table: str

    embeddings_provider: str
    embedding_model: str
    embedding_dimensions: int
    openai_api_key: str
    openai_base_url: str | None

    chunk_size: int
    chunk_overlap: int
    document_event_types: frozenset[str]

    http_host: str
    http_port: int

    log_level: str

    @property
    def async_postgres_uri(self) -> str:
        """SQLAlchemy URL for langchain-postgres, which drives asyncpg."""
        return _with_driver(self.postgres_uri, "asyncpg")

    @property
    def sync_postgres_uri(self) -> str:
        """libpq-style URI used by psycopg for the schema bootstrap."""
        return _with_driver(self.postgres_uri, None)

    @property
    def uses_openai(self) -> bool:
        return self.embeddings_provider == "openai"


def load_settings() -> Settings:
    """Build the settings from the environment, validating what must be valid.

    A `.env` file next to the service is loaded at import time, so a local run
    picks up credentials without exporting them.
    """
    provider = _getenv("EMBEDDINGS_PROVIDER", "openai").lower()
    if provider not in ("openai", "fake"):
        raise ConfigError(f"EMBEDDINGS_PROVIDER must be 'openai' or 'fake', got {provider!r}")

    api_key = _getenv("OPENAI_API_KEY", "")
    if provider == "openai" and not api_key:
        raise ConfigError(
            "OPENAI_API_KEY is required when EMBEDDINGS_PROVIDER=openai; "
            "set EMBEDDINGS_PROVIDER=fake to run without an embedding endpoint"
        )

    table = _getenv("EMBEDDINGS_TABLE", DEFAULT_EMBEDDINGS_TABLE)
    if not _IDENTIFIER_RE.match(table):
        raise ConfigError(f"EMBEDDINGS_TABLE must be a plain SQL identifier, got {table!r}")

    chunk_size = _getenv_int("CHUNK_SIZE", 500)
    chunk_overlap = _getenv_int("CHUNK_OVERLAP", 50)
    if chunk_size <= 0:
        raise ConfigError("CHUNK_SIZE must be greater than 0")
    if not 0 <= chunk_overlap < chunk_size:
        raise ConfigError("CHUNK_OVERLAP must be at least 0 and smaller than CHUNK_SIZE")

    event_types = frozenset(
        part.strip()
        for part in _getenv("DOCUMENT_EVENT_TYPES", DEFAULT_DOCUMENT_EVENT_TYPES).split(",")
        if part.strip()
    )
    if not event_types:
        raise ConfigError("DOCUMENT_EVENT_TYPES must list at least one event type")

    return Settings(
        kafka_brokers=_getenv("KAFKA_BROKERS", DEFAULT_KAFKA_BROKERS),
        kafka_topic=_getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
        kafka_group_id=_getenv("KAFKA_GROUP_ID", SERVICE_NAME),
        kafka_auto_offset_reset=_getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        postgres_uri=_postgres_uri(),
        embeddings_table=table,
        embeddings_provider=provider,
        embedding_model=_getenv("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        embedding_dimensions=_getenv_int("EMBEDDING_DIMENSIONS", DEFAULT_EMBEDDING_DIMENSIONS),
        openai_api_key=api_key,
        openai_base_url=_getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL) or None,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        document_event_types=event_types,
        http_host=_getenv("HTTP_HOST", "0.0.0.0"),
        http_port=_getenv_int("HTTP_PORT", 8000),
        log_level=_getenv("LOG_LEVEL", "INFO").upper(),
    )


def _postgres_uri() -> str:
    """Resolve the Postgres URI, preferring a full DSN over its parts.

    POSTGRES_URI (or DATABASE_URL) wins when set. Otherwise the URI is assembled
    from POSTGRES_* variables, because docker-compose and the Helm chart only
    inject the host.
    """
    for key in ("POSTGRES_URI", "DATABASE_URL"):
        if uri := _getenv(key, ""):
            return uri

    user = _getenv("POSTGRES_USER", "postgres")
    password = _getenv("POSTGRES_PASSWORD", "postgres")
    host = _getenv("POSTGRES_HOST", "localhost")
    port = _getenv("POSTGRES_PORT", "5432")
    database = _getenv("POSTGRES_DB", "eventpulse_db")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _with_driver(uri: str, driver: str | None) -> str:
    """Rewrite the scheme of a Postgres URI to select a specific DBAPI driver.

    `postgresql://` is what operators paste into the environment, but SQLAlchemy
    needs `postgresql+asyncpg://` and psycopg only accepts the bare form.
    """
    scheme, separator, rest = uri.partition("://")
    if not separator:
        raise ConfigError(f"Postgres URI must start with a scheme, got {uri!r}")

    base = scheme.split("+", 1)[0]

    return f"{base}+{driver}{separator}{rest}" if driver else f"{base}{separator}{rest}"


def _getenv(key: str, fallback: str) -> str:
    """Return the trimmed variable, falling back when unset or empty."""
    return os.environ.get(key, "").strip() or fallback


def _getenv_int(key: str, fallback: int) -> int:
    raw = _getenv(key, "")
    if not raw:
        return fallback

    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from exc

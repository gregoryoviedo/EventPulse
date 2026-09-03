"""Infrastructure adapter for the pgvector-backed embedding store.

Two connections to Postgres are used on purpose:

* psycopg (sync) provisions the schema and prunes stale chunks, which are plain
  DDL/DML statements this service owns.
* langchain-postgres' PGVectorStore (asyncpg, driven from a background loop)
  owns the embedding writes, so the LangChain contract is not reimplemented.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable, Sequence

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_postgres import PGEngine, PGVectorStore
from psycopg import sql
from psycopg_pool import ConnectionPool

from config import ConfigError, Settings

# Payload keys promoted to real columns; anything else lands in the
# langchain_metadata JSON column.
METADATA_COLUMNS = ["event_id", "document_id", "source", "chunk_index"]

_SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")


class EmbeddingStore:
    """Persists document chunks and their vectors in a single table."""

    def __init__(
        self,
        engine: PGEngine,
        vector_store: PGVectorStore,
        pool: ConnectionPool,
        table: str,
        logger: logging.Logger,
    ) -> None:
        self._engine = engine
        self._vector_store = vector_store
        self._pool = pool
        self._table = table
        self._logger = logger

    @classmethod
    def bootstrap(
        cls,
        settings: Settings,
        embeddings: Embeddings,
        logger: logging.Logger,
    ) -> EmbeddingStore:
        """Provision the table and map it onto a PGVectorStore.

        Runs before the consumer starts so a broken database or a wrong vector
        size fails the process instead of every single message.
        """
        pool = ConnectionPool(settings.sync_postgres_uri, min_size=1, max_size=2, open=True)
        pool.wait(timeout=30)

        apply_schema(pool, settings.embeddings_table, settings.embedding_dimensions)
        logger.info(
            "vector store schema ready",
            extra={
                "fields": {
                    "table": settings.embeddings_table,
                    "dimensions": settings.embedding_dimensions,
                }
            },
        )

        engine = PGEngine.from_connection_string(url=settings.async_postgres_uri)
        # Distance strategy is left at its default (cosine), which is what the
        # hnsw vector_cosine_ops index in schema.sql is built for.
        vector_store = PGVectorStore.create_sync(
            engine=engine,
            embedding_service=embeddings,
            table_name=settings.embeddings_table,
            metadata_columns=METADATA_COLUMNS,
        )

        return cls(engine, vector_store, pool, settings.embeddings_table, logger)

    def replace_document(
        self,
        document_id: str,
        ids: Sequence[str],
        chunks: Sequence[Document],
    ) -> None:
        """Write the chunks of a document and drop the ones it no longer has.

        Kafka delivers at least once, so writes must be idempotent: the ids are
        derived from the document, which turns the insert into an upsert. Chunks
        left over from a longer previous revision are deleted afterwards, never
        before, so a failure mid-way cannot leave the document unsearchable.
        """
        self._vector_store.add_documents(list(chunks), ids=list(ids))
        self._prune(document_id, ids)

    def search(self, query: str, top_k: int) -> list[tuple[Document, float]]:
        """Retrieve the chunks closest to a query, best first.

        The store is built with the cosine distance strategy, so LangChain's
        similarity_search_with_score returns a distance (0 = identical, higher =
        farther apart). It is converted to a cosine similarity score
        (1 - distance) so a higher number means a better match.
        """
        hits = self._vector_store.similarity_search_with_score(query, k=top_k)

        return [(document, 1.0 - distance) for document, distance in hits]

    def _prune(self, document_id: str, keep: Iterable[str]) -> None:
        statement = sql.SQL(
            "DELETE FROM {table} WHERE document_id = %s AND NOT (langchain_id = ANY(%s::uuid[]))"
        ).format(table=sql.Identifier(self._table))

        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(statement, (document_id, list(keep)))
            if cur.rowcount > 0:
                self._logger.info(
                    "pruned stale chunks",
                    extra={"fields": {"document_id": document_id, "deleted": cur.rowcount}},
                )

    def close(self) -> None:
        """Release the psycopg pool.

        The PGEngine runs its asyncpg pool on a daemon thread that ends with the
        process, so there is nothing to await here.
        """
        self._pool.close()


def apply_schema(pool: ConnectionPool, table: str, dimensions: int) -> None:
    """Execute schema.sql for the configured table.

    Identifiers cannot be bound as parameters, so they are composed with
    psycopg's SQL objects; `table` is validated as an identifier in config.py.
    """
    with open(_SCHEMA_FILE, encoding="utf-8") as handle:
        template = handle.read()

    statement = sql.SQL(template).format(  # type: ignore[arg-type]
        table=sql.Identifier(table),
        dimensions=sql.Literal(dimensions),
        document_index=sql.Identifier(f"{table}_document_id_idx"),
        embedding_index=sql.Identifier(f"{table}_embedding_hnsw_idx"),
    )

    with pool.connection() as conn:
        conn.execute(statement)


def build_embeddings(settings: Settings) -> Embeddings:
    """Instantiate the embedding model described by the environment.

    * openai: OpenAI client pointed at OPENAI_BASE_URL (defaulting to the OpenCode
      zen gateway), which is how the service talks to an OpenAI-compatible gateway
      instead of api.openai.com.
    * huggingface: Hugging Face serverless models through the current
      InferenceClient-based adapter (HuggingFaceEndpointEmbeddings), which routes
      to router.huggingface.co. The endpoint is probed at boot so a wrong token,
      an unknown model or a dimension mismatch with the table fails the process
      instead of every single message.
    """
    if settings.embeddings_provider == "openai":
        from langchain_openai import OpenAIEmbeddings

        kwargs: dict[str, object] = {
            "model": settings.embedding_model,
            "openai_api_key": settings.openai_api_key,
        }
        if settings.openai_base_url:
            kwargs["openai_api_base"] = settings.openai_base_url
        # Shortening the vector is only supported by the text-embedding-3 family;
        # other models reject the parameter outright.
        if settings.embedding_model.startswith("text-embedding-3"):
            kwargs["dimensions"] = settings.embedding_dimensions

        return OpenAIEmbeddings(**kwargs)  # type: ignore[arg-type]

    if settings.embeddings_provider == "huggingface":
        # HuggingFaceInferenceAPIEmbeddings (langchain-community) hardcodes the
        # retired api-inference.huggingface.co endpoint, which Hugging Face
        # turned off in favour of router.huggingface.co. HuggingFaceEndpointEmbeddings
        # (langchain-huggingface) reaches the same serverless models through the
        # current InferenceClient-based API, so it is used instead.
        from langchain_huggingface import HuggingFaceEndpointEmbeddings

        embeddings = HuggingFaceEndpointEmbeddings(
            model=settings.embedding_model,
            huggingfacehub_api_token=settings.huggingfacehub_api_token,
        )
        _verify_embedding_dimensions(embeddings, settings)

        return embeddings

    # Offline stand-in so the pipeline can be exercised without an API key.
    from langchain_core.embeddings import DeterministicFakeEmbedding

    return DeterministicFakeEmbedding(size=settings.embedding_dimensions)


def _verify_embedding_dimensions(embeddings: Embeddings, settings: Settings) -> None:
    """Probe the embedding endpoint at boot so failures surface early.

    A rejected token, an unknown model (404) or a model whose vector width differs
    from the vector(n) column is caught here, at startup, instead of on the first
    Kafka message.
    """
    try:
        vector = embeddings.embed_query("eventpulse dimension probe")
    except Exception as exc:  # noqa: BLE001 - surfaced as a configuration problem
        raise ConfigError(
            f"embedding endpoint for {settings.embedding_model!r} failed: {exc}"
        ) from exc

    width = len(vector)
    if width != settings.embedding_dimensions:
        raise ConfigError(
            f"model {settings.embedding_model!r} returns {width}-dimension vectors "
            f"but EMBEDDING_DIMENSIONS is {settings.embedding_dimensions}; "
            "set EMBEDDING_DIMENSIONS to match the model (bge-large-en-v1.5 = 1024)"
        )

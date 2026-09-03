"""document-processor: the RAG indexing consumer of the eventpulse platform.

It consumes the `raw.events` Kafka topic, picks the `document_uploaded` events,
splits `payload.content` with LangChain's RecursiveCharacterTextSplitter, embeds
the chunks and stores them in the `document_embeddings` pgvector table, which the
mcp-server later queries for retrieval.

This module is the composition root: it is the only place where configuration,
the Kafka consumer, the LangChain pipeline and the HTTP health server are wired
together. The consumer owns the main thread; the health server runs beside it.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from fastapi import FastAPI, Response, status
from pydantic import BaseModel, Field

from config import ConfigError, Settings, load_settings
from consumer import ConsumerStats, RawEventConsumer
from events import RawEvent, UploadedDocument
from logging_config import setup_logging
from pipeline import DocumentPipeline
from store import EmbeddingStore, build_embeddings

# Shared with the consumer so the probes can report what the loop is doing.
STATS = ConsumerStats()

app = FastAPI(title="eventpulse-document-processor")

# How long the health server is given to stop before the process exits anyway.
_HEALTH_SHUTDOWN_TIMEOUT_SECONDS = 5.0


class SearchRequest(BaseModel):
    """Body of POST /api/v1/search."""

    query: str = Field(min_length=1, description="Text to search for")
    top_k: int = Field(default=3, ge=1, description="Number of chunks to return")


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is up and serving."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response) -> dict:
    """Readiness: the Kafka consumer loop is actually running."""
    snapshot = STATS.snapshot()
    if not snapshot["running"]:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ok" if snapshot["running"] else "starting", **snapshot}


@app.post("/api/v1/search")
def search(request: SearchRequest, response: Response) -> dict:
    """Return the chunks most similar to a query, with relevance scores.

    The store is wired onto the app by main() once the bootstrap finishes; before
    that the endpoint reports itself as not ready.
    """
    store = getattr(app.state, "store", None)
    if store is None:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

        return {"detail": "vector store is not ready yet"}

    hits = store.search(request.query, request.top_k)

    return {
        "query": request.query,
        "results": [
            {
                "chunk_id": document.id,
                "content": document.page_content,
                "score": score,
                "metadata": document.metadata,
            }
            for document, score in hits
        ],
    }


def main() -> int:
    """Run the service until a signal arrives; returns the process exit code."""
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"invalid configuration: {exc}", file=sys.stderr)

        return 1

    logger = setup_logging(settings.log_level)

    stop = threading.Event()
    _trap_signals(stop, logger)

    health = _start_health_server(settings)

    store = None
    try:
        # Fail fast on the dependencies: a missing table or an unreachable
        # embedding endpoint should stop the process, not every message.
        store = EmbeddingStore.bootstrap(settings, build_embeddings(settings), logger)
        app.state.store = store
        pipeline = DocumentPipeline.build(settings, store, logger)

        consumer = RawEventConsumer(settings, _handler(pipeline), logger, STATS)
        consumer.run(stop)
    except Exception as exc:  # noqa: BLE001 - top level guard, logged and reported
        logger.error("service stopped with error", exc_info=exc)

        return 1
    finally:
        if store is not None:
            store.close()
        _stop_health_server(health, stop)
        logger.info("shutdown complete", extra={"fields": STATS.snapshot()})

    return 0


def _handler(pipeline: DocumentPipeline):
    """Adapt a raw event to the indexing use case."""

    def handle(event: RawEvent) -> None:
        pipeline.index(UploadedDocument.from_event(event))

    return handle


def _trap_signals(stop: threading.Event, logger: logging.Logger) -> None:
    """Turn SIGINT/SIGTERM into a stop request, as `docker stop` and Kubernetes
    send SIGTERM when draining the container."""

    def request_stop(signum: int, _frame) -> None:
        logger.info("shutdown signal received, draining", extra={"fields": {"signal": signum}})
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)


def _start_health_server(settings: Settings) -> threading.Thread:
    """Serve the probes from a daemon thread, leaving the main thread to Kafka.

    Uvicorn skips its own signal handlers outside the main thread, so the
    handlers installed by _trap_signals stay in charge of the shutdown.
    """
    import uvicorn

    config = uvicorn.Config(
        app,
        host=settings.http_host,
        port=settings.http_port,
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, name="health-server", daemon=True)
    thread.start()

    # Stashed so the shutdown path can ask the server to exit gracefully.
    thread.server = server  # type: ignore[attr-defined]

    return thread


def _stop_health_server(thread: threading.Thread, stop: threading.Event) -> None:
    stop.set()

    server = getattr(thread, "server", None)
    if server is not None:
        server.should_exit = True

    thread.join(timeout=_HEALTH_SHUTDOWN_TIMEOUT_SECONDS)


if __name__ == "__main__":
    sys.exit(main())

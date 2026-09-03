"""The RAG indexing use case: document in, embedded chunks out.

This is the layer that owns the chunking policy and the shape of the metadata
stored next to each vector. It depends on the EmbeddingStore abstraction, not on
Postgres or on an embedding provider.
"""

from __future__ import annotations

import logging
import uuid
from typing import Sequence

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import Settings
from events import InvalidEventError, UploadedDocument
from store import EmbeddingStore

# Namespace for the deterministic chunk ids. Any fixed UUID works; deriving it
# from a URL keeps it self-describing instead of an opaque constant.
_CHUNK_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://eventpulse.dev/document-chunks")


class DocumentPipeline:
    """Splits a document into chunks and persists them with their vectors."""

    def __init__(
        self,
        splitter: RecursiveCharacterTextSplitter,
        store: EmbeddingStore,
        logger: logging.Logger,
    ) -> None:
        self._splitter = splitter
        self._store = store
        self._logger = logger

    @classmethod
    def build(
        cls,
        settings: Settings,
        store: EmbeddingStore,
        logger: logging.Logger,
    ) -> DocumentPipeline:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.chunk_size,
            chunk_overlap=settings.chunk_overlap,
            # Records where each chunk starts in the original text, which lets a
            # retrieval answer point back at the exact passage.
            add_start_index=True,
        )

        return cls(splitter, store, logger)

    def index(self, document: UploadedDocument) -> int:
        """Chunk, embed and store the document. Returns the number of chunks."""
        chunks = self._split(document)
        ids = [chunk_id(document.document_id, index) for index in range(len(chunks))]

        self._store.replace_document(document.document_id, ids, chunks)

        self._logger.info(
            "document indexed",
            extra={
                "fields": {
                    "event_id": document.event_id,
                    "document_id": document.document_id,
                    "source": document.source,
                    "chunks": len(chunks),
                }
            },
        )

        return len(chunks)

    def _split(self, document: UploadedDocument) -> Sequence[Document]:
        """Apply the recursive splitter and attach the retrieval metadata."""
        texts = self._splitter.create_documents(
            [document.content],
            metadatas=[dict(document.metadata)],
        )
        if not texts:
            raise InvalidEventError("splitting produced no chunks")

        for index, chunk in enumerate(texts):
            chunk.metadata.update(
                {
                    "event_id": document.event_id,
                    "document_id": document.document_id,
                    "source": document.source,
                    "chunk_index": index,
                }
            )

        return texts


def chunk_id(document_id: str, index: int) -> str:
    """Derive the primary key of a chunk from the document it belongs to.

    Kafka redelivers on failure, so ids must not be random: a replay of the same
    event has to upsert the same rows rather than duplicate the document.
    """
    return str(uuid.uuid5(_CHUNK_NAMESPACE, f"{document_id}#{index}"))

"""Wire contract shared with the rest of the platform.

RawEvent mirrors the envelope defined in Go at pkg/events.Event, which the
ingestion-gateway publishes to the `raw.events` topic:

    {"id": ..., "type": ..., "source": ..., "timestamp": ..., "payload": {...}}

UploadedDocument is the projection of that envelope this service cares about:
a `document_uploaded` event carrying the text to embed under `payload.content`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

# Payload keys that identify the document across re-uploads, in priority order.
_DOCUMENT_ID_KEYS = ("document_id", "documentId", "doc_id", "id")
# Payload key holding the text to embed.
_CONTENT_KEY = "content"


class InvalidEventError(ValueError):
    """The message cannot become a document, no matter how often it is retried.

    Callers treat this as a poison message: log it and move the offset forward
    instead of blocking the partition.
    """


@dataclass(frozen=True)
class RawEvent:
    """A message consumed from the raw events topic."""

    id: str
    type: str
    source: str
    timestamp: int
    payload: Mapping[str, Any]

    @classmethod
    def from_bytes(cls, raw: bytes | None) -> RawEvent:
        """Decode the JSON envelope produced by the ingestion-gateway."""
        if not raw:
            raise InvalidEventError("message has an empty value")

        try:
            decoded = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise InvalidEventError(f"message is not valid JSON: {exc}") from exc

        if not isinstance(decoded, dict):
            raise InvalidEventError("message must be a JSON object")

        payload = decoded.get("payload") or {}
        if not isinstance(payload, dict):
            raise InvalidEventError("payload must be a JSON object")

        event_type = decoded.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise InvalidEventError("type is required")

        return cls(
            id=str(decoded.get("id") or ""),
            type=event_type,
            source=str(decoded.get("source") or ""),
            timestamp=int(decoded.get("timestamp") or 0),
            payload=payload,
        )


@dataclass(frozen=True)
class UploadedDocument:
    """A document ready to be chunked and embedded."""

    event_id: str
    document_id: str
    source: str
    content: str
    # Everything else in the payload, carried along so retrieval can filter and
    # cite the chunk later.
    metadata: Mapping[str, Any]

    @classmethod
    def from_event(cls, event: RawEvent) -> UploadedDocument:
        """Project a `document_uploaded` event onto a document.

        Raises InvalidEventError when `payload.content` is missing or blank:
        there is nothing to embed and redelivering will not change that.
        """
        content = event.payload.get(_CONTENT_KEY)
        if not isinstance(content, str) or not content.strip():
            raise InvalidEventError("payload.content must be a non-empty string")

        document_id = _document_id(event)

        # Only JSON scalars and containers reach here (the payload came from
        # json.loads), so the leftovers are safe to store as JSON metadata.
        metadata = {
            key: value
            for key, value in event.payload.items()
            if key != _CONTENT_KEY and key not in _DOCUMENT_ID_KEYS
        }

        return cls(
            event_id=event.id,
            document_id=document_id,
            source=event.source,
            content=content,
            metadata=metadata,
        )


def _document_id(event: RawEvent) -> str:
    """Pick the stable identity of the document.

    A caller-supplied id lets a re-upload replace the previous chunks; without
    one the event id is used, so each delivery of the same event still maps to
    the same rows.
    """
    for key in _DOCUMENT_ID_KEYS:
        value = event.payload.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()

    if event.id:
        return event.id

    raise InvalidEventError("cannot derive a document id: event has no id")

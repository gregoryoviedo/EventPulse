"""Structured logging that matches the JSON lines emitted by the Go services.

The Go side uses slog with a JSON handler, so a single log pipeline can parse
every service if Python writes the same shape: time, level, msg, service.
Extra fields travel in `extra={"fields": {...}}`.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from config import SERVICE_NAME


class JSONFormatter(logging.Formatter):
    """Renders a record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
            "service": SERVICE_NAME,
        }

        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)

        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def setup_logging(level: str) -> logging.Logger:
    """Install the JSON formatter on the root logger and return ours."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(getattr(logging, level, logging.INFO))

    # Uvicorn installs its own handlers; drop them so access logs stay JSON.
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True

    return logging.getLogger(SERVICE_NAME)

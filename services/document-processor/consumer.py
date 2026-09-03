"""Kafka delivery layer: turns messages on `raw.events` into pipeline calls.

Offsets are committed by hand, after the handler has persisted the vectors, so
the service keeps at-least-once semantics: a crash re-delivers the message and
the pipeline's deterministic ids turn the retry into an upsert.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from config import Settings
from events import InvalidEventError, RawEvent

# How long a single poll waits before the loop re-checks the stop flag.
_POLL_TIMEOUT_SECONDS = 1.0
# A handler failure is usually the embedding endpoint or Postgres being briefly
# unavailable, so it is retried in-process before giving up on the message.
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 2.0

EventHandler = Callable[[RawEvent], None]


@dataclass
class ConsumerStats:
    """Counters exposed by the health endpoint."""

    consumed: int = 0
    processed: int = 0
    skipped: int = 0
    rejected: int = 0
    running: bool = False
    last_error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "consumed": self.consumed,
                "processed": self.processed,
                "skipped": self.skipped,
                "rejected": self.rejected,
                "running": self.running,
                "last_error": self.last_error,
            }


class RawEventConsumer:
    """Consumes the raw events topic and dispatches document events."""

    def __init__(
        self,
        settings: Settings,
        handler: EventHandler,
        logger: logging.Logger,
        stats: ConsumerStats | None = None,
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._logger = logger
        self._stats = stats or ConsumerStats()
        self._consumer = Consumer(self._client_config(settings), logger=logger)

    @property
    def stats(self) -> ConsumerStats:
        return self._stats

    def _client_config(self, settings: Settings) -> dict[str, object]:
        return {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            # Offsets move forward only once the chunks are durable in Postgres.
            "enable.auto.commit": False,
            "client.id": f"{settings.kafka_group_id}-{socket.gethostname()}",
        }

    def run(self, stop: threading.Event) -> None:
        """Poll until `stop` is set, then leave the consumer group cleanly."""
        self._consumer.subscribe([self._settings.kafka_topic])
        self._stats.running = True

        self._logger.info(
            "kafka consumer started",
            extra={
                "fields": {
                    "brokers": self._settings.kafka_brokers,
                    "topic": self._settings.kafka_topic,
                    "group_id": self._settings.kafka_group_id,
                }
            },
        )

        try:
            while not stop.is_set():
                message = self._consumer.poll(_POLL_TIMEOUT_SECONDS)
                if message is None:
                    continue
                if not self._check_message_error(message):
                    continue

                self._consume(message)
        finally:
            self._stats.running = False
            self.close()

    def _check_message_error(self, message: Message) -> bool:
        """Report whether the message carries data rather than an error event."""
        error = message.error()
        if error is None:
            return True

        if error.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001 - library constant
            return False

        if error.retriable():
            self._logger.warning("transient kafka error", extra={"fields": {"error": str(error)}})

            return False

        raise KafkaException(error)

    def _consume(self, message: Message) -> None:
        """Handle one message and advance the offset past it."""
        self._stats.consumed += 1

        try:
            event = RawEvent.from_bytes(message.value())
        except InvalidEventError as exc:
            # Undecodable payload: committing keeps the partition moving instead
            # of retrying a message that can never succeed.
            self._reject(message, str(exc))
            self._commit(message)

            return

        if event.type not in self._settings.document_event_types:
            self._stats.skipped += 1
            self._logger.debug(
                "event type not handled",
                extra={"fields": {"event_id": event.id, "event_type": event.type}},
            )
            self._commit(message)

            return

        self._dispatch(event, message)
        self._commit(message)

    def _dispatch(self, event: RawEvent, message: Message) -> None:
        """Run the handler, retrying transient failures.

        InvalidEventError means the event itself is unusable, so it is dropped.
        Anything else is retried and, if it keeps failing, propagated: the
        process exits without committing and the message is redelivered.
        """
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                self._handler(event)
                self._stats.processed += 1

                return
            except InvalidEventError as exc:
                self._reject(message, str(exc))

                return
            except Exception as exc:  # noqa: BLE001 - retry policy is intentional
                self._stats.last_error = str(exc)

                if attempt == _MAX_ATTEMPTS:
                    self._logger.error(
                        "giving up on event, offset not committed",
                        extra={"fields": {"event_id": event.id, "attempts": attempt}},
                        exc_info=exc,
                    )

                    raise

                self._logger.warning(
                    "event processing failed, retrying",
                    extra={
                        "fields": {
                            "event_id": event.id,
                            "attempt": attempt,
                            "error": str(exc),
                        }
                    },
                )
                time.sleep(_RETRY_BACKOFF_SECONDS * attempt)

    def _reject(self, message: Message, reason: str) -> None:
        self._stats.rejected += 1
        self._stats.last_error = reason
        self._logger.warning(
            "dropping unprocessable message",
            extra={
                "fields": {
                    "reason": reason,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            },
        )

    def _commit(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        """Leave the group so the broker can reassign the partitions at once."""
        try:
            self._consumer.close()
        except (KafkaException, RuntimeError) as exc:
            self._logger.warning(
                "could not close kafka consumer",
                extra={"fields": {"error": str(exc)}},
            )

"""
Kafka Consumer — subscribes to ``conduit.metrics.computed`` and
``conduit.events.ingested``
=======================================================================

Wraps the confluent-kafka ``Consumer`` with:

* Automatic DLQ routing on processing errors (mirrors the behaviour of
  the Node.js ``consumeMessages`` helper in ``@conduit/shared``)
* Clean shutdown via a threading ``Event``
* Dry-run mode: polls messages but does not commit offsets

Usage::

    from src.kafka.consumer import MLConsumer
    from src.config import config

    consumer = MLConsumer(config.kafka, message_handler)
    consumer.start()           # blocking — call from a background thread
    # from main thread:
    consumer.stop()
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable, Dict

from src.config import KafkaConfig

logger = logging.getLogger(__name__)

# Type alias for the message handler provided by the caller
MessageHandler = Callable[[Dict[str, Any], str], None]


class MLConsumer:
    """
    Thin confluent-kafka consumer wrapper.

    Parameters
    ----------
    kafka_cfg:
        :class:`~src.config.KafkaConfig` from the global singleton.
    handler:
        Callable invoked for each successfully decoded message::

            handler(value: dict, topic: str) -> None

        The handler runs synchronously inside the poll loop, so it should
        be fast.  Any exception raised by the handler is caught, logged,
        and the message is routed to the DLQ.
    """

    def __init__(self, kafka_cfg: KafkaConfig, handler: MessageHandler) -> None:
        self._topics = kafka_cfg.topics_consume
        self._dlq_topic = kafka_cfg.topic_dlq
        self._dry_run = kafka_cfg.dry_run
        self._handler = handler
        self._stop_event = threading.Event()
        self._consumer = None
        self._dlq_producer = None

        if not self._dry_run:
            try:
                from confluent_kafka import Consumer, Producer  # type: ignore[import-untyped]

                self._consumer = Consumer(
                    {
                        "bootstrap.servers": kafka_cfg.broker_str,
                        "group.id": kafka_cfg.group_id,
                        "client.id": f"{kafka_cfg.client_id}-consumer",
                        "auto.offset.reset": kafka_cfg.auto_offset_reset,
                        "enable.auto.commit": True,
                        "max.poll.interval.ms": kafka_cfg.max_poll_interval_ms,
                        # Backoff between reconnection attempts
                        "reconnect.backoff.ms": 1_000,
                        "reconnect.backoff.max.ms": 10_000,
                    }
                )
                # Dedicated lightweight producer for DLQ routing
                self._dlq_producer = Producer(
                    {
                        "bootstrap.servers": kafka_cfg.broker_str,
                        "client.id": f"{kafka_cfg.client_id}-dlq",
                    }
                )
                logger.info(
                    "[MLConsumer] Subscribed to topics: %s", ", ".join(self._topics)
                )
            except Exception as exc:  # pragma: no cover
                logger.error("[MLConsumer] Failed to initialise: %s", exc)
                raise

    # ─────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Subscribe and enter the poll loop.

        **Blocking** — run inside a background thread::

            t = threading.Thread(target=consumer.start, daemon=True)
            t.start()
        """
        if self._dry_run:
            logger.info("[MLConsumer] Dry-run mode — Kafka not connected, poll loop idle")
            self._stop_event.wait()
            return

        self._consumer.subscribe(self._topics)  # type: ignore[union-attr]

        try:
            while not self._stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)  # type: ignore[union-attr]
                if msg is None:
                    continue
                if msg.error():
                    self._handle_kafka_error(msg)
                    continue
                self._dispatch(msg)
        finally:
            self._consumer.close()  # type: ignore[union-attr]
            logger.info("[MLConsumer] Consumer closed")

    def stop(self) -> None:
        """Signal the poll loop to exit on its next iteration."""
        self._stop_event.set()
        logger.info("[MLConsumer] Stop signal sent")

    # ─────────────────────────────────────────────────────────────────
    #  Internal helpers
    # ─────────────────────────────────────────────────────────────────

    def _dispatch(self, msg: Any) -> None:
        """Decode and dispatch a single Kafka message to the handler."""
        topic = msg.topic()
        raw = msg.value()

        try:
            value = json.loads(raw.decode("utf-8"))
            self._handler(value, topic)
        except Exception as exc:
            logger.error(
                "[MLConsumer] Error processing message on %s: %s", topic, exc
            )
            self._route_to_dlq(topic, raw, str(exc))

    def _route_to_dlq(self, original_topic: str, raw_value: bytes, error: str) -> None:
        """
        Publish a failed message to the DLQ topic.

        Mirrors the Node.js ``publishToDLQ`` helper in ``@conduit/shared``.
        The DLQ message envelope includes the ``originalTopic`` header so that
        ops tooling can route alerts correctly.
        """
        if self._dlq_producer is None:
            return

        try:
            self._dlq_producer.produce(
                topic=self._dlq_topic,
                key=original_topic.encode("utf-8"),
                value=raw_value,
                headers={
                    "originalTopic": original_topic,
                    "error": error[:500],  # truncate very long errors
                    "source": "conduit-ml-service",
                },
            )
            self._dlq_producer.poll(0)
            logger.warning(
                "[MLConsumer] Message routed to DLQ topic=%s originalTopic=%s",
                self._dlq_topic,
                original_topic,
            )
        except Exception as dlq_exc:  # pragma: no cover
            logger.error("[MLConsumer] FATAL: Failed to publish to DLQ: %s", dlq_exc)

    @staticmethod
    def _handle_kafka_error(msg: Any) -> None:
        """Log confluent-kafka partition EOF and real errors."""
        from confluent_kafka import KafkaError  # type: ignore[import-untyped]

        if msg.error().code() == KafkaError._PARTITION_EOF:
            logger.debug(
                "[MLConsumer] End of partition: %s [%d] at offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
        else:
            logger.error("[MLConsumer] Kafka error: %s", msg.error())

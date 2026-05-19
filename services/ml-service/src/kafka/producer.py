"""
Kafka Producer — publishes ML predictions to ``conduit.ml.predictions``
=========================================================================

Wraps the confluent-kafka ``Producer`` in a thin class that handles:

* JSON serialisation
* Delivery-report logging
* Graceful flush on shutdown
* Dry-run mode (messages are logged but not sent — safe for local dev
  without a running Kafka cluster)

Usage::

    from src.kafka.producer import MLProducer
    from src.config import config

    producer = MLProducer(config.kafka)
    producer.publish_prediction(prediction_dict)
    # ... on shutdown:
    producer.flush()
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from src.config import KafkaConfig

logger = logging.getLogger(__name__)


class MLProducer:
    """
    Thin confluent-kafka producer wrapper.

    Parameters
    ----------
    kafka_cfg:
        :class:`~src.config.KafkaConfig` instance (from the singleton
        ``config.kafka``).
    """

    def __init__(self, kafka_cfg: KafkaConfig) -> None:
        self._topic = kafka_cfg.topic_predictions
        self._dry_run = kafka_cfg.dry_run
        self._producer = None

        if not self._dry_run:
            try:
                from confluent_kafka import Producer  # type: ignore[import-untyped]

                self._producer = Producer(
                    {
                        "bootstrap.servers": kafka_cfg.broker_str,
                        "client.id": kafka_cfg.client_id,
                        # Wait up to 10 s for broker acks before declaring failure
                        "message.timeout.ms": 10_000,
                        # Idempotent delivery (exactly-once semantics)
                        "enable.idempotence": True,
                    }
                )
                logger.info("[MLProducer] Connected to %s", kafka_cfg.broker_str)
            except Exception as exc:  # pragma: no cover
                logger.error("[MLProducer] Failed to connect: %s", exc)
                raise
        else:
            logger.info("[MLProducer] Dry-run mode — messages will NOT be sent to Kafka")

    # ─────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────

    def publish_prediction(self, prediction: Dict[str, Any]) -> None:
        """
        Serialise and publish a prediction dict to ``conduit.ml.predictions``.

        The ``tenantId`` field is used as the Kafka message key so that all
        predictions for the same tenant land on the same partition (ordered
        delivery per tenant).

        Parameters
        ----------
        prediction:
            Dict conforming to the ML prediction schema documented in
            ``09_ml_service.md``.
        """
        tenant_id = prediction.get("tenantId", "unknown")
        payload = json.dumps(prediction, default=str).encode("utf-8")

        if self._dry_run:
            logger.info(
                "[MLProducer] DRY-RUN: would publish tenant=%s score=%.3f label=%s",
                tenant_id,
                prediction.get("anomalyScore", 0.0),
                prediction.get("label"),
            )
            return

        self._producer.produce(  # type: ignore[union-attr]
            topic=self._topic,
            key=tenant_id.encode("utf-8"),
            value=payload,
            headers={
                "content-type": "application/json",
                "published-at": prediction.get("timestamp", ""),
                "source": "conduit-ml-service",
            },
            on_delivery=self._delivery_report,
        )
        # Non-blocking poll — triggers delivery callbacks without waiting
        self._producer.poll(0)  # type: ignore[union-attr]

    def flush(self, timeout: float = 15.0) -> None:
        """
        Block until all queued messages are delivered (or timeout).

        Call this during graceful shutdown so in-flight predictions are not
        lost.
        """
        if self._producer is not None:
            remaining = self._producer.flush(timeout)
            if remaining > 0:
                logger.warning(
                    "[MLProducer] %d message(s) not delivered before flush timeout",
                    remaining,
                )
            else:
                logger.info("[MLProducer] All messages flushed")

    # ─────────────────────────────────────────────────────────────────
    #  Internal helpers
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _delivery_report(err: Any, msg: Any) -> None:
        """Called by confluent-kafka for every delivered / failed message."""
        if err is not None:
            logger.error(
                "[MLProducer] Delivery failed topic=%s key=%s error=%s",
                msg.topic(),
                msg.key(),
                err,
            )
        else:
            logger.debug(
                "[MLProducer] Delivered topic=%s partition=%d offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

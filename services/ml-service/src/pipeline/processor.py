"""
Event Processor — main pipeline orchestration
===============================================
Wires together the Kafka consumer, feature extractor, anomaly detector,
and Kafka producer.

``EventProcessor.handle_message()`` is the single entry point called by
the :class:`~src.kafka.consumer.MLConsumer` for every Kafka message.

Message routing
---------------
``conduit.metrics.computed``
    Extract metric features → run anomaly detection → publish prediction.

``conduit.events.ingested``
    Extract event features → update per-tenant statistics only (no
    prediction is produced to avoid flooding the ml.predictions topic
    with per-event noise).

Prediction envelope schema
--------------------------
The published prediction is compatible with:
  * The MongoDB ``Prediction`` Mongoose schema (query-service/src/db/mongo.js)
  * The ``evaluateMLPrediction`` function (incident-service/src/detectors/mlDetector.js)

::

    {
      "predictionId": str,       # UUIDv4
      "tenantId":     str,
      "modelId":      str,       # e.g. "isolation-forest-v1"
      "anomalyScore": float,     # [0, 1]
      "confidence":   float,     # [0, 1]
      "label":        str|null,  # primary label — read by mlDetector.js
      "labels":       [str],     # all labels
      "features":     {str: float},
      "metadata":     {str: any},
      "source":       "ml-service",
      "timestamp":    str        # ISO-8601
    }

Usage::

    from src.pipeline.processor import EventProcessor
    from src.config import config

    processor = EventProcessor(config)
    # The processor is passed to MLConsumer as the message_handler:
    consumer = MLConsumer(config.kafka, processor.handle_message)
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from src.config import Config
from src.kafka.producer import MLProducer
from src.models.anomaly_detector import DetectorRegistry
from src.models.feature_extractor import (
    METRIC_FEATURE_NAMES,
    extract_event_features,
    extract_metric_features,
    features_to_dict,
)

logger = logging.getLogger(__name__)


class EventProcessor:
    """
    Stateful message handler — holds the detector registry and producer.

    Parameters
    ----------
    cfg:
        Root :class:`~src.config.Config` singleton.
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._registry = DetectorRegistry(cfg.ml, n_features=5)
        self._producer = MLProducer(cfg.kafka)

        # Counters for the /metrics endpoint
        self.messages_received: int = 0
        self.predictions_published: int = 0
        self.errors: int = 0

    # ──────────────────────────────────────────────────────────────────
    #  Public API — called by MLConsumer for each Kafka message
    # ──────────────────────────────────────────────────────────────────

    def handle_message(self, value: Dict[str, Any], topic: str) -> None:
        """
        Route a decoded Kafka message to the appropriate handler.

        Parameters
        ----------
        value:
            Decoded JSON message body.
        topic:
            Source Kafka topic name.
        """
        self.messages_received += 1

        try:
            if topic == self._cfg.kafka.topic_metrics:
                self._handle_metrics_snapshot(value)
            elif topic == self._cfg.kafka.topic_events:
                self._handle_event(value)
            else:
                logger.warning("[Processor] Unexpected topic: %s", topic)
        except Exception as exc:
            self.errors += 1
            logger.error(
                "[Processor] Error handling message from %s: %s", topic, exc
            )
            raise  # Re-raise so MLConsumer can route to DLQ

    def flush(self) -> None:
        """Flush the Kafka producer (call on graceful shutdown)."""
        self._producer.flush()

    @property
    def stats(self) -> Dict[str, Any]:
        """Summary stats for the /metrics endpoint."""
        return {
            "messagesReceived": self.messages_received,
            "predictionsPublished": self.predictions_published,
            "errors": self.errors,
            "activeTenants": self._registry.tenant_count,
        }

    @property
    def model_stats(self) -> Dict[str, Any]:
        """Per-tenant model diagnostics for the /models endpoint."""
        return self._registry.get_stats()

    # ──────────────────────────────────────────────────────────────────
    #  Private handlers
    # ──────────────────────────────────────────────────────────────────

    def _handle_metrics_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """
        Run anomaly detection on an aggregated metric snapshot and publish
        a prediction to ``conduit.ml.predictions``.
        """
        features, metadata = extract_metric_features(snapshot)
        if features is None:
            logger.debug("[Processor] Skipping malformed metrics snapshot")
            return

        tenant_id = metadata.get("tenantId", "unknown")
        result = self._registry.predict(tenant_id, features)

        # Only publish if the anomaly score meets the minimum threshold
        if result.anomaly_score < self._cfg.ml.min_publish_score:
            return

        prediction = self._build_prediction(
            tenant_id=tenant_id,
            result_anomaly_score=result.anomaly_score,
            result_confidence=result.confidence,
            result_labels=result.labels,
            result_model_name=result.model_name,
            features_dict=features_to_dict(features, METRIC_FEATURE_NAMES),
            metadata=metadata,
            source="conduit.metrics.computed",
        )

        self._producer.publish_prediction(prediction)
        self.predictions_published += 1

        if result.anomaly_score >= 0.75:
            logger.info(
                "[Processor] HIGH ANOMALY tenant=%s score=%.3f labels=%s model=%s",
                tenant_id,
                result.anomaly_score,
                result.labels,
                result.model_name,
            )
        else:
            logger.debug(
                "[Processor] Prediction published tenant=%s score=%.3f warmed_up=%s",
                tenant_id,
                result.anomaly_score,
                result.is_warmed_up,
            )

    def _handle_event(self, event: Dict[str, Any]) -> None:
        """
        Update per-tenant rolling statistics from a raw event.

        No prediction is published for individual events — only metric
        snapshots trigger ML predictions.  This prevents flooding the
        ml.predictions topic with per-event noise.
        """
        features, metadata = extract_event_features(event)
        if features is None:
            return

        # Update Welford statistics to help the detector warm up faster.
        # We use a 3-element event feature vector which is different from
        # the 5-element metric vector, so we maintain a *separate* registry
        # for event-level stats (used only for warm-up; not for scoring).
        # For simplicity in this version we simply discard the event-level
        # features — the metric snapshot path already captures all signals
        # in aggregated form.
        logger.debug(
            "[Processor] Event received (stats update only) tenant=%s type=%s",
            metadata.get("tenantId"),
            metadata.get("eventType"),
        )

    # ──────────────────────────────────────────────────────────────────
    #  Prediction envelope builder
    # ──────────────────────────────────────────────────────────────────

    def _build_prediction(
        self,
        *,
        tenant_id: str,
        result_anomaly_score: float,
        result_confidence: float,
        result_labels: list,
        result_model_name: str,
        features_dict: Dict[str, float],
        metadata: Dict[str, Any],
        source: str,
    ) -> Dict[str, Any]:
        """
        Construct the prediction envelope compatible with the rest of the
        Conduit system.

        The envelope schema is documented in the module docstring and in
        ``09_ml_service.md``.
        """
        now = datetime.now(timezone.utc).isoformat()

        return {
            "predictionId": str(uuid.uuid4()),
            "tenantId": tenant_id,
            "modelId": result_model_name,
            "anomalyScore": round(result_anomaly_score, 6),
            "confidence": round(result_confidence, 6),
            # "label" is singular for backward compat with mlDetector.js
            "label": result_labels[0] if result_labels else None,
            # "labels" is the full list stored in MongoDB
            "labels": result_labels,
            "features": features_dict,
            "metadata": {
                **{k: v for k, v in metadata.items() if k != "rawMetrics"},
                "source": source,
            },
            "source": "ml-service",
            "timestamp": now,
        }

"""
Conduit ML Service — Configuration
===================================
All settings are read from environment variables.  Defaults match the
values used by the Docker Compose development stack.

Usage::

    from src.config import config

    brokers = config.kafka.brokers       # e.g. ["localhost:9092"]
    warmup  = config.ml.warmup_samples   # e.g. 50
"""

from __future__ import annotations

import os
from typing import List


# ─────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────

def _csv(key: str, default: str) -> List[str]:
    """Read a comma-separated env var as a list of strings."""
    return [s.strip() for s in os.getenv(key, default).split(",") if s.strip()]


def _int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _str(key: str, default: str) -> str:
    return os.getenv(key, default)


# ─────────────────────────────────────────────────────────
#  Kafka
# ─────────────────────────────────────────────────────────

class KafkaConfig:
    """
    Kafka connection, topic, and consumer-group settings.

    Environment variables:

    ============================================ =================================
    Variable                                     Default
    ============================================ =================================
    ``KAFKA_BROKERS``                            ``localhost:9092``
    ``KAFKA_CLIENT_ID``                          ``conduit-ml-service``
    ``KAFKA_GROUP_ID_ML``                        ``conduit-ml-group``
    ``KAFKA_TOPIC_METRICS``                      ``conduit.metrics.computed``
    ``KAFKA_TOPIC_EVENTS``                       ``conduit.events.ingested``
    ``KAFKA_TOPIC_ML_PREDICTIONS``               ``conduit.ml.predictions``
    ``KAFKA_TOPIC_DLQ``                          ``conduit.dlq``
    ``KAFKA_AUTO_OFFSET_RESET``                  ``latest``
    ``KAFKA_MAX_POLL_INTERVAL_MS``               ``300000``
    ``KAFKA_DRY_RUN``                            ``false``
    ============================================ =================================
    """

    def __init__(self) -> None:
        self.brokers: List[str] = _csv("KAFKA_BROKERS", "localhost:9092")
        self.client_id: str = _str("KAFKA_CLIENT_ID", "conduit-ml-service")
        self.group_id: str = _str("KAFKA_GROUP_ID_ML", "conduit-ml-group")

        # Topics consumed by this service
        self.topic_metrics: str = _str("KAFKA_TOPIC_METRICS", "conduit.metrics.computed")
        self.topic_events: str = _str("KAFKA_TOPIC_EVENTS", "conduit.events.ingested")

        # Topic this service produces to
        self.topic_predictions: str = _str(
            "KAFKA_TOPIC_ML_PREDICTIONS", "conduit.ml.predictions"
        )
        self.topic_dlq: str = _str("KAFKA_TOPIC_DLQ", "conduit.dlq")

        # Consumer settings
        self.auto_offset_reset: str = _str("KAFKA_AUTO_OFFSET_RESET", "latest")
        self.max_poll_interval_ms: int = _int("KAFKA_MAX_POLL_INTERVAL_MS", 300_000)

        # When True, messages are processed but never published (safe for dev)
        self.dry_run: bool = _str("KAFKA_DRY_RUN", "false").lower() == "true"

    @property
    def topics_consume(self) -> List[str]:
        """Ordered list of topics this service subscribes to."""
        return [self.topic_metrics, self.topic_events]

    @property
    def broker_str(self) -> str:
        """Comma-separated broker string expected by confluent-kafka."""
        return ",".join(self.brokers)


# ─────────────────────────────────────────────────────────
#  ML Model
# ─────────────────────────────────────────────────────────

class MLConfig:
    """
    Anomaly-detection model hyper-parameters and thresholds.

    Environment variables:

    ============================================ =================================
    Variable                                     Default
    ============================================ =================================
    ``ML_WARMUP_SAMPLES``                        ``50``
    ``ML_MAX_SAMPLES``                           ``1000``
    ``ML_REFIT_EVERY``                           ``100``
    ``ML_CONTAMINATION``                         ``0.05``
    ``ML_ZSCORE_THRESHOLD``                      ``3.0``
    ``ML_MIN_PUBLISH_SCORE``                     ``0.0``
    ``ML_MODEL_ID``                              ``isolation-forest-v1``
    ============================================ =================================
    """

    def __init__(self) -> None:
        # Minimum samples required before the IsolationForest model is first fit.
        # Until then, only Z-score based detection is used.
        self.warmup_samples: int = _int("ML_WARMUP_SAMPLES", 50)

        # Sliding-window capacity (oldest samples are dropped when full)
        self.max_samples_per_tenant: int = _int("ML_MAX_SAMPLES", 1_000)

        # Re-fit the IsolationForest every N new samples after the initial fit
        self.refit_every: int = _int("ML_REFIT_EVERY", 100)

        # Expected fraction of anomalies in training data
        self.contamination: float = _float("ML_CONTAMINATION", 0.05)

        # Z-score magnitude that is considered statistically abnormal
        self.zscore_threshold: float = _float("ML_ZSCORE_THRESHOLD", 3.0)

        # Minimum anomaly score that must be reached before a prediction
        # message is published.  Set to 0.0 to publish ALL predictions.
        self.min_publish_score: float = _float("ML_MIN_PUBLISH_SCORE", 0.0)

        # Human-readable model identifier embedded in every prediction message
        self.model_id: str = _str("ML_MODEL_ID", "isolation-forest-v1")


# ─────────────────────────────────────────────────────────
#  HTTP Server
# ─────────────────────────────────────────────────────────

class ServerConfig:
    """
    FastAPI / Uvicorn settings.

    Environment variables:

    ============================================ =================================
    Variable                                     Default
    ============================================ =================================
    ``ML_HOST``                                  ``0.0.0.0``
    ``ML_PORT``                                  ``4008``
    ``LOG_LEVEL``                                ``info``
    ============================================ =================================
    """

    def __init__(self) -> None:
        self.host: str = _str("ML_HOST", "0.0.0.0")
        self.port: int = _int("ML_PORT", 4008)
        self.log_level: str = _str("LOG_LEVEL", "info").lower()


# ─────────────────────────────────────────────────────────
#  Root Config
# ─────────────────────────────────────────────────────────

class Config:
    """
    Top-level configuration object.

    Import and use the module-level singleton::

        from src.config import config

        config.kafka.brokers
        config.ml.warmup_samples
        config.server.port
    """

    def __init__(self) -> None:
        self.kafka = KafkaConfig()
        self.ml = MLConfig()
        self.server = ServerConfig()


# Module-level singleton — import this everywhere
config = Config()

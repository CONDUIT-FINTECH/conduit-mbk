"""
Feature Extractor
==================
Converts raw Kafka messages into fixed-length numeric feature vectors
suitable for the :class:`~src.models.anomaly_detector.TenantAnomalyDetector`.

Two message shapes are handled:

``conduit.metrics.computed``
    Aggregated metric snapshot produced by the Metrics Service every
    ~25 events per tenant.  This is the **primary** signal for ML.

``conduit.events.ingested``
    Individual events from the Ingestion Service.  These are used to
    update per-tenant rolling statistics so the detector can warm up
    faster, but they do **not** generate a standalone prediction.

Feature vector for metric snapshots (length = 5):

    ``[error_rate, p95_latency_ms, success_rate, throughput, avg_latency_ms]``

    All values are returned in their natural units.  The anomaly detector
    is responsible for any normalisation it needs.

Usage::

    from src.models.feature_extractor import extract_metric_features
    from src.models.feature_extractor import extract_event_features

    # From a metrics.computed message:
    features, meta = extract_metric_features(snapshot_dict)

    # From an events.ingested message:
    features, meta = extract_event_features(event_dict)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

# ─── Feature vector indices ──────────────────────────────────────────
# Keeping these as module-level constants makes tests and downstream
# code more readable than bare magic integers.

METRIC_IDX_ERROR_RATE = 0       # float  [0, 1]
METRIC_IDX_P95_LATENCY = 1      # float  [0, ∞) ms
METRIC_IDX_SUCCESS_RATE = 2     # float  [0, 1]
METRIC_IDX_THROUGHPUT = 3       # int    [0, ∞) events/window
METRIC_IDX_AVG_LATENCY = 4      # float  [0, ∞) ms

METRIC_FEATURE_NAMES: List[str] = [
    "error_rate",
    "p95_latency_ms",
    "success_rate",
    "throughput",
    "avg_latency_ms",
]

EVENT_IDX_LATENCY = 0           # float  [0, ∞) ms  (0 if missing)
EVENT_IDX_IS_ERROR = 1          # float  0.0 or 1.0
EVENT_IDX_PAYLOAD_SIZE = 2      # float  [0, ∞) bytes (approximate)

EVENT_FEATURE_NAMES: List[str] = [
    "latency_ms",
    "is_error",
    "payload_size_bytes",
]


# ─────────────────────────────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────────────────────────────


def extract_metric_features(
    snapshot: Dict[str, Any],
) -> Tuple[Optional[List[float]], Dict[str, Any]]:
    """
    Build a feature vector from a ``conduit.metrics.computed`` message.

    Parameters
    ----------
    snapshot:
        Decoded Kafka message value (dict).

    Returns
    -------
    features:
        List of five floats, or ``None`` if the message is malformed.
    metadata:
        Auxiliary info (tenantId, window, sampleSize, computedAt) that is
        included in the prediction envelope but not passed to the ML model.
    """
    metrics = snapshot.get("metrics")
    if not isinstance(metrics, dict):
        return None, {}

    tenant_id = snapshot.get("tenantId", "unknown")
    computed_at = snapshot.get("computedAt", "")
    window = snapshot.get("window", "")
    sample_size = snapshot.get("sampleSize", 0)

    error_rate = _safe_float(metrics.get("errorRate"), 0.0)
    p95_latency = _safe_float(metrics.get("p95LatencyMs"), 0.0)
    success_rate = _safe_float(metrics.get("successRate"), 1.0)
    throughput = _safe_float(metrics.get("throughput"), 0.0)
    avg_latency = _safe_float(metrics.get("avgLatencyMs"), 0.0)

    features = [error_rate, p95_latency, success_rate, throughput, avg_latency]

    metadata = {
        "tenantId": tenant_id,
        "window": window,
        "sampleSize": sample_size,
        "computedAt": computed_at,
        "rawMetrics": {k: metrics.get(k) for k in metrics},
    }

    return features, metadata


def extract_event_features(
    event: Dict[str, Any],
) -> Tuple[Optional[List[float]], Dict[str, Any]]:
    """
    Build a lightweight feature vector from a ``conduit.events.ingested``
    message.

    Parameters
    ----------
    event:
        Decoded Kafka message value (dict).

    Returns
    -------
    features:
        List of three floats, or ``None`` if the message is malformed.
    metadata:
        Auxiliary info (tenantId, eventType, source, eventId).
    """
    tenant_id = event.get("tenantId")
    if not tenant_id:
        return None, {}

    event_type = event.get("eventType", "")
    payload = event.get("payload") or {}

    latency_ms = _safe_float(payload.get("latencyMs"), 0.0)
    is_error = 1.0 if (event_type and event_type.startswith("error.")) else 0.0

    # Approximate payload size as a feature (large payloads can indicate
    # unusual activity such as bulk data exfiltration)
    try:
        import json as _json
        payload_size = float(len(_json.dumps(payload).encode()))
    except Exception:
        payload_size = 0.0

    features = [latency_ms, is_error, payload_size]

    metadata = {
        "tenantId": tenant_id,
        "eventId": event.get("eventId", ""),
        "eventType": event_type,
        "source": event.get("source", ""),
        "ingestedAt": event.get("ingestedAt", ""),
    }

    return features, metadata


def features_to_dict(features: List[float], names: List[str]) -> Dict[str, float]:
    """
    Zip a feature vector back into a human-readable dict.

    Used when embedding ``features`` in the prediction envelope so that
    consumers (Query Service, WebSocket Service) can display per-feature
    values without knowing the index layout.
    """
    return {name: round(value, 6) for name, value in zip(names, features)}


# ─────────────────────────────────────────────────────────────────────
#  Private helpers
# ─────────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float) -> float:
    """Return ``value`` cast to float, or ``default`` on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

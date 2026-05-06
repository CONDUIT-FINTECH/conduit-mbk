"""
Unit tests for src.pipeline.processor and src.api.health
"""

import pytest
from unittest.mock import MagicMock, patch
from src.pipeline.processor import EventProcessor
from src.api.health import build_app
from src.config import Config


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_config(dry_run=True) -> Config:
    """Create a config with dry_run=True so no Kafka connections are made."""
    import os
    os.environ["KAFKA_DRY_RUN"] = "true" if dry_run else "false"
    cfg = Config()
    del os.environ["KAFKA_DRY_RUN"]
    return cfg


def _metric_snapshot(
    tenant_id="tenant-test",
    error_rate=0.01,
    p95_latency=300.0,
    success_rate=0.99,
    throughput=100,
    avg_latency=200.0,
):
    return {
        "tenantId": tenant_id,
        "window": "60s",
        "sampleSize": throughput,
        "computedAt": "2024-01-01T00:00:00Z",
        "metrics": {
            "errorRate": error_rate,
            "p95LatencyMs": p95_latency,
            "successRate": success_rate,
            "throughput": throughput,
            "avgLatencyMs": avg_latency,
            "p50LatencyMs": 150.0,
            "p99LatencyMs": 450.0,
            "maxLatencyMs": 600.0,
            "errorCount": int(error_rate * throughput),
            "successCount": int((1 - error_rate) * throughput),
        },
    }


def _event_message(tenant_id="tenant-test", event_type="transaction.created"):
    return {
        "eventId": "evt-abc",
        "tenantId": tenant_id,
        "eventType": event_type,
        "source": "payment-service",
        "payload": {"latencyMs": 120.0, "amount": 50.0},
        "ingestedAt": "2024-01-01T00:00:00Z",
    }


# ─── EventProcessor ──────────────────────────────────────────────────

class TestEventProcessor:
    """Tests for EventProcessor.handle_message()."""

    def _make_processor(self):
        cfg = _make_config(dry_run=True)
        return EventProcessor(cfg)

    def test_metrics_snapshot_increments_messages_received(self):
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_metrics
        proc.handle_message(_metric_snapshot(), topic)
        assert proc.messages_received == 1

    def test_event_message_increments_messages_received(self):
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_events
        proc.handle_message(_event_message(), topic)
        assert proc.messages_received == 1

    def test_metrics_snapshot_increments_predictions_published(self):
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_metrics
        proc.handle_message(_metric_snapshot(), topic)
        assert proc.predictions_published == 1

    def test_event_message_does_not_publish_prediction(self):
        """Raw events update stats but do not trigger a prediction."""
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_events
        proc.handle_message(_event_message(), topic)
        assert proc.predictions_published == 0

    def test_unknown_topic_does_not_raise(self):
        proc = self._make_processor()
        proc.handle_message({"tenantId": "t1"}, "unknown.topic")

    def test_malformed_metrics_snapshot_does_not_raise(self):
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_metrics
        proc.handle_message({"tenantId": "t1"}, topic)  # missing "metrics" key
        assert proc.errors == 0  # no error counter increment (just skipped)

    def test_multiple_tenants_tracked_separately(self):
        proc = self._make_processor()
        topic = proc._cfg.kafka.topic_metrics
        proc.handle_message(_metric_snapshot("tenant-A"), topic)
        proc.handle_message(_metric_snapshot("tenant-B"), topic)
        assert proc._registry.tenant_count == 2

    def test_high_anomaly_score_triggers_for_degraded_metrics(self):
        """
        After warm-up on varied normal data, a severely degraded snapshot
        should score at least as high as a healthy one.

        IsolationForest requires varied training data to establish a useful
        decision boundary, so we feed random-but-normal samples before testing.
        """
        import os
        import random

        os.environ["KAFKA_DRY_RUN"] = "true"
        os.environ["ML_WARMUP_SAMPLES"] = "5"
        os.environ["ML_REFIT_EVERY"] = "5"
        cfg = Config()
        for k in ["KAFKA_DRY_RUN", "ML_WARMUP_SAMPLES", "ML_REFIT_EVERY"]:
            del os.environ[k]

        proc = EventProcessor(cfg)
        topic = cfg.kafka.topic_metrics

        # Train on varied normal data so IsolationForest can establish a baseline
        rng = random.Random(42)
        for _ in range(20):
            proc.handle_message(
                _metric_snapshot(
                    "T1",
                    error_rate=rng.uniform(0.005, 0.02),
                    p95_latency=rng.uniform(150, 400),
                    success_rate=rng.uniform(0.97, 0.995),
                    throughput=rng.randint(80, 130),
                    avg_latency=rng.uniform(100, 280),
                ),
                topic,
            )

        det = proc._registry._get_or_create("T1")
        normal_result = det.predict([0.01, 250.0, 0.99, 100.0, 180.0])
        # Extreme anomaly: 60% error rate, 20 s latency, 40% success
        anomalous_result = det.predict([0.60, 20_000.0, 0.40, 100.0, 15_000.0])

        assert anomalous_result.anomaly_score >= normal_result.anomaly_score

    def test_stats_dict_structure(self):
        proc = self._make_processor()
        stats = proc.stats
        assert "messagesReceived" in stats
        assert "predictionsPublished" in stats
        assert "errors" in stats
        assert "activeTenants" in stats

    def test_model_stats_empty_at_start(self):
        proc = self._make_processor()
        assert proc.model_stats == {}

    def test_flush_does_not_raise(self):
        proc = self._make_processor()
        proc.flush()  # dry-run: should be a no-op


# ─── Health endpoints ─────────────────────────────────────────────────

class TestHealthEndpoints:
    """Tests for the FastAPI health / management API."""

    def _make_client(self, is_ready=True):
        from fastapi.testclient import TestClient

        app = build_app(
            processor_stats_fn=lambda: {
                "messagesReceived": 42,
                "predictionsPublished": 10,
                "errors": 1,
                "activeTenants": 3,
            },
            model_stats_fn=lambda: {
                "tenant-A": {"totalSamples": 100, "isWarmedUp": True, "windowSize": 100},
            },
            is_ready_fn=lambda: is_ready,
        )
        return TestClient(app)

    def test_health_returns_200(self):
        client = self._make_client()
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_contains_service_name(self):
        client = self._make_client()
        body = client.get("/health").json()
        assert body["service"] == "ml-service"

    def test_health_contains_pipeline_stats(self):
        client = self._make_client()
        body = client.get("/health").json()
        assert body["pipeline"]["messagesReceived"] == 42

    def test_ready_returns_200_when_ready(self):
        client = self._make_client(is_ready=True)
        resp = client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

    def test_ready_returns_503_when_not_ready(self):
        client = self._make_client(is_ready=False)
        resp = client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "not_ready"

    def test_metrics_returns_200(self):
        client = self._make_client()
        resp = client.get("/metrics")
        assert resp.status_code == 200
        body = resp.json()
        assert body["messagesReceived"] == 42

    def test_models_returns_tenant_stats(self):
        client = self._make_client()
        resp = client.get("/models")
        assert resp.status_code == 200
        body = resp.json()
        assert "tenant-A" in body["tenants"]
        assert body["tenantCount"] == 1

    def test_docs_endpoint_available(self):
        """OpenAPI docs should be reachable (confirms FastAPI config is correct)."""
        client = self._make_client()
        resp = client.get("/docs")
        assert resp.status_code == 200

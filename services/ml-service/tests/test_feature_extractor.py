"""
Unit tests for src.models.feature_extractor
"""

import pytest
from src.models.feature_extractor import (
    METRIC_FEATURE_NAMES,
    EVENT_FEATURE_NAMES,
    METRIC_IDX_ERROR_RATE,
    METRIC_IDX_P95_LATENCY,
    METRIC_IDX_SUCCESS_RATE,
    METRIC_IDX_THROUGHPUT,
    METRIC_IDX_AVG_LATENCY,
    extract_metric_features,
    extract_event_features,
    features_to_dict,
)


# ─── extract_metric_features ─────────────────────────────────────────

class TestExtractMetricFeatures:
    """Tests for extract_metric_features()."""

    def _make_snapshot(self, **overrides):
        """Return a valid metrics.computed message with sane defaults."""
        metrics = {
            "errorRate": 0.02,
            "p95LatencyMs": 350.0,
            "successRate": 0.98,
            "throughput": 100,
            "avgLatencyMs": 210.0,
            "p50LatencyMs": 190.0,
            "p99LatencyMs": 480.0,
            "maxLatencyMs": 620.0,
            "errorCount": 2,
            "successCount": 98,
        }
        metrics.update(overrides)
        return {
            "tenantId": "tenant-test",
            "window": "60s",
            "sampleSize": 100,
            "computedAt": "2024-01-01T00:00:00Z",
            "metrics": metrics,
        }

    def test_normal_snapshot_returns_five_features(self):
        features, meta = extract_metric_features(self._make_snapshot())
        assert features is not None
        assert len(features) == 5

    def test_feature_values_match_input(self):
        snap = self._make_snapshot(errorRate=0.05, p95LatencyMs=1200.0,
                                    successRate=0.95, throughput=200,
                                    avgLatencyMs=300.0)
        features, _ = extract_metric_features(snap)
        assert features[METRIC_IDX_ERROR_RATE] == pytest.approx(0.05)
        assert features[METRIC_IDX_P95_LATENCY] == pytest.approx(1200.0)
        assert features[METRIC_IDX_SUCCESS_RATE] == pytest.approx(0.95)
        assert features[METRIC_IDX_THROUGHPUT] == pytest.approx(200.0)
        assert features[METRIC_IDX_AVG_LATENCY] == pytest.approx(300.0)

    def test_metadata_contains_tenant_id(self):
        _, meta = extract_metric_features(self._make_snapshot())
        assert meta["tenantId"] == "tenant-test"

    def test_missing_metrics_key_returns_none(self):
        features, meta = extract_metric_features({"tenantId": "t1"})
        assert features is None
        assert meta == {}

    def test_metrics_key_not_dict_returns_none(self):
        features, _ = extract_metric_features({"tenantId": "t1", "metrics": "bad"})
        assert features is None

    def test_partial_metrics_use_defaults(self):
        """Missing metric values should fall back to safe defaults (0 or 1)."""
        features, _ = extract_metric_features({
            "tenantId": "t1",
            "metrics": {},  # all fields missing
        })
        assert features is not None
        assert features[METRIC_IDX_ERROR_RATE] == pytest.approx(0.0)
        assert features[METRIC_IDX_SUCCESS_RATE] == pytest.approx(1.0)

    def test_non_numeric_values_use_defaults(self):
        features, _ = extract_metric_features({
            "tenantId": "t1",
            "metrics": {"errorRate": "bad", "throughput": None},
        })
        assert features is not None
        assert features[METRIC_IDX_ERROR_RATE] == pytest.approx(0.0)
        assert features[METRIC_IDX_THROUGHPUT] == pytest.approx(0.0)

    def test_empty_snapshot_returns_none(self):
        features, _ = extract_metric_features({})
        assert features is None


# ─── extract_event_features ──────────────────────────────────────────

class TestExtractEventFeatures:
    """Tests for extract_event_features()."""

    def _make_event(self, event_type="transaction.created", latency_ms=150.0):
        return {
            "eventId": "evt-123",
            "tenantId": "tenant-A",
            "eventType": event_type,
            "source": "payment-service",
            "payload": {"latencyMs": latency_ms, "amount": 99.99},
            "ingestedAt": "2024-01-01T00:00:00Z",
        }

    def test_normal_event_returns_three_features(self):
        features, _ = extract_event_features(self._make_event())
        assert features is not None
        assert len(features) == 3

    def test_is_error_false_for_non_error_event(self):
        features, _ = extract_event_features(self._make_event("transaction.created"))
        assert features[1] == pytest.approx(0.0)

    def test_is_error_true_for_error_event(self):
        features, _ = extract_event_features(self._make_event("error.payment_failed"))
        assert features[1] == pytest.approx(1.0)

    def test_latency_extracted_from_payload(self):
        features, _ = extract_event_features(self._make_event(latency_ms=300.0))
        assert features[0] == pytest.approx(300.0)

    def test_missing_latency_defaults_to_zero(self):
        event = self._make_event()
        del event["payload"]["latencyMs"]
        features, _ = extract_event_features(event)
        assert features[0] == pytest.approx(0.0)

    def test_payload_size_is_positive(self):
        features, _ = extract_event_features(self._make_event())
        assert features[2] > 0.0

    def test_missing_tenant_id_returns_none(self):
        event = self._make_event()
        del event["tenantId"]
        features, _ = extract_event_features(event)
        assert features is None

    def test_metadata_contains_event_id(self):
        _, meta = extract_event_features(self._make_event())
        assert meta["eventId"] == "evt-123"
        assert meta["tenantId"] == "tenant-A"


# ─── features_to_dict ────────────────────────────────────────────────

class TestFeaturesToDict:
    def test_correct_mapping(self):
        names = ["a", "b", "c"]
        features = [1.0, 2.5, 3.333333]
        result = features_to_dict(features, names)
        assert result == {"a": 1.0, "b": 2.5, "c": 3.333333}

    def test_values_rounded_to_six_decimals(self):
        result = features_to_dict([1.123456789], ["x"])
        assert result["x"] == pytest.approx(1.123457, abs=1e-6)

    def test_empty_inputs(self):
        assert features_to_dict([], []) == {}

    def test_metric_feature_names_count(self):
        assert len(METRIC_FEATURE_NAMES) == 5

    def test_event_feature_names_count(self):
        assert len(EVENT_FEATURE_NAMES) == 3

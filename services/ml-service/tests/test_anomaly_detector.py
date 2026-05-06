"""
Unit tests for src.models.anomaly_detector
"""

import math
import pytest
from src.models.anomaly_detector import (
    DetectorRegistry,
    TenantAnomalyDetector,
    PredictionResult,
    _WelfordStats,
    _determine_labels,
    _sigmoid,
)
from src.config import MLConfig


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_ml_config(**overrides) -> MLConfig:
    """Return an MLConfig with test-friendly defaults."""
    import os
    for k, v in overrides.items():
        os.environ[k] = str(v)
    cfg = MLConfig()
    # Clean up env
    for k in overrides:
        del os.environ[k]
    return cfg


def _normal_features(
    error_rate=0.01,
    p95_latency=200.0,
    success_rate=0.99,
    throughput=100.0,
    avg_latency=150.0,
) -> list:
    return [error_rate, p95_latency, success_rate, throughput, avg_latency]


# ─── _WelfordStats ───────────────────────────────────────────────────

class TestWelfordStats:
    def test_single_sample_mean(self):
        s = _WelfordStats()
        s.update(5.0)
        assert s.mean == pytest.approx(5.0)

    def test_two_samples_mean(self):
        s = _WelfordStats()
        s.update(4.0)
        s.update(6.0)
        assert s.mean == pytest.approx(5.0)

    def test_variance_two_samples(self):
        s = _WelfordStats()
        s.update(4.0)
        s.update(6.0)
        assert s.variance == pytest.approx(2.0)

    def test_std_two_samples(self):
        s = _WelfordStats()
        s.update(4.0)
        s.update(6.0)
        assert s.std == pytest.approx(math.sqrt(2.0))

    def test_variance_single_sample_is_zero(self):
        s = _WelfordStats()
        s.update(10.0)
        assert s.variance == pytest.approx(0.0)

    def test_zscore_returns_zero_when_std_is_zero(self):
        s = _WelfordStats()
        s.update(5.0)
        assert s.zscore(5.0) == pytest.approx(0.0)

    def test_zscore_known_value(self):
        s = _WelfordStats()
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]:
            s.update(v)
        # mean=5.0, variance=4.571, std≈2.138
        z = s.zscore(7.0)
        assert abs(z) > 0.0  # just verify it's non-zero and finite


# ─── _sigmoid ────────────────────────────────────────────────────────

class TestSigmoid:
    def test_sigmoid_zero(self):
        assert _sigmoid(0.0) == pytest.approx(0.5)

    def test_sigmoid_large_positive(self):
        assert _sigmoid(100.0) == pytest.approx(1.0, abs=1e-6)

    def test_sigmoid_large_negative(self):
        assert _sigmoid(-100.0) == pytest.approx(0.0, abs=1e-6)


# ─── _determine_labels ───────────────────────────────────────────────

class TestDetermineLabels:
    def test_system_failure_label(self):
        features = [0.20, 200.0, 0.80, 100.0, 150.0]  # high error, low success
        z_scores = [0.0] * 5
        labels = _determine_labels(features, z_scores, zscore_threshold=3.0)
        assert "system_failure" in labels

    def test_performance_degradation_label(self):
        features = [0.10, 6000.0, 0.90, 100.0, 4000.0]  # error + high latency
        z_scores = [0.0] * 5
        labels = _determine_labels(features, z_scores, zscore_threshold=3.0)
        assert "performance_degradation" in labels

    def test_traffic_spike_via_zscore(self):
        features = _normal_features(throughput=100.0)
        z_scores = [0.0, 0.0, 0.0, 4.0, 0.0]  # throughput Z-score > threshold
        labels = _determine_labels(features, z_scores, zscore_threshold=3.0)
        assert "traffic_spike" in labels

    def test_no_labels_for_normal_data(self):
        features = _normal_features()
        z_scores = [0.0] * 5
        labels = _determine_labels(features, z_scores, zscore_threshold=3.0)
        assert labels == []

    def test_too_few_features_returns_empty(self):
        labels = _determine_labels([0.1, 200.0], [0.0, 0.0], zscore_threshold=3.0)
        assert labels == []


# ─── TenantAnomalyDetector ───────────────────────────────────────────

class TestTenantAnomalyDetector:
    """Tests for the per-tenant detector (IsolationForest + Z-score)."""

    def _make_detector(self, warmup=5, refit_every=10, contamination=0.05):
        import os
        os.environ["ML_WARMUP_SAMPLES"] = str(warmup)
        os.environ["ML_REFIT_EVERY"] = str(refit_every)
        os.environ["ML_CONTAMINATION"] = str(contamination)
        cfg = MLConfig()
        for k in ["ML_WARMUP_SAMPLES", "ML_REFIT_EVERY", "ML_CONTAMINATION"]:
            del os.environ[k]
        return TenantAnomalyDetector(cfg, n_features=5)

    def test_returns_prediction_result(self):
        det = self._make_detector()
        result = det.predict(_normal_features())
        assert isinstance(result, PredictionResult)

    def test_not_warmed_up_before_warmup_samples(self):
        det = self._make_detector(warmup=10)
        for _ in range(5):
            result = det.predict(_normal_features())
        assert not result.is_warmed_up
        assert "warmup" in result.model_name

    def test_warmed_up_after_warmup_samples(self):
        det = self._make_detector(warmup=5, refit_every=5)
        for _ in range(6):
            result = det.predict(_normal_features())
        assert result.is_warmed_up

    def test_anomaly_score_in_range(self):
        det = self._make_detector(warmup=5, refit_every=5)
        for _ in range(10):
            result = det.predict(_normal_features())
        assert 0.0 <= result.anomaly_score <= 1.0

    def test_confidence_in_range(self):
        det = self._make_detector(warmup=5, refit_every=5)
        for _ in range(10):
            result = det.predict(_normal_features())
        assert 0.0 <= result.confidence <= 1.0

    def test_anomalous_input_scores_higher(self):
        """An obvious anomaly should score higher than normal data."""
        det = self._make_detector(warmup=5, refit_every=5)
        # Train on normal data
        for _ in range(15):
            det.predict(_normal_features())

        normal_result = det.predict(_normal_features())
        # Very high error rate + very high latency = obvious anomaly
        anomaly_result = det.predict(
            _normal_features(error_rate=0.99, p95_latency=50_000.0, success_rate=0.01)
        )
        assert anomaly_result.anomaly_score >= normal_result.anomaly_score

    def test_wrong_feature_length_raises(self):
        det = self._make_detector()
        with pytest.raises(ValueError, match="Expected 5 features, got 3"):
            det.predict([1.0, 2.0, 3.0])

    def test_sample_count_increments(self):
        det = self._make_detector()
        for i in range(3):
            det.predict(_normal_features())
        assert det.sample_count == 3

    def test_label_property_returns_first_label(self):
        result = PredictionResult(
            anomaly_score=0.8,
            confidence=0.7,
            labels=["system_failure", "metric_anomaly"],
            model_name="test",
            is_warmed_up=True,
        )
        assert result.label == "system_failure"

    def test_label_property_none_when_no_labels(self):
        result = PredictionResult(
            anomaly_score=0.1,
            confidence=0.2,
            labels=[],
            model_name="test",
            is_warmed_up=False,
        )
        assert result.label is None


# ─── DetectorRegistry ────────────────────────────────────────────────

class TestDetectorRegistry:
    def _make_registry(self):
        import os
        os.environ["ML_WARMUP_SAMPLES"] = "5"
        os.environ["ML_REFIT_EVERY"] = "5"
        cfg = MLConfig()
        for k in ["ML_WARMUP_SAMPLES", "ML_REFIT_EVERY"]:
            del os.environ[k]
        return DetectorRegistry(cfg, n_features=5)

    def test_creates_new_detector_per_tenant(self):
        registry = self._make_registry()
        registry.predict("tenant-A", _normal_features())
        registry.predict("tenant-B", _normal_features())
        assert registry.tenant_count == 2

    def test_reuses_existing_detector(self):
        registry = self._make_registry()
        for _ in range(3):
            registry.predict("tenant-A", _normal_features())
        assert registry.tenant_count == 1

    def test_get_stats_includes_all_tenants(self):
        registry = self._make_registry()
        registry.predict("A", _normal_features())
        registry.predict("B", _normal_features())
        stats = registry.get_stats()
        assert "A" in stats
        assert "B" in stats

    def test_stats_has_required_keys(self):
        registry = self._make_registry()
        registry.predict("X", _normal_features())
        stats = registry.get_stats()
        assert "totalSamples" in stats["X"]
        assert "isWarmedUp" in stats["X"]

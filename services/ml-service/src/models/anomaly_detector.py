"""
Per-Tenant Anomaly Detector
=============================
Provides an online, per-tenant anomaly detection pipeline that combines:

1. **Welford online statistics** — tracks a running mean and variance for
   each feature without storing all historical values.  Used for Z-score
   computation and label assignment.

2. **IsolationForest** (scikit-learn) — tree-ensemble model that assigns
   an anomaly score in [0, 1] once enough training samples have been
   collected.  The model is periodically re-fit on the latest sliding
   window of samples.

3. **Rule-based labelling** — maps raw feature values and Z-scores to
   human-readable anomaly labels that are compatible with the Node.js
   ``LABEL_SEVERITY_MAP`` in ``mlDetector.js``.

Design decisions
----------------
* **Per-tenant isolation**: each tenant has its own detector instance,
  so a traffic spike at tenant A does not pollute tenant B's baseline.
* **Warm-up phase**: during the first ``warmup_samples`` observations
  only Z-score detection is used.  This prevents false positives while
  the model is learning the normal distribution.
* **No external storage**: all state is in-memory.  Models survive as long
  as the process does.  A restart will restart the warm-up period (which
  is acceptable given that only aggregated metric snapshots are used as
  input, not high-frequency raw events).

Usage::

    from src.models.anomaly_detector import DetectorRegistry
    from src.config import config

    registry = DetectorRegistry(config.ml)

    # Feed a feature vector (list of floats) for a tenant:
    result = registry.predict("tenant-abc", features=[0.02, 450.0, 0.98, 120.0, 310.0])

    print(result.anomaly_score)   # float [0, 1]
    print(result.labels)          # e.g. ["latency_drift"]
    print(result.confidence)      # float [0, 1]
    print(result.model_name)      # "isolation-forest-v1" or "zscore-warmup-v1"
    print(result.is_warmed_up)    # bool
"""

from __future__ import annotations

import collections
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
from sklearn.ensemble import IsolationForest

from src.config import MLConfig

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
#  Result dataclass
# ─────────────────────────────────────────────────────────────────────


@dataclass
class PredictionResult:
    """
    Encapsulates a single anomaly prediction for one tenant snapshot.

    Attributes
    ----------
    anomaly_score:
        Float in [0, 1].  Values ≥ 0.75 trigger incident creation in
        the Node.js Incident Service (``ML_ANOMALY_THRESHOLD``).
    confidence:
        Float in [0, 1].  Indicates how certain the model is.  During
        warm-up this will be lower because Z-score alone is used.
    labels:
        List of human-readable anomaly labels.  The first element
        (``label``) is also included separately for compatibility with
        ``mlDetector.js``.
    model_name:
        Identifier embedded in the Kafka prediction message.
    is_warmed_up:
        True once the IsolationForest has been trained at least once.
    z_scores:
        Per-feature Z-scores (same index order as the input feature
        vector).  Included for diagnostics.
    """

    anomaly_score: float
    confidence: float
    labels: List[str]
    model_name: str
    is_warmed_up: bool
    z_scores: List[float] = field(default_factory=list)

    @property
    def label(self) -> Optional[str]:
        """Primary label (first element) or None."""
        return self.labels[0] if self.labels else None


# ─────────────────────────────────────────────────────────────────────
#  Online mean / variance  (Welford's algorithm)
# ─────────────────────────────────────────────────────────────────────


class _WelfordStats:
    """
    Maintains a running mean and sample variance using Welford's
    one-pass algorithm.  O(1) space, O(1) per update.
    """

    __slots__ = ("n", "_mean", "_m2")

    def __init__(self) -> None:
        self.n: int = 0
        self._mean: float = 0.0
        self._m2: float = 0.0

    def update(self, x: float) -> None:
        self.n += 1
        delta = x - self._mean
        self._mean += delta / self.n
        self._m2 += delta * (x - self._mean)

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        return self._m2 / (self.n - 1) if self.n >= 2 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def zscore(self, x: float) -> float:
        """Return the Z-score of *x* given the current mean / std."""
        std = self.std
        if std < 1e-10:
            return 0.0
        return (x - self._mean) / std


# ─────────────────────────────────────────────────────────────────────
#  Per-tenant detector
# ─────────────────────────────────────────────────────────────────────


class TenantAnomalyDetector:
    """
    Stateful, online anomaly detector for a single tenant.

    Parameters
    ----------
    ml_cfg:
        The :class:`~src.config.MLConfig` singleton.
    n_features:
        Length of the feature vectors this detector will receive.
    """

    def __init__(self, ml_cfg: MLConfig, n_features: int) -> None:
        self._cfg = ml_cfg
        self._n_features = n_features

        # ── Online stats (one _WelfordStats per feature) ─────────────
        self._stats: List[_WelfordStats] = [_WelfordStats() for _ in range(n_features)]

        # ── Sliding window of raw feature vectors ────────────────────
        # maxlen prevents unbounded memory growth
        self._window: collections.deque = collections.deque(
            maxlen=ml_cfg.max_samples_per_tenant
        )

        # ── IsolationForest model ─────────────────────────────────────
        self._model: Optional[IsolationForest] = None
        self._samples_since_refit: int = 0

        # ── Counters ─────────────────────────────────────────────────
        self.total_samples: int = 0
        self.total_anomalies: int = 0

    # ──────────────────────────────────────────────────────────────────
    #  Core API
    # ──────────────────────────────────────────────────────────────────

    def predict(self, features: List[float]) -> PredictionResult:
        """
        Feed a feature vector, update internal state, and return a
        :class:`PredictionResult`.

        Parameters
        ----------
        features:
            Numeric feature vector produced by
            :func:`~src.models.feature_extractor.extract_metric_features`.
            Must have length equal to ``n_features``.
        """
        if len(features) != self._n_features:
            raise ValueError(
                f"Expected {self._n_features} features, got {len(features)}"
            )

        # 1. Update Welford stats + sliding window
        self._update_stats(features)
        self.total_samples += 1
        self._samples_since_refit += 1

        # 2. Compute Z-scores (always available)
        z_scores = [s.zscore(v) for s, v in zip(self._stats, features)]

        # 3. Determine model phase
        is_warmed_up = self._model is not None
        should_fit = (
            self.total_samples >= self._cfg.warmup_samples
            and self._samples_since_refit >= self._cfg.refit_every
        )

        if should_fit:
            self._refit_model()

        # 4. Score
        if is_warmed_up:
            anomaly_score, confidence = self._score_with_if(features, z_scores)
            model_name = self._cfg.model_id
        else:
            anomaly_score, confidence = self._score_with_zscore(z_scores)
            model_name = "zscore-warmup-v1"

        # 5. Assign labels
        labels = _determine_labels(features, z_scores, self._cfg.zscore_threshold)

        if anomaly_score >= 0.5 and not labels:
            labels = ["metric_anomaly"]

        if anomaly_score >= 0.75:
            self.total_anomalies += 1

        return PredictionResult(
            anomaly_score=anomaly_score,
            confidence=confidence,
            labels=labels,
            model_name=model_name,
            is_warmed_up=is_warmed_up,
            z_scores=z_scores,
        )

    @property
    def is_warmed_up(self) -> bool:
        return self._model is not None

    @property
    def sample_count(self) -> int:
        return self.total_samples

    # ──────────────────────────────────────────────────────────────────
    #  Private helpers
    # ──────────────────────────────────────────────────────────────────

    def _update_stats(self, features: List[float]) -> None:
        """Update Welford stats and append to sliding window."""
        for stat, value in zip(self._stats, features):
            stat.update(value)
        self._window.append(features)

    def _refit_model(self) -> None:
        """
        (Re-)fit the IsolationForest on the current sliding window.

        Called when we first reach ``warmup_samples`` and periodically
        every ``refit_every`` samples thereafter.
        """
        X = np.array(list(self._window), dtype=float)
        model = IsolationForest(
            contamination=self._cfg.contamination,
            random_state=42,
            n_estimators=100,
        )
        try:
            model.fit(X)
            self._model = model
            self._samples_since_refit = 0
            logger.debug(
                "[Detector] Model refit on %d samples (total=%d)",
                len(X),
                self.total_samples,
            )
        except Exception as exc:
            logger.warning("[Detector] Model refit failed: %s", exc)

    def _score_with_if(
        self, features: List[float], z_scores: List[float]
    ) -> tuple[float, float]:
        """
        Score using the fitted IsolationForest.

        Returns (anomaly_score, confidence) both in [0, 1].
        """
        X = np.array([features], dtype=float)

        # decision_function: negative → anomaly, positive → normal
        df = float(self._model.decision_function(X)[0])  # type: ignore[union-attr]

        # Sigmoid mapping → [0, 1].  Scale factor 8 gives a crisp
        # separation around the decision boundary while keeping scores
        # well-calibrated for the [0.75, 1.0] incident-trigger range.
        anomaly_score = _sigmoid(-df * 8)
        anomaly_score = float(np.clip(anomaly_score, 0.0, 1.0))

        # Confidence: how far from the decision boundary (|df| normalised)
        confidence = float(np.clip(abs(df) * 4, 0.0, 1.0))

        # Blend with Z-score for extra sensitivity to sudden spikes
        z_score_anomaly, _ = self._score_with_zscore(z_scores)
        anomaly_score = float(max(anomaly_score, z_score_anomaly * 0.7))

        return anomaly_score, confidence

    def _score_with_zscore(
        self, z_scores: List[float]
    ) -> tuple[float, float]:
        """
        Score using the maximum absolute Z-score across all features.

        Returns (anomaly_score, confidence) both in [0, 1].

        The mapping normalises z → [0, 1] such that:
          z = 0  → score = 0.0  (totally normal)
          z = 3  → score ≈ 0.75 (approaching incident threshold)
          z = 5  → score ≈ 0.92 (strong anomaly signal)
        """
        if not z_scores or all(z == 0.0 for z in z_scores):
            return 0.0, 0.0

        max_z = max(abs(z) for z in z_scores)
        # Smooth normalisation: score grows quickly above threshold
        anomaly_score = float(np.clip(max_z / (max_z + 3.0), 0.0, 1.0))
        confidence = float(np.clip(max_z / 5.0, 0.0, 1.0)) * 0.6  # lower in warm-up

        return anomaly_score, confidence


# ─────────────────────────────────────────────────────────────────────
#  Label determination  (rule-based, compatible with mlDetector.js)
# ─────────────────────────────────────────────────────────────────────

# Matches keys in LABEL_SEVERITY_MAP from mlDetector.js
_LABEL_SYSTEM_FAILURE = "system_failure"
_LABEL_PERF_DEGRADATION = "performance_degradation"
_LABEL_TRAFFIC_SPIKE = "traffic_spike"
_LABEL_LATENCY_DRIFT = "latency_drift"
_LABEL_METRIC_ANOMALY = "metric_anomaly"


def _determine_labels(
    features: List[float],
    z_scores: List[float],
    zscore_threshold: float,
) -> List[str]:
    """
    Assign human-readable labels based on raw feature values and Z-scores.

    The label set is ordered from most severe to least severe so that
    ``PredictionResult.label`` (the first element) carries the highest-
    priority signal.

    Feature vector layout (metric snapshot):
        [error_rate, p95_latency_ms, success_rate, throughput, avg_latency_ms]
    """
    labels: List[str] = []

    if len(features) < 5 or len(z_scores) < 5:
        return labels

    error_rate, p95_latency, success_rate, throughput, avg_latency = features
    z_er, z_p95, z_sr, z_tp, z_avg = z_scores

    # ── Highest severity ─────────────────────────────────────────────
    if error_rate > 0.15 and success_rate < 0.85:
        labels.append(_LABEL_SYSTEM_FAILURE)

    # ── High severity ────────────────────────────────────────────────
    elif error_rate > 0.05 and p95_latency > 5_000:
        labels.append(_LABEL_PERF_DEGRADATION)

    # ── Statistical anomalies ─────────────────────────────────────────
    if abs(z_tp) > zscore_threshold:
        labels.append(_LABEL_TRAFFIC_SPIKE)

    if abs(z_p95) > zscore_threshold * 0.9 and abs(z_avg) > zscore_threshold * 0.7:
        if _LABEL_PERF_DEGRADATION not in labels:
            labels.append(_LABEL_LATENCY_DRIFT)

    if not labels and max((abs(z) for z in z_scores), default=0) > zscore_threshold:
        labels.append(_LABEL_METRIC_ANOMALY)

    return labels


# ─────────────────────────────────────────────────────────────────────
#  Registry  (maps tenantId → TenantAnomalyDetector)
# ─────────────────────────────────────────────────────────────────────


class DetectorRegistry:
    """
    Thread-safe registry of :class:`TenantAnomalyDetector` instances.

    A new detector is created on first access for each tenant; existing
    detectors are reused on subsequent calls.

    Parameters
    ----------
    ml_cfg:
        Shared :class:`~src.config.MLConfig` configuration.
    n_features:
        Length of the feature vectors that will be fed to the detectors.
        Defaults to 5 (metrics snapshot).
    """

    def __init__(self, ml_cfg: MLConfig, n_features: int = 5) -> None:
        self._cfg = ml_cfg
        self._n_features = n_features
        self._detectors: Dict[str, TenantAnomalyDetector] = {}

    def predict(
        self, tenant_id: str, features: List[float]
    ) -> PredictionResult:
        """
        Route a feature vector to the appropriate per-tenant detector and
        return a :class:`PredictionResult`.
        """
        detector = self._get_or_create(tenant_id)
        return detector.predict(features)

    def get_stats(self) -> Dict[str, Dict]:
        """
        Return diagnostic stats for every known tenant.

        Included in the ``GET /models`` API response.
        """
        stats = {}
        for tenant_id, det in self._detectors.items():
            stats[tenant_id] = {
                "totalSamples": det.total_samples,
                "totalAnomalies": det.total_anomalies,
                "isWarmedUp": det.is_warmed_up,
                "windowSize": len(det._window),
            }
        return stats

    @property
    def tenant_count(self) -> int:
        return len(self._detectors)

    def _get_or_create(self, tenant_id: str) -> TenantAnomalyDetector:
        if tenant_id not in self._detectors:
            self._detectors[tenant_id] = TenantAnomalyDetector(
                self._cfg, self._n_features
            )
            logger.info("[DetectorRegistry] New detector created for tenant=%s", tenant_id)
        return self._detectors[tenant_id]


# ─────────────────────────────────────────────────────────────────────
#  Utilities
# ─────────────────────────────────────────────────────────────────────

def _sigmoid(x: float) -> float:
    """Numerically stable sigmoid function."""
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    ex = math.exp(x)
    return ex / (1.0 + ex)

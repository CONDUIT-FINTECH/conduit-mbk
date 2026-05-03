const { createIncident } = require('../state/incidentStateMachine');

/**
 * ═══════════════════════════════════════════════════
 *  ML Anomaly Detector
 * ═══════════════════════════════════════════════════
 * 
 * Evaluates ML prediction events from conduit.ml.predictions.
 * 
 * ML predictions contain anomaly scores (0.0–1.0) and
 * optional labels from the upstream ML pipeline (LSTMWatch, etc.)
 * 
 * Detects:
 *   - High anomaly scores exceeding confidence threshold
 *   - Specific anomaly labels (drift, spike, degradation)
 */

const ANOMALY_THRESHOLD = parseFloat(process.env.ML_ANOMALY_THRESHOLD || '0.75');

const SEVERITY_BY_SCORE = [
  { min: 0.95, severity: 'critical' },
  { min: 0.85, severity: 'high' },
  { min: 0.75, severity: 'medium' },
  { min: 0.60, severity: 'low' },
];

const LABEL_SEVERITY_MAP = {
  'system_failure':       'critical',
  'cascading_failure':    'critical',
  'performance_degradation': 'high',
  'traffic_spike':        'high',
  'latency_drift':        'medium',
  'metric_anomaly':       'medium',
};

/**
 * Evaluate an ML prediction against anomaly thresholds.
 * @param {object} prediction - ML prediction event
 * @returns {object|null} - Incident object or null
 */
function evaluateMLPrediction(prediction) {
  const {
    tenantId,
    anomalyScore,
    label,
    modelId,
    source,
    confidence,
    features,
  } = prediction;

  // Skip low-confidence predictions
  if (anomalyScore === undefined || anomalyScore === null) return null;
  if (anomalyScore < ANOMALY_THRESHOLD) return null;

  // Determine severity from label (if known) or score
  const severity = LABEL_SEVERITY_MAP[label] || classifyByScore(anomalyScore);

  return createIncident({
    type:         'ml_anomaly',
    severity,
    source:       source || 'ml-pipeline',
    tenantId,
    detectorType: 'ml_anomaly',
    description:  `ML anomaly detected: score=${anomalyScore.toFixed(3)}${label ? ` label=${label}` : ''} (model: ${modelId || 'unknown'})`,
    triggerData: {
      anomalyScore,
      label:      label || null,
      modelId:    modelId || null,
      confidence: confidence || null,
      features:   features || null,
      threshold:  ANOMALY_THRESHOLD,
    },
  });
}

function classifyByScore(score) {
  for (const tier of SEVERITY_BY_SCORE) {
    if (score >= tier.min) return tier.severity;
  }
  return 'low';
}

module.exports = { evaluateMLPrediction };

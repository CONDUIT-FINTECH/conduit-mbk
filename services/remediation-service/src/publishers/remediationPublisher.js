const { kafka } = require('@conduit/shared');
const { publish, TOPICS } = kafka;

/**
 * ═══════════════════════════════════════════════════
 *  Remediation Event Publisher
 * ═══════════════════════════════════════════════════
 *
 * Publishes remediation lifecycle events to conduit.remediations.
 *
 * Event types:
 *   - remediation.started   → Execution began
 *   - remediation.succeeded → Fix applied successfully
 *   - remediation.failed    → All retries exhausted
 *   - remediation.retrying  → Retrying after failure
 */

const STATUS_EVENT_MAP = {
  executing:    'remediation.started',
  success:      'remediation.succeeded',
  failed:       'remediation.failed',
  rolling_back: 'remediation.rolling_back',
  rolled_back:  'remediation.rolled_back',
};

/**
 * Publish a remediation lifecycle event.
 */
async function publishRemediationEvent(remediation, eventType = null) {
  const type = eventType || STATUS_EVENT_MAP[remediation.status] || `remediation.${remediation.status}`;

  const event = {
    eventType:      type,
    remediationId:  remediation.remediationId,
    incidentId:     remediation.incidentId,
    tenantId:       remediation.tenantId,
    action:         remediation.action,
    status:         remediation.status,
    attempts:       remediation.attempts,
    maxRetries:     remediation.maxRetries,
    error:          remediation.error,
    details:        remediation.details,
    createdAt:      remediation.createdAt,
    startedAt:      remediation.startedAt,
    completedAt:    remediation.completedAt,
    publishedAt:    new Date().toISOString(),
  };

  await publish(TOPICS.REMEDIATIONS, remediation.tenantId, event);
  console.log(`[Remediation Publisher] ${type} → ${remediation.remediationId}`);

  return event;
}

module.exports = { publishRemediationEvent };

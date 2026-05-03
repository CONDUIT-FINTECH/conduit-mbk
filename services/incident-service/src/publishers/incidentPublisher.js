const { kafka } = require('@conduit/shared');
const { publish, TOPICS } = kafka;

/**
 * ═══════════════════════════════════════════════════
 *  Incident Event Publisher
 * ═══════════════════════════════════════════════════
 * 
 * Publishes incident lifecycle events to conduit.incidents.events.
 * 
 * Event types:
 *   - incident.detected  → New incident created
 *   - incident.activated → Incident acknowledged / escalated
 *   - incident.resolved  → Incident closed
 */

const EVENT_TYPE_MAP = {
  detected: 'incident.detected',
  active:   'incident.activated',
  resolved: 'incident.resolved',
};

/**
 * Publish an incident lifecycle event to Kafka.
 * @param {object} incident - The incident object (after state transition)
 * @param {string} [previousStatus] - The status before the transition (for context)
 */
async function publishIncidentEvent(incident, previousStatus = null) {
  const eventType = EVENT_TYPE_MAP[incident.status] || `incident.${incident.status}`;

  const event = {
    eventType,
    incidentId:   incident.incidentId,
    tenantId:     incident.tenantId,
    status:       incident.status,
    severity:     incident.severity,
    type:         incident.type,
    source:       incident.source,
    detectorType: incident.detectorType,
    description:  incident.description,
    previousStatus,
    detectedAt:     incident.detectedAt,
    acknowledgedAt: incident.acknowledgedAt,
    resolvedAt:     incident.resolvedAt,
    duration:       incident.duration,
    publishedAt:    new Date().toISOString(),
  };

  await publish(TOPICS.INCIDENTS, incident.tenantId, event);
  console.log(`[Incident Publisher] ${eventType} → ${incident.incidentId} (${incident.severity})`);

  return event;
}

module.exports = { publishIncidentEvent };

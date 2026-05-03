const { Router } = require('express');
const { VALID_TRANSITIONS } = require('../state/incidentStateMachine');
const { getIncident, transitionIncident, listIncidents, getStatusCounts } = require('../state/incidentStore');
const { publishIncidentEvent } = require('../publishers/incidentPublisher');

const router = Router();

/**
 * GET /incidents
 * List incidents with optional filters.
 */
router.get('/', (req, res) => {
  const { status, severity, tenantId, limit = 50, offset = 0 } = req.query;

  const result = listIncidents({
    status,
    severity,
    tenantId,
    limit: Math.min(parseInt(limit, 10), 100),
    offset: parseInt(offset, 10) || 0,
  });

  res.json(result);
});

/**
 * GET /incidents/counts
 * Dashboard endpoint — returns counts by status.
 */
router.get('/counts', (req, res) => {
  res.json(getStatusCounts());
});

/**
 * GET /incidents/:id
 * Get a single incident by ID.
 */
router.get('/:id', (req, res) => {
  const incident = getIncident(req.params.id);
  if (!incident) {
    return res.status(404).json({ error: 'NOT_FOUND', message: `Incident ${req.params.id} not found` });
  }
  res.json(incident);
});

/**
 * PATCH /incidents/:id/transition
 * Transition an incident's status using the state machine.
 * 
 * Body: { "status": "active" | "resolved" }
 * 
 * Publishes the transition event to conduit.incidents.events.
 */
router.patch('/:id/transition', async (req, res) => {
  const { id } = req.params;
  const { status: targetStatus } = req.body;

  if (!targetStatus) {
    return res.status(400).json({ error: 'MISSING_STATUS', message: 'Body must include "status"' });
  }

  const current = getIncident(id);
  if (!current) {
    return res.status(404).json({ error: 'NOT_FOUND', message: `Incident ${id} not found` });
  }

  const allowed = VALID_TRANSITIONS[current.status] || [];
  if (!allowed.includes(targetStatus)) {
    return res.status(422).json({
      error: 'INVALID_TRANSITION',
      message: `Cannot transition from '${current.status}' to '${targetStatus}'`,
      currentStatus: current.status,
      allowedTransitions: allowed,
    });
  }

  try {
    const previousStatus = current.status;
    const updated = await transitionIncident(id, targetStatus);

    // Publish lifecycle event to Kafka
    await publishIncidentEvent(updated, previousStatus);

    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: 'TRANSITION_FAILED', message: err.message });
  }
});

module.exports = router;

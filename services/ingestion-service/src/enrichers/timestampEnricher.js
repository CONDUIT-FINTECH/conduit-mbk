/**
 * Normalizes the event timestamp to UTC ISO-8601.
 */
function enrichTimestamp(event) {
  const ts = new Date(event.timestamp);
  return {
    ...event,
    timestamp: ts.toISOString(),
    timestampEpochMs: ts.getTime(),
  };
}

module.exports = { enrichTimestamp };

-- Conduit Incidents Table (PostgreSQL)
CREATE TABLE IF NOT EXISTS incidents (
    id              UUID PRIMARY KEY,
    type            VARCHAR(64) NOT NULL,       -- error_rate_breach, latency_breach, error_spike
    severity        VARCHAR(16) NOT NULL,       -- critical, high, medium, low
    status          VARCHAR(24) NOT NULL DEFAULT 'detected',
    source          VARCHAR(128) NOT NULL,
    tenant_id       VARCHAR(64) NOT NULL,
    description     TEXT NOT NULL,
    trigger_data    JSONB NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ,
    assigned_to     VARCHAR(128),
    resolution_note TEXT
);

-- Valid status values: detected, acknowledged, investigating, mitigated, resolved, closed

CREATE INDEX idx_incidents_tenant_status ON incidents (tenant_id, status);
CREATE INDEX idx_incidents_severity ON incidents (severity);
CREATE INDEX idx_incidents_detected_at ON incidents (detected_at DESC);

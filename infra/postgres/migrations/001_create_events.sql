-- Conduit Events Table (PostgreSQL)
CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY,
    event_type      VARCHAR(128) NOT NULL,
    source          VARCHAR(128) NOT NULL,
    tenant_id       VARCHAR(64) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id  UUID,
    idempotency_key UUID UNIQUE NOT NULL,
    payload         JSONB NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type);
CREATE INDEX idx_events_timestamp ON events (timestamp DESC);
CREATE INDEX idx_events_source ON events (source);

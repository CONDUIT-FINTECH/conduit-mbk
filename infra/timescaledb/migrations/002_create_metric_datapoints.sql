-- ═══════════════════════════════════════════════════
--  Conduit Metrics v2 — TimescaleDB Schema
--  Run against the conduit_metrics database
--  Supersedes 001_create_metrics.sql
-- ═══════════════════════════════════════════════════

-- ─── Raw metric datapoints ──────────────────────
-- Every ingested event writes a row here for precise aggregation.
-- TimescaleDB hypertable for automatic time-partitioning.
CREATE TABLE IF NOT EXISTS metric_datapoints (
    time            TIMESTAMPTZ     NOT NULL,
    tenant_id       VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    source          VARCHAR(128)    NOT NULL,
    latency_ms      DOUBLE PRECISION,
    is_error        BOOLEAN         NOT NULL DEFAULT FALSE,
    payload_size    INTEGER,
    correlation_id  UUID
);

-- Convert to TimescaleDB hypertable (auto-partitioned by time)
SELECT create_hypertable('metric_datapoints', 'time', if_not_exists => TRUE);

-- ─── Indexes ────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_mdp_tenant_time
    ON metric_datapoints (tenant_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_mdp_type_time
    ON metric_datapoints (event_type, time DESC);

CREATE INDEX IF NOT EXISTS idx_mdp_source
    ON metric_datapoints (source, time DESC);


-- ═══════════════════════════════════════════════════
--  Continuous Aggregates (TimescaleDB materialized views)
--  These run in the background and keep pre-computed stats
--  for 1-minute and 5-minute windows.
-- ═══════════════════════════════════════════════════

-- ─── 1-minute aggregation ───────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time)       AS bucket,
    tenant_id,
    source,
    COUNT(*)                            AS total_count,
    COUNT(*) FILTER (WHERE is_error)    AS error_count,
    COUNT(*) FILTER (WHERE NOT is_error) AS success_count,

    -- Latency aggregates
    AVG(latency_ms)                     AS avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency_ms,
    MAX(latency_ms)                     AS max_latency_ms,
    MIN(latency_ms)                     AS min_latency_ms,

    -- Rates (0.0–1.0)
    COUNT(*) FILTER (WHERE NOT is_error)::FLOAT / NULLIF(COUNT(*), 0) AS success_rate,
    COUNT(*) FILTER (WHERE is_error)::FLOAT    / NULLIF(COUNT(*), 0) AS error_rate
FROM metric_datapoints
GROUP BY bucket, tenant_id, source
WITH NO DATA;

-- Refresh policy: auto-refresh every 1 minute, look back 10 minutes
SELECT add_continuous_aggregate_policy('metrics_1m',
    start_offset    => INTERVAL '10 minutes',
    end_offset      => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists   => TRUE
);


-- ─── 5-minute aggregation ───────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time)      AS bucket,
    tenant_id,
    source,
    COUNT(*)                            AS total_count,
    COUNT(*) FILTER (WHERE is_error)    AS error_count,
    COUNT(*) FILTER (WHERE NOT is_error) AS success_count,

    AVG(latency_ms)                     AS avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_latency_ms,
    MAX(latency_ms)                     AS max_latency_ms,

    COUNT(*) FILTER (WHERE NOT is_error)::FLOAT / NULLIF(COUNT(*), 0) AS success_rate,
    COUNT(*) FILTER (WHERE is_error)::FLOAT    / NULLIF(COUNT(*), 0) AS error_rate
FROM metric_datapoints
GROUP BY bucket, tenant_id, source
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_5m',
    start_offset    => INTERVAL '30 minutes',
    end_offset      => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists   => TRUE
);


-- ─── Data Retention ─────────────────────────────
-- Raw datapoints: keep 7 days (detailed)
-- 1-minute aggregates: keep 30 days
-- 5-minute aggregates: keep 90 days
SELECT add_retention_policy('metric_datapoints',  INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_retention_policy('metrics_1m',         INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('metrics_5m',         INTERVAL '90 days', if_not_exists => TRUE);

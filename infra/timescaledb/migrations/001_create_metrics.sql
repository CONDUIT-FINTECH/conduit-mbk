-- Conduit Metrics Hypertable (TimescaleDB)
CREATE TABLE IF NOT EXISTS metric_points (
    time        TIMESTAMPTZ NOT NULL,
    tenant_id   VARCHAR(64) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    tags        JSONB DEFAULT '{}'
);

-- Convert to TimescaleDB hypertable (7-day chunks)
SELECT create_hypertable('metric_points', 'time', chunk_time_interval => INTERVAL '7 days');

-- Continuous aggregate for hourly rollups
CREATE MATERIALIZED VIEW metric_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    tenant_id,
    metric_name,
    AVG(value) AS avg_value,
    MAX(value) AS max_value,
    MIN(value) AS min_value,
    COUNT(*) AS sample_count
FROM metric_points
GROUP BY bucket, tenant_id, metric_name;

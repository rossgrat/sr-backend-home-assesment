CREATE TABLE IF NOT EXISTS device_events_cleaned (
    device_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (device_id, timestamp)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('device_events_cleaned', 'timestamp', if_not_exists => TRUE);
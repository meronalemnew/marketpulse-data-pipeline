CREATE TABLE IF NOT EXISTS trades (
    trade_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    symbol VARCHAR(15) NOT NULL,
    price NUMERIC(18, 4) NOT NULL CHECK (price > 0),
    size INTEGER NOT NULL CHECK (size > 0),
    trade_value NUMERIC(18, 4) NOT NULL CHECK (trade_value > 0),
    event_time TIMESTAMPTZ NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    processing_latency_seconds DOUBLE PRECISION NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    CONSTRAINT trades_kafka_position_unique
        UNIQUE (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS trades_symbol_event_time_idx
    ON trades (symbol, event_time);
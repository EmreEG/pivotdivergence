-- Pivot Divergence Trading System Database Schema
-- TimescaleDB extension required

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS book_snapshots CASCADE;
DROP TABLE IF EXISTS mark_price CASCADE;
DROP TABLE IF EXISTS open_interest CASCADE;
DROP TABLE IF EXISTS indicator_state CASCADE;
DROP TABLE IF EXISTS signals CASCADE;
DROP TABLE IF EXISTS obi_regime_stats CASCADE;

-- Book snapshots table: stores order book snapshots
CREATE TABLE book_snapshots (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    bids_json JSONB NOT NULL,
    asks_json JSONB NOT NULL,
    last_update_id BIGINT,
    PRIMARY KEY (symbol, ts)
);

-- Convert to hypertable
SELECT create_hypertable('book_snapshots', 'ts', chunk_time_interval => INTERVAL '1 day');

-- Create index
CREATE INDEX idx_book_symbol_ts ON book_snapshots(symbol, ts DESC);

-- Mark price and funding rate table
CREATE TABLE mark_price (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    mark_price DOUBLE PRECISION NOT NULL,
    funding_rate DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);

-- Convert to hypertable
SELECT create_hypertable('mark_price', 'ts', chunk_time_interval => INTERVAL '1 day');

-- Create index
CREATE INDEX idx_mark_symbol_ts ON mark_price(symbol, ts DESC);

-- Open interest table
CREATE TABLE open_interest (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    oi_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, ts)
);

-- Convert to hypertable
SELECT create_hypertable('open_interest', 'ts', chunk_time_interval => INTERVAL '1 day');

-- Create index
CREATE INDEX idx_oi_symbol_ts ON open_interest(symbol, ts DESC);

-- Indicator state table: stores RSI, MACD, OBV, CVD, OBI values
CREATE TABLE indicator_state (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    rsi DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,
    obv DOUBLE PRECISION,
    ad_line DOUBLE PRECISION,
    realized_vol DOUBLE PRECISION,
    cvd DOUBLE PRECISION,
    obi DOUBLE PRECISION,
    obi_z DOUBLE PRECISION,
    obi_bid_qty DOUBLE PRECISION,
    obi_ask_qty DOUBLE PRECISION,
    obi_depth_levels INTEGER,
    PRIMARY KEY (symbol, ts)
);

-- Convert to hypertable
SELECT create_hypertable('indicator_state', 'ts', chunk_time_interval => INTERVAL '1 day');

-- Create index
CREATE INDEX idx_indicator_symbol_ts ON indicator_state(symbol, ts DESC);

-- OBI regime stats table: stores imbalance with depth context for normalization
CREATE TABLE obi_regime_stats (
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    obi DOUBLE PRECISION,
    obi_z DOUBLE PRECISION,
    bid_qty DOUBLE PRECISION,
    ask_qty DOUBLE PRECISION,
    depth_levels INTEGER,
    total_depth DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    mid_price DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable('obi_regime_stats', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX idx_obi_regime_symbol_ts ON obi_regime_stats(symbol, ts DESC);

-- Signals table: stores generated trading signals
CREATE TABLE signals (
    signal_id VARCHAR(50) NOT NULL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    side VARCHAR(10) NOT NULL,
    level_price DOUBLE PRECISION NOT NULL,
    zone_low DOUBLE PRECISION NOT NULL,
    zone_high DOUBLE PRECISION NOT NULL,
    score DOUBLE PRECISION,
    state VARCHAR(20) NOT NULL,
    divergences JSONB,
    entry_price DOUBLE PRECISION,
    stop_price DOUBLE PRECISION,
    target_price DOUBLE PRECISION,
    exit_price DOUBLE PRECISION,
    pnl DOUBLE PRECISION,
    order_id VARCHAR(50),
    filled_qty DOUBLE PRECISION,
    avg_fill_price DOUBLE PRECISION,
    feature_vector JSONB
);

-- Create index for time-based queries
CREATE INDEX idx_signals_ts ON signals(ts DESC);
CREATE INDEX idx_signals_symbol_state ON signals(symbol, state);

-- Compression policies for space efficiency
SELECT add_compression_policy('book_snapshots', INTERVAL '3 days');
SELECT add_compression_policy('mark_price', INTERVAL '7 days');
SELECT add_compression_policy('open_interest', INTERVAL '7 days');
SELECT add_compression_policy('indicator_state', INTERVAL '7 days');

-- Retention policies (optional - adjust based on storage requirements)
-- SELECT add_retention_policy('book_snapshots', INTERVAL '30 days');
-- SELECT add_retention_policy('mark_price', INTERVAL '90 days');
-- SELECT add_retention_policy('open_interest', INTERVAL '90 days');
-- SELECT add_retention_policy('indicator_state', INTERVAL '90 days');

-- Grant permissions (adjust user as needed)
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO pivotdivergence_user;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO pivotdivergence_user;

-- Display table info
SELECT
    hypertable_name,
    num_dimensions,
    num_chunks,
    compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'public';

COMMENT ON TABLE book_snapshots IS 'Stores periodic order book snapshots';
COMMENT ON TABLE mark_price IS 'Stores mark price and funding rate data';
COMMENT ON TABLE open_interest IS 'Stores open interest data';
COMMENT ON TABLE indicator_state IS 'Stores calculated technical indicators';
COMMENT ON TABLE signals IS 'Stores generated trading signals and their state';

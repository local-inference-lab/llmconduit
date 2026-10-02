-- Baseline schema. Uses IF NOT EXISTS so it adopts databases created by the
-- pre-migration (CREATE-IF-NOT-EXISTS) code without conflict; new databases get
-- the full schema. Types are kept portable across sqlite and postgres
-- (TEXT / BIGINT), with epoch-millisecond integers for timestamps.

CREATE TABLE IF NOT EXISTS requests (
    id TEXT PRIMARY KEY,
    conversation_id TEXT,
    virtual_key_id TEXT,
    client_protocol TEXT,
    client_model TEXT,
    alias TEXT,
    backend TEXT,
    resolved_model TEXT,
    status TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT,
    first_token_at_ms BIGINT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cached_tokens BIGINT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS request_events (
    request_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    ts_ms BIGINT NOT NULL,
    hop TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT,
    bytes BIGINT,
    PRIMARY KEY (request_id, seq)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backend_metrics (
    backend TEXT NOT NULL,
    ts_ms BIGINT NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backend_metrics_ts ON backend_metrics(ts_ms);

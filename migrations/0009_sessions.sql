-- Session tree and prefix lineage.
--
-- `sessions` holds one row per session node. Nodes nest via parent_id and are
-- `declared` (the harness put the id on the wire) or `inferred` (created by
-- the gateway when a request started a new conversation inside its parent).
-- Each request belongs to one node (`requests.session_id`) and records its
-- chain predecessor plus where and how it diverged from it.

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    kind TEXT NOT NULL,
    harness TEXT NOT NULL,
    harness_version TEXT,
    external_id TEXT,
    session_kind TEXT,
    client_label TEXT,
    virtual_key_id TEXT,
    depth BIGINT NOT NULL,
    root_request_id TEXT,
    spawned_by_request_id TEXT,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,
    request_count BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_id);
CREATE INDEX IF NOT EXISTS idx_sessions_external ON sessions(harness, external_id);
CREATE INDEX IF NOT EXISTS idx_sessions_last_seen ON sessions(last_seen_ms);

ALTER TABLE requests ADD COLUMN session_id TEXT;
ALTER TABLE requests ADD COLUMN chain_parent_request_id TEXT;
ALTER TABLE requests ADD COLUMN item_count BIGINT;
ALTER TABLE requests ADD COLUMN shared_prefix_items BIGINT;
ALTER TABLE requests ADD COLUMN divergence_kind TEXT;
ALTER TABLE requests ADD COLUMN divergence_index BIGINT;
ALTER TABLE requests ADD COLUMN cache_bust BIGINT;

CREATE INDEX IF NOT EXISTS idx_requests_session ON requests(session_id, created_at_ms);
CREATE INDEX IF NOT EXISTS idx_requests_cache_bust ON requests(cache_bust, created_at_ms);

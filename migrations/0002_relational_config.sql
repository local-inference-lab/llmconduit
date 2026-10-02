-- Phase 2: promote the operational config from a single JSON blob in `settings`
-- to relational tables. Every config entity carries a full audit envelope
-- (created/updated actor + epoch-ms timestamps and a soft-delete tombstone),
-- ordered membership lives in join tables, and an append-only `audit_log`
-- records every mutation. Also gives `backend_metrics` a real primary key so
-- every table has an `id`.
--
-- Portable across sqlite + postgres: TEXT / BIGINT only, 0/1 for booleans,
-- epoch-millisecond integers for timestamps, and JSON-as-TEXT for the value
-- objects that don't decompose into columns (a backend's metrics config, a
-- profile's upstream chat kwargs).

-- ---- config entities -------------------------------------------------------

CREATE TABLE IF NOT EXISTS backends (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    api_key TEXT,
    request_log_path TEXT,
    enabled BIGINT NOT NULL DEFAULT 1,
    metrics_json TEXT,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    deleted_at_ms BIGINT
);

CREATE TABLE IF NOT EXISTS model_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    upstream_model TEXT,
    system_prompt_prefix TEXT,
    upstream_chat_kwargs_json TEXT,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    deleted_at_ms BIGINT
);

CREATE TABLE IF NOT EXISTS aliases (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    deleted_at_ms BIGINT
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    secret TEXT NOT NULL,
    label TEXT,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    deleted_at_ms BIGINT
);

-- ---- ordered membership (join) tables --------------------------------------
-- Foreign keys keep edges pointing at real entities. Entities are only ever
-- soft-deleted (their rows persist), so an edge target always exists; the read
-- path additionally filters edges to live (non-tombstoned) entities.

CREATE TABLE IF NOT EXISTS profile_backends (
    profile_id TEXT NOT NULL REFERENCES model_profiles(id),
    backend_id TEXT NOT NULL REFERENCES backends(id),
    ordinal BIGINT NOT NULL,
    PRIMARY KEY (profile_id, backend_id)
);

CREATE TABLE IF NOT EXISTS alias_profiles (
    alias_id TEXT NOT NULL REFERENCES aliases(id),
    profile_id TEXT NOT NULL REFERENCES model_profiles(id),
    ordinal BIGINT NOT NULL,
    PRIMARY KEY (alias_id, profile_id)
);

CREATE TABLE IF NOT EXISTS key_aliases (
    key_id TEXT NOT NULL REFERENCES api_keys(id),
    alias_id TEXT NOT NULL REFERENCES aliases(id),
    PRIMARY KEY (key_id, alias_id)
);

-- ---- audit trail -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    ts_ms BIGINT NOT NULL,
    actor TEXT,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    entity_name TEXT,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(ts_ms);

-- ---- backend_metrics: add a primary key ------------------------------------
-- Rebuild to introduce an `id` PK. The table holds short-lived, retention-
-- pruned telemetry samples (sparkline history), so dropping prior rows is
-- harmless — fresh samples repopulate within one scrape interval.

DROP TABLE IF EXISTS backend_metrics;

CREATE TABLE backend_metrics (
    id TEXT PRIMARY KEY,
    backend TEXT NOT NULL,
    ts_ms BIGINT NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backend_metrics_ts ON backend_metrics(ts_ms);

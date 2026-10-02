-- Phase 3: local user accounts for admin login. Passwords are Argon2 hashes
-- (never plaintext). Carries the same audit envelope as the config entities
-- (created/updated actor + epoch-ms timestamps, soft-delete tombstone).
--
-- Sessions are kept in memory by the running gateway (re-login after a restart),
-- so there is no sessions table. A break-glass bootstrap admin authenticated
-- from the LLMCONDUIT_ADMIN_PASSWORD env/YAML credential always works — including
-- in no-storage mode, where this table does not exist.

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_admin BIGINT NOT NULL DEFAULT 1,
    created_at_ms BIGINT NOT NULL,
    created_by TEXT,
    updated_at_ms BIGINT NOT NULL,
    updated_by TEXT,
    deleted_at_ms BIGINT
);

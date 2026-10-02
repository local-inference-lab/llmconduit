-- Phase 5: an API key belongs to a user (its owner). Nullable so legacy keys and
-- keys created in open mode stay valid until assigned. Nullable + default NULL is
-- required for SQLite's ADD COLUMN to accept the REFERENCES clause.

ALTER TABLE api_keys ADD COLUMN user_id TEXT REFERENCES users(id);

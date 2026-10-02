-- Attribution: the owner of the virtual key that opened a session node.
--
-- `LinkInput` already carries `virtual_key_id` on every link call; `user_id`
-- arrives through the same seam (the authenticating key's owner, from the live
-- client-auth registry). Nullable: legacy rows, open-mode requests, and keys
-- without an owner have no user. Backfill is not possible (the historical key
-- owner is not derivable from the session row alone); new rows populate it.

ALTER TABLE sessions ADD COLUMN user_id TEXT;
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

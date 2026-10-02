-- User accounts and per-user API keys.
--
-- `requests.user_id` attributes every request to the owner of the virtual key
-- that authenticated it (from the live key registry), so usage rolls up per
-- user as well as per key. `api_keys.allowed_models_json` carries the optional
-- list of client-facing model/alias names a SQL-managed key may request
-- (the YAML equivalent is `allowed_aliases`).

ALTER TABLE requests ADD COLUMN user_id TEXT;
CREATE INDEX IF NOT EXISTS idx_requests_user_created_at_ms ON requests(user_id, created_at_ms);

ALTER TABLE api_keys ADD COLUMN allowed_models_json TEXT;

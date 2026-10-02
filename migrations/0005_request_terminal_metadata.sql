-- Integration/upstream-control-plane: keep the legacy request id as the stable
-- api_call_id while retaining the wire response id separately, and persist the
-- authoritative terminal aggregate without reconstructing it from hop events.
-- Nullable columns preserve all pre-existing rows and remain portable across
-- SQLite and Postgres.

ALTER TABLE requests ADD COLUMN response_id TEXT;
ALTER TABLE requests ADD COLUMN attempts_json TEXT;
ALTER TABLE requests ADD COLUMN timings_json TEXT;
ALTER TABLE requests ADD COLUMN reasoning_tokens BIGINT;
ALTER TABLE requests ADD COLUMN terminal_reason TEXT;
ALTER TABLE requests ADD COLUMN client_source TEXT;
ALTER TABLE requests ADD COLUMN client_label TEXT;

CREATE INDEX IF NOT EXISTS idx_requests_response_id ON requests(response_id);

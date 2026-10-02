-- Harness identity per request: which client program sent it and the session
-- identifiers it declared on the wire. Session-tree linkage (gateway session
-- nodes, lineage, divergence) is a later migration; these columns are the raw
-- detected facts and are written at request begin.

ALTER TABLE requests ADD COLUMN harness TEXT;
ALTER TABLE requests ADD COLUMN harness_version TEXT;
ALTER TABLE requests ADD COLUMN harness_session_id TEXT;
ALTER TABLE requests ADD COLUMN harness_sub_session_id TEXT;
ALTER TABLE requests ADD COLUMN harness_parent_session_id TEXT;
ALTER TABLE requests ADD COLUMN session_kind TEXT;

CREATE INDEX IF NOT EXISTS idx_requests_harness_session
    ON requests(harness, harness_session_id, created_at_ms);

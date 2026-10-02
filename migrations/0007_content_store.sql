-- Content-addressed request body storage.
--
-- Request bodies are split into items (instructions/system, each tool
-- definition, each message). Each distinct item is stored once in
-- `content_blobs`, keyed by the SHA-256 of its canonical JSON. `request_items`
-- records which items a request carried on which hop, in body order. The hop's
-- `request_events` row keeps the skeleton: the body with each item replaced by
-- a `{"$llmconduit_blob": "<hash>"}` reference.
--
-- Type notes: `content` is TEXT because items are always canonical UTF-8 JSON;
-- TEXT/BIGINT are the only column types shared verbatim by SQLite and Postgres.

CREATE TABLE IF NOT EXISTS content_blobs (
    hash TEXT PRIMARY KEY,
    media TEXT NOT NULL,
    size BIGINT NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS request_items (
    request_id TEXT NOT NULL,
    hop TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    section TEXT NOT NULL,
    kind TEXT,
    blob_hash TEXT NOT NULL,
    PRIMARY KEY (request_id, hop, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_request_items_blob ON request_items(blob_hash);

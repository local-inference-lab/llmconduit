-- Bound dashboard history and usage reads by the request timestamp without
-- scanning the complete lifecycle table. The composite index serves the
-- per-virtual-key usage filter while retaining the time-range ordering.

CREATE INDEX IF NOT EXISTS idx_requests_created_at_ms
    ON requests(created_at_ms);

CREATE INDEX IF NOT EXISTS idx_requests_virtual_key_created_at_ms
    ON requests(virtual_key_id, created_at_ms);

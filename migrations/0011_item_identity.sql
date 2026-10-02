-- Lineage identity per stored item: the storage hash with volatile markers
-- (`cache_control`) stripped. NULL for rows written before this column
-- existed; readers fall back to blob_hash.
ALTER TABLE request_items ADD COLUMN identity_hash TEXT;

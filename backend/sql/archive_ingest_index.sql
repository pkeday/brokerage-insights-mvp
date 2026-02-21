-- Optional helper table for fast lookups from archived email IDs to metadata.
-- This does not replace JSON archive storage in the MVP state snapshot.
CREATE TABLE IF NOT EXISTS archive_ingest_index (
  archive_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  message_id TEXT,
  thread_id TEXT,
  broker TEXT,
  subject TEXT,
  from_address TEXT,
  published_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS archive_ingest_index_user_message_uidx
  ON archive_ingest_index (user_id, message_id)
  WHERE message_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS archive_ingest_index_user_archived_idx
  ON archive_ingest_index (user_id, archived_at DESC);

CREATE INDEX IF NOT EXISTS archive_ingest_index_user_broker_idx
  ON archive_ingest_index (user_id, broker);

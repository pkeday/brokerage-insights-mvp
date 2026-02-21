-- Canonical extracted report rows, deduped per user by duplicate_key.
CREATE TABLE IF NOT EXISTS extracted_reports (
  id BIGSERIAL PRIMARY KEY,
  archive_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  extraction_run_id BIGINT REFERENCES extraction_runs(id) ON DELETE SET NULL,
  broker TEXT NOT NULL,
  company_canonical TEXT NOT NULL,
  company_raw TEXT,
  report_type TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT,
  published_at TIMESTAMPTZ,
  confidence NUMERIC(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  duplicate_key TEXT NOT NULL,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, duplicate_key)
);

CREATE INDEX IF NOT EXISTS extracted_reports_user_published_idx
  ON extracted_reports (user_id, published_at DESC NULLS LAST, id DESC);

CREATE INDEX IF NOT EXISTS extracted_reports_user_broker_company_idx
  ON extracted_reports (user_id, broker, company_canonical, published_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS extracted_reports_run_idx
  ON extracted_reports (extraction_run_id, id DESC);

CREATE INDEX IF NOT EXISTS extracted_reports_archive_idx
  ON extracted_reports (archive_id);

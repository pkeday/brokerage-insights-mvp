-- Tracks each extraction pipeline run from enqueue to completion.
CREATE TABLE IF NOT EXISTS extraction_runs (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  trigger_ref TEXT,
  status TEXT NOT NULL DEFAULT 'queued',
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  archives_considered INTEGER NOT NULL DEFAULT 0 CHECK (archives_considered >= 0),
  archives_processed INTEGER NOT NULL DEFAULT 0 CHECK (archives_processed >= 0),
  reports_extracted INTEGER NOT NULL DEFAULT 0 CHECK (reports_extracted >= 0),
  reports_inserted INTEGER NOT NULL DEFAULT 0 CHECK (reports_inserted >= 0),
  reports_updated INTEGER NOT NULL DEFAULT 0 CHECK (reports_updated >= 0),
  points_inserted INTEGER NOT NULL DEFAULT 0 CHECK (points_inserted >= 0),
  error_code TEXT,
  error_message TEXT,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'canceled')),
  CHECK (finished_at IS NULL OR started_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS extraction_runs_user_requested_idx
  ON extraction_runs (user_id, requested_at DESC);

CREATE INDEX IF NOT EXISTS extraction_runs_status_requested_idx
  ON extraction_runs (status, requested_at DESC);

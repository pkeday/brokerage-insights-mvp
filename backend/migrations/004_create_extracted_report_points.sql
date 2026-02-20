BEGIN;

-- 1:N key points for each extracted report.
CREATE TABLE IF NOT EXISTS extracted_report_points (
  id BIGSERIAL PRIMARY KEY,
  report_id BIGINT NOT NULL REFERENCES extracted_reports(id) ON DELETE CASCADE,
  point_order INTEGER NOT NULL CHECK (point_order >= 0),
  point_text TEXT NOT NULL,
  point_type TEXT NOT NULL DEFAULT 'key_point',
  confidence NUMERIC(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (report_id, point_order)
);

CREATE INDEX IF NOT EXISTS extracted_report_points_report_order_idx
  ON extracted_report_points (report_id, point_order ASC);

COMMIT;

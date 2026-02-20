# Phase 2 Schema + Jobs (Agent A)

This document defines the normalized extraction schema introduced for Phase 2 and how to apply migrations.

## Tables

### `extraction_runs`
Tracks lifecycle and counters for each extraction pipeline run.

Core columns:
- `id BIGSERIAL PRIMARY KEY`
- `user_id TEXT NOT NULL`
- `trigger_type TEXT NOT NULL`
- `status TEXT NOT NULL` (`queued|running|succeeded|failed|canceled`)
- `requested_at`, `started_at`, `finished_at`
- `archives_considered`, `archives_processed`
- `reports_extracted`, `reports_inserted`, `reports_updated`, `points_inserted`
- `error_code`, `error_message`
- `meta JSONB`
- `created_at`, `updated_at`

### `archive_ingest_index` (optional helper)
Lookup/index table for archived email metadata. This is additive and does not replace current JSON snapshot archive storage.

Core columns:
- `archive_id TEXT PRIMARY KEY`
- `user_id TEXT NOT NULL`
- `message_id`, `thread_id`
- `broker`, `subject`, `from_address`
- `published_at`, `archived_at`
- `meta JSONB`

### `extracted_reports`
Canonical normalized report rows produced by extraction.

Core columns:
- `id BIGSERIAL PRIMARY KEY`
- `archive_id TEXT NOT NULL`
- `user_id TEXT NOT NULL`
- `extraction_run_id BIGINT REFERENCES extraction_runs(id)`
- `broker`, `company_canonical`, `company_raw`
- `report_type`, `title`, `summary`
- `published_at`, `confidence`
- `duplicate_key TEXT NOT NULL`
- `raw_payload JSONB`
- `created_at`, `updated_at`

Constraints:
- `UNIQUE (user_id, duplicate_key)` for per-user dedupe.

### `extracted_report_points`
Normalized point rows for each extracted report (1:N relationship).

Core columns:
- `id BIGSERIAL PRIMARY KEY`
- `report_id BIGINT NOT NULL REFERENCES extracted_reports(id) ON DELETE CASCADE`
- `point_order INTEGER NOT NULL`
- `point_text TEXT NOT NULL`
- `point_type TEXT NOT NULL DEFAULT 'key_point'`
- `confidence NUMERIC(5,4)`
- `meta JSONB`
- `created_at`, `updated_at`

Constraints:
- `UNIQUE (report_id, point_order)` for deterministic ordering.

## Migration files

Ordered migration scripts in `backend/migrations/`:
1. `001_create_extraction_runs.sql`
2. `002_create_archive_ingest_index.sql`
3. `003_create_extracted_reports.sql`
4. `004_create_extracted_report_points.sql`

Canonical SQL definitions are also available in `backend/sql/`.

## Applying migrations

From repository root:

```bash
export DATABASE_URL='postgres://...'

psql "$DATABASE_URL" -f backend/migrations/001_create_extraction_runs.sql
psql "$DATABASE_URL" -f backend/migrations/002_create_archive_ingest_index.sql
psql "$DATABASE_URL" -f backend/migrations/003_create_extracted_reports.sql
psql "$DATABASE_URL" -f backend/migrations/004_create_extracted_report_points.sql
```

To verify tables:

```bash
psql "$DATABASE_URL" -c "\dt extraction_runs extracted_reports extracted_report_points archive_ingest_index"
```

## Extraction run store module

`backend/jobs/extractionRunsStore.js` exposes:
- `createExtractionRun(pool, params)`
- `updateExtractionRun(pool, runId, updates)`
- `getExtractionRunById(pool, runId)`
- `getExtractionRunStatus(pool, runId)`
- `getLatestExtractionRunStatusForUser(pool, userId)`

Example usage in API/orchestration layer:

```js
import {
  createExtractionRun,
  updateExtractionRun,
  getExtractionRunStatus,
  EXTRACTION_RUN_STATUSES
} from "./jobs/extractionRunsStore.js";

const run = await createExtractionRun(pool, {
  userId,
  triggerType: "daily-cron",
  triggerRef: scheduleDate,
  status: EXTRACTION_RUN_STATUSES.QUEUED,
  meta: { source: "api/jobs/daily" }
});

await updateExtractionRun(pool, run.id, {
  status: EXTRACTION_RUN_STATUSES.RUNNING,
  startedAt: new Date()
});

const status = await getExtractionRunStatus(pool, run.id);
```

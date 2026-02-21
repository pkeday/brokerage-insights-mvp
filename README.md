# Brokerage Insights MVP

Isolated repository for brokerage-email ingestion and insight extraction.

This repo is intentionally split from `ai-webapp` to avoid collisions with other ongoing agent work.

## Isolation guarantees

- Separate git repo and deployment workflow.
- Separate backend service name (`brokerage-insights-mvp-api`).
- Separate Postgres table namespace via `BROKERAGE_DB_TABLE` (default `brokerage_insights_mvp_state`).
- Separate GitHub Actions secrets for cron:
  - `MVP_BROKERAGE_API_BASE_URL`
  - `MVP_BROKERAGE_CRON_SECRET`

## Local run

```bash
cd backend
npm install
cp .env.example .env
npm start
```

Static frontend can be served from repo root:

```bash
python3 -m http.server 5173
```

## Render env vars (backend)

- `APP_NAME=brokerage-insights-mvp-api`
- `APP_ENV=production`
- `CORS_ORIGIN=<your frontend origin>`
- `FRONTEND_URL=<your frontend URL>`
- `PUBLIC_API_BASE_URL=<this backend URL>`
- `AUTH_SECRET=<random long secret>`
- `TOKEN_ENCRYPTION_KEY=<random long secret>`
- `CRON_SECRET=<random long secret>`
- `DATABASE_URL=<render postgres connection>`
- `DATABASE_SSL_MODE=require`
- `DATABASE_MAX_CONNECTIONS=5`
- `BROKERAGE_DB_TABLE=brokerage_insights_mvp_state`
- `BROKERAGE_DB_STATE_ID=primary`

## Notes

- This is currently MVP code with Phase 2 schema/jobs foundations added.

## Phase 2 schema + jobs

Normalized extraction schema and job-run tracking are now defined under:
- `backend/sql/`
- `backend/migrations/`
- `backend/jobs/extractionRunsStore.js`

Detailed table definitions and usage notes:
- `docs/PHASE2_SCHEMA_JOBS.md`

Apply migrations (in order):

```bash
export DATABASE_URL='postgres://...'

psql "$DATABASE_URL" -f backend/migrations/001_create_extraction_runs.sql
psql "$DATABASE_URL" -f backend/migrations/002_create_archive_ingest_index.sql
psql "$DATABASE_URL" -f backend/migrations/003_create_extracted_reports.sql
psql "$DATABASE_URL" -f backend/migrations/004_create_extracted_report_points.sql
```

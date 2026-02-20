# Phase 2 Multi-Agent Split

This repo is now split into four parallel agent lanes using dedicated git worktrees.

## Worktrees and branches

- Agent A (Schema + Jobs)
  - Branch: `codex/phase2-agent-a-schema-jobs`
  - Path: `/Users/parikshitkabra/Projects/codex_projects/brokerage-insights-mvp-agent-a`
- Agent B (Extraction + Dedupe core)
  - Branch: `codex/phase2-agent-b-extraction`
  - Path: `/Users/parikshitkabra/Projects/codex_projects/brokerage-insights-mvp-agent-b`
- Agent C (API + Orchestration)
  - Branch: `codex/phase2-agent-c-api-orchestration`
  - Path: `/Users/parikshitkabra/Projects/codex_projects/brokerage-insights-mvp-agent-c`
- Agent D (Frontend + UX wiring)
  - Branch: `codex/phase2-agent-d-frontend`
  - Path: `/Users/parikshitkabra/Projects/codex_projects/brokerage-insights-mvp-agent-d`

## Ownership boundaries

- Agent A owns:
  - `backend/sql/**`
  - `backend/migrations/**`
  - `backend/jobs/**` (job shell + run status model)
- Agent B owns:
  - `backend/extraction/**`
  - `backend/dedupe/**`
  - `backend/normalization/**`
- Agent C owns:
  - `backend/server.js` (new endpoints only)
  - `backend/routes/**` (if created)
  - `backend/services/**` orchestration glue
- Agent D owns:
  - `index.html`
  - `app.js`
  - `style.css`

## Shared contracts (must stay stable)

- Report extraction output shape:
  - `archiveId`, `userId`, `broker`, `companyCanonical`, `companyRaw`
  - `reportType`, `title`, `summary`, `keyPoints[]`
  - `publishedAt`, `confidence`, `duplicateKey`
- Dedupe key policy:
  - same `userId` + `broker` + `companyCanonical` + normalized `title/reportType` + day-bucket
- API response for extracted items:
  - `items[]`, `total`, `limit`, `offset`

## Merge order

1. Agent A (schema base)
2. Agent B (depends on schema)
3. Agent C (depends on A+B contracts)
4. Agent D (depends on C endpoints)

## Rules for parallel safety

- Do not edit another agent's owned files unless explicitly coordinated.
- If cross-lane changes are required, publish a contract note first in PR description.
- Keep commits small and lane-specific.
- Rebase on `main` before merge.


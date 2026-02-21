# Extraction + Dedupe Heuristics (Phase 2 / Agent B)

This document describes the rule-based extraction pipeline added under:

- `backend/extraction/**`
- `backend/normalization/**`
- `backend/dedupe/**`

## Input

The parser expects archived email records with fields commonly present in `emailArchives`:

- `id`, `userId`, `broker`
- `subject`, `snippet`, `bodyPreview`
- `dateHeader`, `internalDateMs`, `ingestedAt`

## Output Contract

Each archive is transformed into a normalized object with:

- `archiveId`, `userId`, `broker`
- `companyCanonical`, `companyRaw`
- `reportType`, `title`, `summary`, `keyPoints[]`
- `publishedAt`, `confidence`, `duplicateKey`

Primary entrypoints:

- `parseArchiveToReport(record)` in `backend/extraction/archive-to-report.js`
- `extractReportsFromArchives(records)` in `backend/extraction/archive-to-report.js`

## Report Type Classification

Classifier file: `backend/extraction/report-type-classifier.js`

Order of precedence:

1. `initiation`
2. `results_update`
3. `target_change`
4. `rating_change`
5. `general_update` (explicit pattern or fallback)

Patterns are regex-based and inspect subject first, then combined text (`subject + snippet + bodyPreview`).

## Company Extraction / Normalization

Company extraction file: `backend/normalization/company.js`

Heuristics:

- Prefer subject-based extraction:
  - `Company: ...`
  - `... on Company`
  - `Company results update ...`
- Fallback to body snippet using `for|on|of Company` style patterns.
- Remove report noise words and legal suffixes for canonicalization.

Canonicalization examples:

- `ABC Industries Ltd` -> `ABC Industries`
- `XYZ Tech Limited` -> `XYZ Tech`

If no reliable match is found:

- `companyRaw = "Unknown Company"`
- `companyCanonical = "Unknown Company"`

## Dedupe Key Policy

Dedupe key generator: `backend/dedupe/report-dedupe-key.js`

Key inputs:

- `userId`
- `broker` (explicitly included to enforce broker separation)
- `companyCanonical`
- normalized `reportType`
- normalized `title`
- UTC day bucket derived from `publishedAt`

The final key is deterministic:

- `rpt_<YYYY-MM-DD>_<sha256-prefix>`

## Confidence Scoring

`confidence` is rule-based and combines:

- report type classification confidence
- company extraction confidence
- small summary-availability boost

Current score range is clamped to `[0.2, 0.99]`.

## Self-Check Runner

A fixture-driven runner is available at:

- `backend/run-extraction-self-check.js`

Fixture file:

- `backend/extraction/fixtures/archive-records.fixture.json`

Run:

```bash
cd backend
npm run self-check:extraction
```

## Known Limitations

- English-only regex heuristics; no multilingual support.
- Company extraction can fail for highly stylized subjects or missing entity mentions.
- No attachment parsing or PDF text extraction in this phase.
- Dedupe key is rule-based; semantically similar titles with different wording can still diverge.
- No ML/LLM semantic classification; categories rely entirely on keyword patterns.

export const EXTRACTION_RUN_STATUSES = Object.freeze({
  QUEUED: "queued",
  RUNNING: "running",
  SUCCEEDED: "succeeded",
  FAILED: "failed",
  CANCELED: "canceled"
});

const VALID_STATUSES = new Set(Object.values(EXTRACTION_RUN_STATUSES));
const ALLOWED_UPDATE_FIELDS = new Set([
  "status",
  "triggerRef",
  "startedAt",
  "finishedAt",
  "archivesConsidered",
  "archivesProcessed",
  "reportsExtracted",
  "reportsInserted",
  "reportsUpdated",
  "pointsInserted",
  "errorCode",
  "errorMessage",
  "meta",
  "metaPatch"
]);

function assertPool(pool) {
  if (!pool || typeof pool.query !== "function") {
    throw new TypeError("A pg Pool/Client with a query method is required");
  }
}

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
  return value.trim();
}

function assertStatus(status) {
  const normalized = assertNonEmptyString(status, "status").toLowerCase();
  if (!VALID_STATUSES.has(normalized)) {
    throw new TypeError(`Unsupported extraction run status: ${status}`);
  }
  return normalized;
}

function normalizeRunId(runId) {
  const id = Number.parseInt(String(runId), 10);
  if (!Number.isInteger(id) || id <= 0) {
    throw new TypeError("runId must be a positive integer");
  }
  return id;
}

function normalizeTimestamp(value, fieldName) {
  if (value === null) {
    return null;
  }

  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError(`${fieldName} must be a valid date value`);
  }

  return parsed.toISOString();
}

function normalizeCounter(value, fieldName) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new TypeError(`${fieldName} must be an integer >= 0`);
  }
  return parsed;
}

function normalizeOptionalText(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function normalizeJson(value, fieldName) {
  if (value === undefined || value === null) {
    return "{}";
  }

  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${fieldName} must be a plain object`);
  }

  return JSON.stringify(value);
}

export async function createExtractionRun(pool, params = {}) {
  assertPool(pool);

  const userId = assertNonEmptyString(params.userId, "userId");
  const triggerType = assertNonEmptyString(params.triggerType ?? "manual", "triggerType");
  const triggerRef = normalizeOptionalText(params.triggerRef);
  const status = assertStatus(params.status ?? EXTRACTION_RUN_STATUSES.QUEUED);

  const archivesConsidered = normalizeCounter(params.archivesConsidered ?? 0, "archivesConsidered");
  const archivesProcessed = normalizeCounter(params.archivesProcessed ?? 0, "archivesProcessed");
  const reportsExtracted = normalizeCounter(params.reportsExtracted ?? 0, "reportsExtracted");
  const reportsInserted = normalizeCounter(params.reportsInserted ?? 0, "reportsInserted");
  const reportsUpdated = normalizeCounter(params.reportsUpdated ?? 0, "reportsUpdated");
  const pointsInserted = normalizeCounter(params.pointsInserted ?? 0, "pointsInserted");
  const meta = normalizeJson(params.meta, "meta");

  const result = await pool.query(
    `INSERT INTO extraction_runs (
       user_id,
       trigger_type,
       trigger_ref,
       status,
       archives_considered,
       archives_processed,
       reports_extracted,
       reports_inserted,
       reports_updated,
       points_inserted,
       meta,
       updated_at
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, NOW())
     RETURNING *`,
    [
      userId,
      triggerType,
      triggerRef,
      status,
      archivesConsidered,
      archivesProcessed,
      reportsExtracted,
      reportsInserted,
      reportsUpdated,
      pointsInserted,
      meta
    ]
  );

  return result.rows[0] ?? null;
}

export async function updateExtractionRun(pool, runId, updates = {}) {
  assertPool(pool);
  const id = normalizeRunId(runId);

  if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
    throw new TypeError("updates must be an object");
  }

  for (const key of Object.keys(updates)) {
    if (!ALLOWED_UPDATE_FIELDS.has(key)) {
      throw new TypeError(`Unsupported extraction run update field: ${key}`);
    }
  }

  if (Object.hasOwn(updates, "meta") && Object.hasOwn(updates, "metaPatch")) {
    throw new TypeError("Use either meta or metaPatch in one update call, not both");
  }

  const values = [];
  const setClauses = [];

  const addSet = (expression, value) => {
    values.push(value);
    setClauses.push(`${expression} = $${values.length}`);
  };

  if (Object.hasOwn(updates, "status")) {
    addSet("status", assertStatus(updates.status));
  }

  if (Object.hasOwn(updates, "triggerRef")) {
    addSet("trigger_ref", normalizeOptionalText(updates.triggerRef));
  }

  if (Object.hasOwn(updates, "startedAt")) {
    addSet("started_at", normalizeTimestamp(updates.startedAt, "startedAt"));
  }

  if (Object.hasOwn(updates, "finishedAt")) {
    addSet("finished_at", normalizeTimestamp(updates.finishedAt, "finishedAt"));
  }

  if (Object.hasOwn(updates, "archivesConsidered")) {
    addSet("archives_considered", normalizeCounter(updates.archivesConsidered, "archivesConsidered"));
  }

  if (Object.hasOwn(updates, "archivesProcessed")) {
    addSet("archives_processed", normalizeCounter(updates.archivesProcessed, "archivesProcessed"));
  }

  if (Object.hasOwn(updates, "reportsExtracted")) {
    addSet("reports_extracted", normalizeCounter(updates.reportsExtracted, "reportsExtracted"));
  }

  if (Object.hasOwn(updates, "reportsInserted")) {
    addSet("reports_inserted", normalizeCounter(updates.reportsInserted, "reportsInserted"));
  }

  if (Object.hasOwn(updates, "reportsUpdated")) {
    addSet("reports_updated", normalizeCounter(updates.reportsUpdated, "reportsUpdated"));
  }

  if (Object.hasOwn(updates, "pointsInserted")) {
    addSet("points_inserted", normalizeCounter(updates.pointsInserted, "pointsInserted"));
  }

  if (Object.hasOwn(updates, "errorCode")) {
    addSet("error_code", normalizeOptionalText(updates.errorCode));
  }

  if (Object.hasOwn(updates, "errorMessage")) {
    addSet("error_message", normalizeOptionalText(updates.errorMessage));
  }

  if (Object.hasOwn(updates, "meta")) {
    values.push(normalizeJson(updates.meta, "meta"));
    setClauses.push(`meta = $${values.length}::jsonb`);
  }

  if (Object.hasOwn(updates, "metaPatch")) {
    values.push(normalizeJson(updates.metaPatch, "metaPatch"));
    setClauses.push(`meta = COALESCE(meta, '{}'::jsonb) || $${values.length}::jsonb`);
  }

  if (setClauses.length === 0) {
    throw new TypeError("No valid updates were provided");
  }

  setClauses.push("updated_at = NOW()");
  values.push(id);

  const result = await pool.query(
    `UPDATE extraction_runs
     SET ${setClauses.join(", ")}
     WHERE id = $${values.length}
     RETURNING *`,
    values
  );

  return result.rows[0] ?? null;
}

export async function getExtractionRunById(pool, runId) {
  assertPool(pool);
  const id = normalizeRunId(runId);

  const result = await pool.query("SELECT * FROM extraction_runs WHERE id = $1", [id]);
  return result.rows[0] ?? null;
}

export async function getExtractionRunStatus(pool, runId) {
  assertPool(pool);
  const id = normalizeRunId(runId);

  const result = await pool.query(
    `SELECT
       id,
       user_id,
       status,
       requested_at,
       started_at,
       finished_at,
       archives_considered,
       archives_processed,
       reports_extracted,
       reports_inserted,
       reports_updated,
       points_inserted,
       error_code,
       error_message,
       updated_at
     FROM extraction_runs
     WHERE id = $1`,
    [id]
  );

  return result.rows[0] ?? null;
}

export async function getLatestExtractionRunStatusForUser(pool, userId) {
  assertPool(pool);
  const normalizedUserId = assertNonEmptyString(userId, "userId");

  const result = await pool.query(
    `SELECT
       id,
       user_id,
       status,
       requested_at,
       started_at,
       finished_at,
       archives_considered,
       archives_processed,
       reports_extracted,
       reports_inserted,
       reports_updated,
       points_inserted,
       error_code,
       error_message,
       updated_at
     FROM extraction_runs
     WHERE user_id = $1
     ORDER BY requested_at DESC, id DESC
     LIMIT 1`,
    [normalizedUserId]
  );

  return result.rows[0] ?? null;
}

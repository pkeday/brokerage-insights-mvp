import { createServer } from "node:http";
import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual
} from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { createExtractionOrchestrator } from "./services/extraction-orchestration.js";
import { createCompanyUpdateExtractor } from "./services/company-update-extractor.js";
import { redactPiiText } from "./extraction/pii-redaction.js";

const port = Number.parseInt(process.env.PORT ?? "10001", 10);
const appName = process.env.APP_NAME ?? "brokerage-insights-mvp-api";
const appEnv = process.env.APP_ENV?.trim() || process.env.NODE_ENV?.trim() || "development";
const isProduction = appEnv.toLowerCase() === "production";
const cronSecret = process.env.CRON_SECRET ?? "";
const corsOrigins = (process.env.CORS_ORIGIN ?? "")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const configuredAuthSecret = process.env.AUTH_SECRET?.trim() || "";
const configuredTokenEncryptionKey = process.env.TOKEN_ENCRYPTION_KEY?.trim() || "";

if (isProduction) {
  const missingVars = [];

  if (!configuredAuthSecret) {
    missingVars.push("AUTH_SECRET");
  }

  if (!configuredTokenEncryptionKey) {
    missingVars.push("TOKEN_ENCRYPTION_KEY");
  }

  if (corsOrigins.length === 0) {
    missingVars.push("CORS_ORIGIN");
  }

  if (missingVars.length > 0) {
    throw new Error(
      `[config] Missing required production env vars: ${missingVars.join(", ")}`
    );
  }
}

const authSecret = configuredAuthSecret || `dev-${randomBytes(32).toString("hex")}`;
const encryptionKey = deriveKey(configuredTokenEncryptionKey || authSecret);
const publicApiBase = process.env.PUBLIC_API_BASE_URL?.trim().replace(/\/$/, "") || "";

const googleClientId = process.env.GOOGLE_CLIENT_ID?.trim() || "";
const googleClientSecret = process.env.GOOGLE_CLIENT_SECRET?.trim() || "";
const googleRedirectUri = process.env.GOOGLE_REDIRECT_URI?.trim() || "";
const googleScopes =
  process.env.GOOGLE_OAUTH_SCOPES?.trim() ||
  "openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send";
const databaseUrl = process.env.DATABASE_URL?.trim() || "";
const databaseSslMode = (process.env.DATABASE_SSL_MODE || (isProduction ? "require" : "disable")).trim().toLowerCase();
const databaseMaxConnections = Number.parseInt(process.env.DATABASE_MAX_CONNECTIONS ?? "5", 10);

const frontendUrl = process.env.FRONTEND_URL?.trim() || corsOrigins[0] || "http://localhost:5173";
const allowedRedirects = new Set(
  [
    frontendUrl,
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    ...(process.env.AUTH_ALLOWED_REDIRECTS ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean)
  ].map((value) => normalizeRedirect(value)).filter(Boolean)
);

const serviceRootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)));
const dataDir = path.resolve(serviceRootDir, process.env.BROKERAGE_DATA_DIR?.trim() || "data");
const dbFilePath = path.join(dataDir, "app-db.json");
const archiveRootDir = path.join(dataDir, "email-archive");
const configuredDbSnapshotTable = process.env.BROKERAGE_DB_TABLE?.trim() || "brokerage_insights_mvp_state";
const dbSnapshotTable = isSafeSqlIdentifier(configuredDbSnapshotTable)
  ? configuredDbSnapshotTable
  : "brokerage_insights_mvp_state";
const dbSnapshotId = process.env.BROKERAGE_DB_STATE_ID?.trim() || "primary";
const ingestedEmailsTable = "brokerage_ingested_emails";
const companyUpdatesTable = "brokerage_company_updates";

function isSafeSqlIdentifier(value) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(String(value ?? ""));
}

const defaultBrokerMappings = [
  {
    broker: "Axis Capital",
    patterns: ["axiscapital", "axis", "@axiscap"],
    enabled: true
  },
  {
    broker: "Kotak Institutional Equities",
    patterns: ["kotak", "institutional.equities"],
    enabled: true
  },
  {
    broker: "Jefferies India",
    patterns: ["jefferies"],
    enabled: true
  },
  {
    broker: "Emkay Global",
    patterns: ["emkay", "@emkayglobal"],
    enabled: true
  }
];

const defaultDb = {
  users: [],
  sessions: [],
  gmailConnections: [],
  gmailPreferences: [],
  emailArchives: [],
  extractionRuns: [],
  extractedReports: [],
  notes: [],
  counters: {
    noteId: 1
  },
  cronState: {
    runCount: 0,
    lastRunAt: null,
    workerHeartbeatCount: 0,
    lastWorkerHeartbeatAt: null
  }
};

const { Pool } = pg;
const databasePool = databaseUrl
  ? new Pool({
      connectionString: databaseUrl,
      max: Number.isFinite(databaseMaxConnections) ? Math.max(1, databaseMaxConnections) : 5,
      ssl: databaseSslMode === "disable" ? false : { rejectUnauthorized: false }
    })
  : null;
let databaseReady = false;

await initializeDatabase();
let db = await loadDb();
let persistQueue = Promise.resolve();
const extractionOrchestrator = await createExtractionOrchestrator({
  getDb: () => db,
  persistDb,
  newId,
  log
});
const companyUpdateExtractor = createCompanyUpdateExtractor({
  apiKey: process.env.OPENAI_API_KEY
});

if (!configuredAuthSecret) {
  log("AUTH_SECRET is not set. A temporary runtime secret is being used.");
}

function getAllowedOrigin(originHeader) {
  if (corsOrigins.length === 0) {
    return "*";
  }

  if (originHeader && corsOrigins.includes(originHeader)) {
    return originHeader;
  }

  return null;
}

function applyCorsHeaders(req, res) {
  const allowedOrigin = getAllowedOrigin(req.headers.origin);

  if (allowedOrigin) {
    res.setHeader("Access-Control-Allow-Origin", allowedOrigin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-cron-secret");
}

function sendJson(req, res, statusCode, payload) {
  applyCorsHeaders(req, res);
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(payload));
}

function sendRedirect(req, res, targetUrl) {
  applyCorsHeaders(req, res);
  res.statusCode = 302;
  res.setHeader("Location", targetUrl);
  res.end();
}

function sendText(req, res, statusCode, text) {
  applyCorsHeaders(req, res);
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end(text);
}

function log(message, extra = undefined) {
  const prefix = `[${new Date().toISOString()}] [api]`;
  if (extra === undefined) {
    console.log(`${prefix} ${message}`);
    return;
  }

  console.log(`${prefix} ${message}`, extra);
}

async function readJsonBody(req) {
  const chunks = [];
  let totalSize = 0;

  for await (const chunk of req) {
    totalSize += chunk.length;
    if (totalSize > 5_000_000) {
      throw new Error("Payload too large");
    }
    chunks.push(chunk);
  }

  if (chunks.length === 0) {
    return {};
  }

  const text = Buffer.concat(chunks).toString("utf8");
  if (!text.trim()) {
    return {};
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON body");
  }
}

function isDatabaseEnabled() {
  return Boolean(databasePool && databaseReady);
}

async function initializeDatabase() {
  if (!databasePool) {
    return;
  }

  try {
    await databasePool.query(`
      CREATE TABLE IF NOT EXISTS ${dbSnapshotTable} (
        id TEXT PRIMARY KEY,
        state_json JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await databasePool.query(`
      CREATE TABLE IF NOT EXISTS ${ingestedEmailsTable} (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        archive_id TEXT,
        gmail_message_id TEXT NOT NULL,
        gmail_thread_id TEXT,
        broker TEXT NOT NULL,
        sender TEXT,
        subject TEXT,
        snippet TEXT,
        body_preview TEXT,
        message_date TIMESTAMPTZ,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        attachments JSONB NOT NULL DEFAULT '[]'::jsonb,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        ai_status TEXT NOT NULL DEFAULT 'pending',
        ai_error TEXT,
        ai_extracted_at TIMESTAMPTZ,
        dedupe_checked_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (user_id, gmail_message_id)
      );
      CREATE INDEX IF NOT EXISTS ${ingestedEmailsTable}_user_ingested_idx
        ON ${ingestedEmailsTable} (user_id, ingested_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS ${ingestedEmailsTable}_user_status_idx
        ON ${ingestedEmailsTable} (user_id, ai_status, id DESC);
    `);
    await databasePool.query(`
      CREATE TABLE IF NOT EXISTS ${companyUpdatesTable} (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        email_record_id BIGINT NOT NULL REFERENCES ${ingestedEmailsTable}(id) ON DELETE CASCADE,
        archive_id TEXT,
        broker TEXT NOT NULL,
        company_raw TEXT,
        company_canonical TEXT NOT NULL,
        report_type TEXT NOT NULL,
        summary TEXT NOT NULL,
        decision TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        confidence NUMERIC(5,4),
        published_at TIMESTAMPTZ,
        is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
        duplicate_of_update_id BIGINT REFERENCES ${companyUpdatesTable}(id) ON DELETE SET NULL,
        dedupe_reason TEXT,
        dedupe_score NUMERIC(5,4),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS ${companyUpdatesTable}_user_date_idx
        ON ${companyUpdatesTable} (user_id, COALESCE(published_at, created_at) DESC, id DESC);
      CREATE INDEX IF NOT EXISTS ${companyUpdatesTable}_user_company_broker_idx
        ON ${companyUpdatesTable} (user_id, company_canonical, broker, id DESC);
      CREATE INDEX IF NOT EXISTS ${companyUpdatesTable}_user_duplicate_idx
        ON ${companyUpdatesTable} (user_id, is_duplicate, id DESC);
      CREATE INDEX IF NOT EXISTS ${companyUpdatesTable}_email_idx
        ON ${companyUpdatesTable} (email_record_id, id DESC);
    `);
    databaseReady = true;
    log("Postgres storage initialized", { table: dbSnapshotTable });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown database init error";
    log("Postgres init failed. Falling back to file storage.", { message });
    databaseReady = false;
  }
}

async function readDbFromDatabase() {
  if (!isDatabaseEnabled()) {
    return null;
  }

  const result = await databasePool.query(`SELECT state_json FROM ${dbSnapshotTable} WHERE id = $1`, [dbSnapshotId]);
  if (result.rowCount === 0) {
    return null;
  }

  return result.rows[0]?.state_json ?? null;
}

async function writeDbToDatabase(value) {
  if (!isDatabaseEnabled()) {
    return;
  }

  await databasePool.query(
    `INSERT INTO ${dbSnapshotTable} (id, state_json, updated_at)
     VALUES ($1, $2::jsonb, NOW())
     ON CONFLICT (id)
     DO UPDATE SET state_json = EXCLUDED.state_json, updated_at = NOW()`,
    [dbSnapshotId, JSON.stringify(value)]
  );
}

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeIsoTimestamp(value) {
  const text = normalizeText(value);
  if (!text) {
    return null;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function normalizeReportTypeLabel(value) {
  const normalized = normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  const map = {
    initiating_coverage: "Initiating Coverage",
    initiation: "Initiating Coverage",
    results_update: "Results Update",
    result_update: "Results Update",
    general_update: "General Update",
    sector_update: "Sector Update",
    target_price_change: "Target Price Change",
    rating_change: "Rating Change",
    management_commentary: "Management Commentary",
    corporate_action: "Corporate Action",
    other: "Other"
  };
  return map[normalized] || "Other";
}

function normalizeDecisionLabel(value) {
  const normalized = normalizeText(value).toUpperCase();
  const allowed = new Set(["BUY", "SELL", "HOLD", "ADD", "REDUCE", "UNKNOWN"]);
  if (allowed.has(normalized)) {
    return normalized;
  }
  if (!normalized) {
    return "UNKNOWN";
  }
  if (normalized.includes("BUY") || normalized.includes("OUTPERFORM") || normalized.includes("OVERWEIGHT")) {
    return "BUY";
  }
  if (normalized.includes("SELL") || normalized.includes("UNDERPERFORM") || normalized.includes("UNDERWEIGHT")) {
    return "SELL";
  }
  if (normalized.includes("HOLD") || normalized.includes("NEUTRAL")) {
    return "HOLD";
  }
  if (normalized.includes("ADD")) {
    return "ADD";
  }
  if (normalized.includes("REDUCE") || normalized.includes("TRIM")) {
    return "REDUCE";
  }
  return "UNKNOWN";
}

function normalizeSimilarityTokens(value) {
  const stopWords = new Set([
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "have",
    "has",
    "was",
    "were",
    "update",
    "report",
    "broker",
    "note",
    "company"
  ]);
  return normalizeText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 3 && !stopWords.has(token));
}

function jaccardSimilarity(leftText, rightText) {
  const leftSet = new Set(normalizeSimilarityTokens(leftText));
  const rightSet = new Set(normalizeSimilarityTokens(rightText));
  if (leftSet.size === 0 || rightSet.size === 0) {
    return 0;
  }

  let intersection = 0;
  for (const token of leftSet) {
    if (rightSet.has(token)) {
      intersection += 1;
    }
  }
  const union = leftSet.size + rightSet.size - intersection;
  return union <= 0 ? 0 : intersection / union;
}

function toSafeNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

async function executePipelineDbQuery(queryText, values = []) {
  if (!isDatabaseEnabled()) {
    throw new Error("Postgres database is required for DB-first pipeline.");
  }
  return databasePool.query(queryText, values);
}

async function upsertIngestedEmailRecord(user, item) {
  const messageDate = normalizeIsoTimestamp(item.dateHeader) || normalizeIsoTimestamp(item.ingestedAt);
  const ingestedAt = normalizeIsoTimestamp(item.ingestedAt) || new Date().toISOString();
  const attachments = Array.isArray(item.attachments) ? item.attachments : [];
  const rawPayload = {
    archiveId: item.id,
    gmailMessageUrl: item.gmailMessageUrl || "",
    duplicateOfArchiveId: item.duplicateOfArchiveId || null
  };

  const result = await executePipelineDbQuery(
    `INSERT INTO ${ingestedEmailsTable} (
      user_id, archive_id, gmail_message_id, gmail_thread_id, broker, sender, subject, snippet, body_preview,
      message_date, ingested_at, attachments, raw_payload, ai_status, ai_error, ai_extracted_at, dedupe_checked_at, updated_at
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9,
      $10, $11, $12::jsonb, $13::jsonb, 'pending', NULL, NULL, NULL, NOW()
    )
    ON CONFLICT (user_id, gmail_message_id)
    DO UPDATE SET
      archive_id = EXCLUDED.archive_id,
      gmail_thread_id = EXCLUDED.gmail_thread_id,
      broker = EXCLUDED.broker,
      sender = EXCLUDED.sender,
      subject = EXCLUDED.subject,
      snippet = EXCLUDED.snippet,
      body_preview = EXCLUDED.body_preview,
      message_date = EXCLUDED.message_date,
      ingested_at = EXCLUDED.ingested_at,
      attachments = EXCLUDED.attachments,
      raw_payload = EXCLUDED.raw_payload,
      ai_status = 'pending',
      ai_error = NULL,
      ai_extracted_at = NULL,
      dedupe_checked_at = NULL,
      updated_at = NOW()
    RETURNING id, archive_id, gmail_message_id, gmail_thread_id, broker, sender, subject, snippet, body_preview, message_date, ingested_at, attachments, raw_payload`,
    [
      user.id,
      item.id || null,
      item.gmailMessageId || "",
      item.gmailThreadId || "",
      item.broker || "Unmapped Broker",
      item.from || "",
      item.subject || "",
      item.snippet || "",
      item.bodyPreview || "",
      messageDate,
      ingestedAt,
      JSON.stringify(attachments),
      JSON.stringify(rawPayload)
    ]
  );
  return result.rows[0] || null;
}

function toPipelineArchiveItem(record) {
  const archive = record && typeof record === "object" ? record : {};
  return {
    id: String(archive.id ?? ""),
    gmailMessageId: String(archive.gmailMessageId ?? ""),
    gmailThreadId: String(archive.gmailThreadId ?? ""),
    broker: String(archive.broker ?? "Unmapped Broker"),
    from: String(archive.from ?? ""),
    subject: String(archive.subject ?? ""),
    snippet: String(archive.snippet ?? ""),
    bodyPreview: String(archive.bodyPreview ?? ""),
    dateHeader: String(archive.dateHeader ?? ""),
    ingestedAt: String(archive.ingestedAt ?? ""),
    attachments: Array.isArray(archive.attachments)
      ? archive.attachments.map((entry) => ({
          filename: String(entry?.filename ?? ""),
          mimeType: String(entry?.mimeType ?? ""),
          sizeBytes: Number(entry?.sizeBytes ?? 0)
        }))
      : [],
    gmailMessageUrl: String(archive.gmailMessageUrl ?? ""),
    duplicateOfArchiveId: archive.duplicateOfArchiveId ?? null
  };
}

async function filterRecordsNeedingCompanyPipeline(userId, records, options = {}) {
  const rows = Array.isArray(records) ? records.filter(Boolean) : [];
  if (rows.length === 0) {
    return [];
  }

  const cap = Math.max(1, Math.min(200, Number(options.limit) || 25));
  const includeCompleted = options.includeCompleted === true;
  if (!isDatabaseEnabled()) {
    return rows.slice(0, cap);
  }

  const messageIds = [...new Set(rows.map((entry) => normalizeText(entry.gmailMessageId)).filter(Boolean))];
  if (messageIds.length === 0) {
    return rows.slice(0, cap);
  }

  const statusQuery = await executePipelineDbQuery(
    `SELECT gmail_message_id, ai_status
       FROM ${ingestedEmailsTable}
      WHERE user_id = $1
        AND gmail_message_id = ANY($2::text[])`,
    [userId, messageIds]
  );

  const statusMap = new Map(
    (statusQuery.rows || []).map((entry) => [normalizeText(entry.gmail_message_id), normalizeText(entry.ai_status).toLowerCase()])
  );

  return rows
    .filter((entry) => {
      const id = normalizeText(entry.gmailMessageId);
      if (!id) {
        return false;
      }
      const status = statusMap.get(id) || "";
      return includeCompleted ? true : status !== "completed";
    })
    .slice(0, cap);
}

async function setIngestedEmailAiStatus(userId, emailRecordId, status, errorMessage = null) {
  await executePipelineDbQuery(
    `UPDATE ${ingestedEmailsTable}
       SET ai_status = $3,
           ai_error = $4,
           ai_extracted_at = CASE WHEN $3 = 'completed' THEN NOW() ELSE ai_extracted_at END,
           updated_at = NOW()
     WHERE user_id = $1 AND id = $2`,
    [userId, emailRecordId, status, errorMessage]
  );
}

async function replaceCompanyUpdatesForEmail(userId, emailRecord, extractedRecords, extractionSource, rawResponse) {
  await executePipelineDbQuery(`DELETE FROM ${companyUpdatesTable} WHERE user_id = $1 AND email_record_id = $2`, [
    userId,
    emailRecord.id
  ]);

  if (!Array.isArray(extractedRecords) || extractedRecords.length === 0) {
    return { inserted: 0 };
  }

  let inserted = 0;
  for (const record of extractedRecords) {
    const payload = {
      extractionSource,
      rawModelOutput: record.sourcePayload || {},
      modelEnvelope: rawResponse && typeof rawResponse === "object" ? rawResponse : {}
    };
    await executePipelineDbQuery(
      `INSERT INTO ${companyUpdatesTable} (
        user_id, email_record_id, archive_id, broker, company_raw, company_canonical, report_type,
        summary, decision, payload, confidence, published_at, is_duplicate, duplicate_of_update_id,
        dedupe_reason, dedupe_score, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10::jsonb, $11, $12, FALSE, NULL,
        NULL, NULL, NOW(), NOW()
      )`,
      [
        userId,
        emailRecord.id,
        emailRecord.archive_id || null,
        normalizeText(record.broker || emailRecord.broker || "Unmapped Broker"),
        normalizeText(record.companyRaw || "Unknown Company"),
        normalizeText(record.companyCanonical || "Unknown Company"),
        normalizeReportTypeLabel(record.reportType),
        normalizeText(record.summary || "No summary available."),
        normalizeDecisionLabel(record.decision),
        JSON.stringify(payload),
        toSafeNumber(record.confidence, 0.45),
        normalizeIsoTimestamp(record.publishedAt || emailRecord.message_date || emailRecord.ingested_at)
      ]
    );
    inserted += 1;
  }

  return { inserted };
}

async function runCompanyUpdatesDedupeForUser(userId) {
  const dedupeWindowDays = 14;
  const query = await executePipelineDbQuery(
    `SELECT id, broker, company_canonical, report_type, summary, published_at, created_at
       FROM ${companyUpdatesTable}
      WHERE user_id = $1
      ORDER BY COALESCE(published_at, created_at) ASC, id ASC`,
    [userId]
  );
  const rows = Array.isArray(query.rows) ? query.rows : [];

  await executePipelineDbQuery(
    `UPDATE ${companyUpdatesTable}
        SET is_duplicate = FALSE,
            duplicate_of_update_id = NULL,
            dedupe_reason = NULL,
            dedupe_score = NULL,
            updated_at = NOW()
      WHERE user_id = $1`,
    [userId]
  );

  const canonicalByGroup = new Map();
  let markedDuplicates = 0;

  for (const row of rows) {
    const broker = normalizeText(row.broker).toLowerCase();
    const company = normalizeText(row.company_canonical).toLowerCase();
    const groupKey = `${broker}|${company}`;
    const candidates = canonicalByGroup.get(groupKey) || [];
    const rowEpoch = new Date(row.published_at || row.created_at || 0).getTime();
    let duplicateOf = null;
    let duplicateScore = 0;
    let duplicateReason = "";

    for (let index = candidates.length - 1; index >= 0; index -= 1) {
      const candidate = candidates[index];
      const candidateEpoch = new Date(candidate.published_at || candidate.created_at || 0).getTime();
      const dayDistance = Math.abs(rowEpoch - candidateEpoch) / (24 * 60 * 60 * 1000);
      if (dayDistance > dedupeWindowDays) {
        continue;
      }

      const sameType = normalizeText(row.report_type).toLowerCase() === normalizeText(candidate.report_type).toLowerCase();
      if (!sameType) {
        continue;
      }

      const rowSummary = normalizeText(row.summary);
      const candidateSummary = normalizeText(candidate.summary);
      const shortestSummary = Math.min(rowSummary.length, candidateSummary.length);
      const score = jaccardSimilarity(rowSummary, candidateSummary);
      const requiredScore = shortestSummary < 120 ? 0.9 : 0.82;
      const isDuplicate = score >= requiredScore;
      if (isDuplicate) {
        duplicateOf = candidate.id;
        duplicateScore = score;
        duplicateReason = shortestSummary < 120 ? "similar_update_same_type_short_text" : "similar_update_same_type";
        break;
      }
    }

    if (duplicateOf) {
      markedDuplicates += 1;
      await executePipelineDbQuery(
        `UPDATE ${companyUpdatesTable}
            SET is_duplicate = TRUE,
                duplicate_of_update_id = $3,
                dedupe_reason = $4,
                dedupe_score = $5,
                updated_at = NOW()
          WHERE user_id = $1 AND id = $2`,
        [userId, row.id, duplicateOf, duplicateReason, duplicateScore]
      );
      continue;
    }

    candidates.push(row);
    canonicalByGroup.set(groupKey, candidates);
  }

  await executePipelineDbQuery(
    `UPDATE ${ingestedEmailsTable}
        SET dedupe_checked_at = NOW(),
            updated_at = NOW()
      WHERE user_id = $1`,
    [userId]
  );

  return {
    totalRows: rows.length,
    markedDuplicates
  };
}

async function runCompanyUpdatePipelineForIngestedRecords(user, ingestedRecords) {
  const rows = Array.isArray(ingestedRecords) ? ingestedRecords.filter(Boolean) : [];
  if (rows.length === 0) {
    return {
      ingestedEmails: 0,
      aiProcessedEmails: 0,
      aiFailedEmails: 0,
      aiFailureSamples: [],
      extractedCompanyUpdates: 0,
      duplicatesMarked: 0,
      source: companyUpdateExtractor.source
    };
  }

  const companyDictionary = Array.from(
    new Set(
      rows
        .map((entry) => normalizeText(entry.subject))
        .filter(Boolean)
    )
  );

  let aiProcessedEmails = 0;
  let aiFailedEmails = 0;
  let extractedCompanyUpdates = 0;
  const aiFailureSamples = [];

  for (const emailRecord of rows) {
    await setIngestedEmailAiStatus(user.id, emailRecord.id, "processing", null);
    try {
      const extraction = await companyUpdateExtractor.extractEmailToCompanies({
        userId: user.id,
        emailRecord: {
          id: emailRecord.id,
          archiveId: emailRecord.archive_id,
          broker: emailRecord.broker,
          sender: emailRecord.sender,
          subject: emailRecord.subject,
          snippet: emailRecord.snippet,
          bodyPreview: emailRecord.body_preview,
          messageDate: emailRecord.message_date,
          ingestedAt: emailRecord.ingested_at,
          attachments: Array.isArray(emailRecord.attachments) ? emailRecord.attachments : []
        },
        companyDictionary
      });
      const normalizedReports = Array.isArray(extraction?.reports)
        ? extraction.reports.map((entry) => ({ ...entry, broker: emailRecord.broker }))
        : [];
      const extractionSource = normalizeText(extraction?.source || companyUpdateExtractor.source);
      const replaceResult = await replaceCompanyUpdatesForEmail(
        user.id,
        emailRecord,
        normalizedReports,
        extractionSource,
        extraction?.rawResponse || {}
      );
      extractedCompanyUpdates += replaceResult.inserted;
      aiProcessedEmails += 1;
      await setIngestedEmailAiStatus(user.id, emailRecord.id, "completed", null);
    } catch (error) {
      aiFailedEmails += 1;
      const message = error instanceof Error ? error.message.slice(0, 600) : "AI extraction failed";
      if (aiFailureSamples.length < 3) {
        aiFailureSamples.push({
          emailRecordId: String(emailRecord.id),
          archiveId: normalizeText(emailRecord.archive_id),
          broker: normalizeText(emailRecord.broker),
          subject: normalizeText(emailRecord.subject).slice(0, 120),
          error: message
        });
      }
      await setIngestedEmailAiStatus(
        user.id,
        emailRecord.id,
        "failed",
        message
      );
    }
  }

  const dedupeSummary = await runCompanyUpdatesDedupeForUser(user.id);
  return {
    ingestedEmails: rows.length,
    aiProcessedEmails,
    aiFailedEmails,
    aiFailureSamples,
    extractedCompanyUpdates,
    duplicatesMarked: dedupeSummary.markedDuplicates,
    source: companyUpdateExtractor.source
  };
}

function normalizeGmailPreferencesEntry(entry) {
  const nowIso = new Date().toISOString();
  const scheduleHour = Number.parseInt(String(entry?.scheduleHour ?? "7"), 10);
  const scheduleMinute = Number.parseInt(String(entry?.scheduleMinute ?? "30"), 10);
  const ingestFromDate = normalizeDateOnly(entry?.ingestFromDate);
  const ingestToDate = normalizeDateOnly(entry?.ingestToDate);
  const hasInvalidDateRange = Boolean(ingestFromDate && ingestToDate && ingestFromDate > ingestToDate);

  return {
    userId: String(entry?.userId ?? ""),
    maxResults: Math.max(1, Math.min(100, Number(entry?.maxResults ?? 25) || 25)),
    query: typeof entry?.query === "string" ? entry.query : "",
    includeUnmapped: entry?.includeUnmapped === true,
    brokerMappings: mergeBrokerMappingsWithDefaults(entry?.brokerMappings),
    trackedLabelIds: Array.isArray(entry?.trackedLabelIds)
      ? entry.trackedLabelIds.map((value) => String(value).trim()).filter(Boolean)
      : [],
    trackedLabelNames: Array.isArray(entry?.trackedLabelNames)
      ? entry.trackedLabelNames.map((value) => String(value).trim()).filter(Boolean)
      : [],
    scheduleEnabled: entry?.scheduleEnabled === true,
    scheduleHour: Number.isInteger(scheduleHour) ? Math.max(0, Math.min(23, scheduleHour)) : 7,
    scheduleMinute: Number.isInteger(scheduleMinute) ? Math.max(0, Math.min(59, scheduleMinute)) : 30,
    scheduleTimezone: typeof entry?.scheduleTimezone === "string" ? entry.scheduleTimezone : "Asia/Kolkata",
    ingestFromDate: hasInvalidDateRange ? null : ingestFromDate,
    ingestToDate: hasInvalidDateRange ? null : ingestToDate,
    lastIngestAfterEpoch: Math.max(0, Number.parseInt(String(entry?.lastIngestAfterEpoch ?? "0"), 10) || 0),
    lastScheduledRunDate:
      typeof entry?.lastScheduledRunDate === "string" && entry.lastScheduledRunDate.trim()
        ? entry.lastScheduledRunDate.trim()
        : null,
    lastIngestAt:
      typeof entry?.lastIngestAt === "string" && entry.lastIngestAt.trim() ? entry.lastIngestAt.trim() : null,
    updatedAt: typeof entry?.updatedAt === "string" ? entry.updatedAt : nowIso
  };
}

function mergeBrokerMappingsWithDefaults(mappings) {
  const normalized = Array.isArray(mappings)
    ? mappings
        .filter((entry) => entry && typeof entry.broker === "string")
        .map((entry) => ({
          broker: entry.broker.trim() || "Unnamed Broker",
          patterns: Array.isArray(entry.patterns)
            ? entry.patterns.map((pattern) => String(pattern).trim()).filter(Boolean)
            : [],
          enabled: entry.enabled !== false
        }))
        .filter((entry) => entry.patterns.length > 0)
    : [];

  if (normalized.length === 0) {
    return structuredClone(defaultBrokerMappings);
  }

  const knownBrokers = new Set(normalized.map((entry) => entry.broker.toLowerCase()));
  for (const fallback of defaultBrokerMappings) {
    if (!knownBrokers.has(fallback.broker.toLowerCase())) {
      normalized.push(structuredClone(fallback));
    }
  }

  return normalized;
}

function mergeDbDefaults(value) {
  return {
    ...structuredClone(defaultDb),
    ...value,
    counters: {
      ...structuredClone(defaultDb.counters),
      ...(value.counters ?? {})
    },
    cronState: {
      ...structuredClone(defaultDb.cronState),
      ...(value.cronState ?? {})
    },
    users: Array.isArray(value.users) ? value.users : [],
    sessions: Array.isArray(value.sessions) ? value.sessions : [],
    gmailConnections: Array.isArray(value.gmailConnections) ? value.gmailConnections : [],
    gmailPreferences: Array.isArray(value.gmailPreferences)
      ? value.gmailPreferences.map((entry) => normalizeGmailPreferencesEntry(entry))
      : [],
    emailArchives: Array.isArray(value.emailArchives) ? value.emailArchives : [],
    extractionRuns: Array.isArray(value.extractionRuns) ? value.extractionRuns : [],
    extractedReports: Array.isArray(value.extractedReports) ? value.extractedReports : [],
    notes: Array.isArray(value.notes) ? value.notes : []
  };
}

async function loadDb() {
  await mkdir(dataDir, { recursive: true });

  if (isDatabaseEnabled()) {
    try {
      const dbState = await readDbFromDatabase();
      if (dbState) {
        log("Loaded brokerage state from Postgres");
        return mergeDbDefaults(dbState);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown DB read error";
      log("Failed reading brokerage state from Postgres. Falling back to file.", { message });
    }
  }

  try {
    const raw = await readFile(dbFilePath, "utf8");
    const parsed = JSON.parse(raw);
    const merged = mergeDbDefaults(parsed);
    if (isDatabaseEnabled()) {
      await writeDbToDatabase(merged);
      log("Backfilled brokerage state to Postgres from file snapshot");
    }
    return merged;
  } catch {
    const fresh = structuredClone(defaultDb);
    await writeJsonFile(dbFilePath, fresh);
    if (isDatabaseEnabled()) {
      await writeDbToDatabase(fresh);
    }
    return fresh;
  }
}

function persistDb() {
  persistQueue = persistQueue
    .then(async () => {
      if (isDatabaseEnabled()) {
        await writeDbToDatabase(db);
      }
      await writeJsonFile(dbFilePath, db);
    })
    .catch((error) => {
      log("Failed to persist db", { message: error instanceof Error ? error.message : String(error) });
    });

  return persistQueue;
}

async function writeJsonFile(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.${randomBytes(4).toString("hex")}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tempPath, filePath);
}

function deriveKey(secret) {
  return createHash("sha256").update(secret).digest();
}

function base64UrlEncode(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value), "utf8");
  return buffer
    .toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function base64UrlDecode(value) {
  const padding = (4 - (value.length % 4)) % 4;
  const normalized = `${value}${"=".repeat(padding)}`.replaceAll("-", "+").replaceAll("_", "/");
  return Buffer.from(normalized, "base64");
}

function signPayload(payload) {
  const header = { alg: "HS256", typ: "JWT" };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const data = `${encodedHeader}.${encodedPayload}`;
  const signature = createHmac("sha256", authSecret).update(data).digest();
  return `${data}.${base64UrlEncode(signature)}`;
}

function verifySignedPayload(token) {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return null;
  }

  const [encodedHeader, encodedPayload, encodedSignature] = parts;
  const data = `${encodedHeader}.${encodedPayload}`;
  const expectedSignature = createHmac("sha256", authSecret).update(data).digest();
  const providedSignature = base64UrlDecode(encodedSignature);

  if (expectedSignature.length !== providedSignature.length) {
    return null;
  }

  if (!timingSafeEqual(expectedSignature, providedSignature)) {
    return null;
  }

  try {
    const payload = JSON.parse(base64UrlDecode(encodedPayload).toString("utf8"));
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

function encryptValue(value) {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", encryptionKey, iv);
  const encrypted = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${base64UrlEncode(iv)}.${base64UrlEncode(tag)}.${base64UrlEncode(encrypted)}`;
}

function decryptValue(encoded) {
  const [ivRaw, tagRaw, dataRaw] = encoded.split(".");
  if (!ivRaw || !tagRaw || !dataRaw) {
    throw new Error("Invalid encrypted payload format");
  }

  const iv = base64UrlDecode(ivRaw);
  const tag = base64UrlDecode(tagRaw);
  const encryptedData = base64UrlDecode(dataRaw);
  const decipher = createDecipheriv("aes-256-gcm", encryptionKey, iv);
  decipher.setAuthTag(tag);

  const decrypted = Buffer.concat([decipher.update(encryptedData), decipher.final()]);
  return decrypted.toString("utf8");
}

function newId(prefix) {
  return `${prefix}_${randomBytes(10).toString("hex")}`;
}

function getBearerToken(req) {
  const header = req.headers.authorization;
  if (!header) {
    return null;
  }

  const [scheme, token] = header.split(" ");
  if (scheme !== "Bearer" || !token) {
    return null;
  }

  return token;
}

function getSessionForRequest(req) {
  const token = getBearerToken(req);
  if (!token) {
    return null;
  }

  const payload = verifySignedPayload(token);
  if (!payload || payload.typ !== "session") {
    return null;
  }

  const session = db.sessions.find((item) => item.id === payload.sid && item.userId === payload.uid);
  if (!session) {
    return null;
  }

  if (session.revokedAt) {
    return null;
  }

  if (session.expiresAt && new Date(session.expiresAt).getTime() < Date.now()) {
    return null;
  }

  return {
    session,
    token,
    payload
  };
}

function getUserById(userId) {
  return db.users.find((user) => user.id === userId) || null;
}

function getGoogleConnection(userId) {
  return db.gmailConnections.find((connection) => connection.userId === userId) || null;
}

function getOrCreateGmailPreferences(userId) {
  let prefs = db.gmailPreferences.find((entry) => entry.userId === userId);
  if (!prefs) {
    const nowEpoch = Math.floor(Date.now() / 1000);
    prefs = {
      userId,
      maxResults: 25,
      query: "",
      includeUnmapped: false,
      brokerMappings: structuredClone(defaultBrokerMappings),
      trackedLabelIds: [],
      trackedLabelNames: [],
      scheduleEnabled: false,
      scheduleHour: 7,
      scheduleMinute: 30,
      scheduleTimezone: "Asia/Kolkata",
      ingestFromDate: null,
      ingestToDate: null,
      lastIngestAfterEpoch: nowEpoch,
      lastScheduledRunDate: null,
      lastIngestAt: null,
      updatedAt: new Date().toISOString()
    };
    db.gmailPreferences.push(prefs);
  } else {
    Object.assign(prefs, normalizeGmailPreferencesEntry(prefs));
  }

  return prefs;
}

function requireAuth(req, res) {
  const authContext = getSessionForRequest(req);
  if (!authContext) {
    sendJson(req, res, 401, { error: "Unauthorized" });
    return null;
  }

  const user = getUserById(authContext.payload.uid);
  if (!user) {
    sendJson(req, res, 401, { error: "Session user missing" });
    return null;
  }

  authContext.session.lastSeenAt = new Date().toISOString();
  persistDb();

  return {
    user,
    session: authContext.session,
    token: authContext.token
  };
}

function normalizeRedirect(value) {
  try {
    const url = new URL(value);
    return `${url.origin}${url.pathname}`.replace(/\/$/, "") || `${url.origin}/`;
  } catch {
    return null;
  }
}

function isAllowedRedirect(value) {
  const normalized = normalizeRedirect(value);
  if (!normalized) {
    return false;
  }
  return allowedRedirects.has(normalized);
}

function resolveRedirect(value) {
  if (value && isAllowedRedirect(value)) {
    return value;
  }
  return frontendUrl;
}

function isValidTimeZone(value) {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function resolveTimeZone(value) {
  const trimmed = String(value ?? "").trim();
  if (trimmed && isValidTimeZone(trimmed)) {
    return trimmed;
  }
  return "Asia/Kolkata";
}

function getZonedDateParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: resolveTimeZone(timeZone),
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
  const partMap = Object.fromEntries(
    formatter
      .formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );

  return {
    dateKey: `${partMap.year}-${partMap.month}-${partMap.day}`,
    hour: Number.parseInt(partMap.hour ?? "0", 10),
    minute: Number.parseInt(partMap.minute ?? "0", 10)
  };
}

function isScheduleDueNow(prefs, now = new Date()) {
  if (!prefs?.scheduleEnabled) {
    return false;
  }

  const parts = getZonedDateParts(now, prefs.scheduleTimezone);
  const scheduleHour = Number.parseInt(String(prefs.scheduleHour ?? 0), 10);
  const scheduleMinute = Number.parseInt(String(prefs.scheduleMinute ?? 0), 10);
  const dueTimeReached = parts.hour > scheduleHour || (parts.hour === scheduleHour && parts.minute >= scheduleMinute);
  if (!dueTimeReached) {
    return false;
  }

  return prefs.lastScheduledRunDate !== parts.dateKey;
}

function normalizeDateOnly(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return null;
  }

  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }

  const utcTime = Date.UTC(year, month - 1, day);
  const date = new Date(utcTime);
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

function dateOnlyToEpochStart(dateOnlyValue) {
  const normalized = normalizeDateOnly(dateOnlyValue);
  if (!normalized) {
    return null;
  }
  return Math.floor(new Date(`${normalized}T00:00:00.000Z`).getTime() / 1000);
}

function dateOnlyToEpochEndExclusive(dateOnlyValue) {
  const startEpoch = dateOnlyToEpochStart(dateOnlyValue);
  if (!Number.isFinite(startEpoch)) {
    return null;
  }
  return startEpoch + 24 * 60 * 60;
}

function composeIngestQuery(baseQuery, afterEpoch, ingestFromDate = null, ingestToDate = null) {
  const chunks = [];
  if (typeof baseQuery === "string" && baseQuery.trim()) {
    chunks.push(baseQuery.trim());
  }

  const fromEpoch = dateOnlyToEpochStart(ingestFromDate);
  const beforeEpoch = dateOnlyToEpochEndExclusive(ingestToDate);
  const hasExplicitDateBoundary = Number.isFinite(fromEpoch) || Number.isFinite(beforeEpoch);
  // If user provides any date boundary, prefer that over moving cursor to avoid contradictory queries.
  let effectiveAfterEpoch = 0;
  if (hasExplicitDateBoundary) {
    effectiveAfterEpoch = Number.isFinite(fromEpoch) ? Math.floor(fromEpoch) : 0;
  } else if (Number.isFinite(afterEpoch) && afterEpoch > 0) {
    effectiveAfterEpoch = Math.floor(afterEpoch);
  }

  if (effectiveAfterEpoch > 0) {
    chunks.push(`after:${effectiveAfterEpoch}`);
  }

  if (Number.isFinite(beforeEpoch) && beforeEpoch > 0) {
    chunks.push(`before:${beforeEpoch}`);
  }

  return chunks.join(" ").trim();
}

function sanitizeLabelList(values) {
  if (!Array.isArray(values)) {
    return [];
  }

  return values
    .map((value) => String(value).trim())
    .filter(Boolean)
    .slice(0, 100);
}

function isGoogleConfigured() {
  return Boolean(googleClientId && googleClientSecret && googleRedirectUri);
}

function createOAuthStateToken(redirectUri) {
  const nowSec = Math.floor(Date.now() / 1000);
  return signPayload({
    typ: "oauth_state",
    redirectUri,
    nonce: randomBytes(12).toString("hex"),
    iat: nowSec,
    exp: nowSec + 10 * 60
  });
}

function createSessionToken(userId, sessionId) {
  const nowSec = Math.floor(Date.now() / 1000);
  return signPayload({
    typ: "session",
    uid: userId,
    sid: sessionId,
    iat: nowSec,
    exp: nowSec + 7 * 24 * 60 * 60
  });
}

function createShareToken(params) {
  const nowSec = Math.floor(Date.now() / 1000);
  const expiresHours = Number.isFinite(params.expiresHours) ? params.expiresHours : 24;

  return signPayload({
    typ: "share_file",
    aid: params.archiveId,
    kind: params.kind,
    idx: params.index,
    iat: nowSec,
    exp: nowSec + Math.max(1, Math.min(168, expiresHours)) * 60 * 60
  });
}

async function exchangeAuthCodeForTokens(code) {
  const body = new URLSearchParams({
    client_id: googleClientId,
    client_secret: googleClientSecret,
    code,
    grant_type: "authorization_code",
    redirect_uri: googleRedirectUri
  });

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(`Google token exchange failed (${response.status}): ${data.error ?? "unknown"}`);
  }

  return data;
}

async function refreshGoogleTokens(refreshToken) {
  const body = new URLSearchParams({
    client_id: googleClientId,
    client_secret: googleClientSecret,
    refresh_token: refreshToken,
    grant_type: "refresh_token"
  });

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(`Google token refresh failed (${response.status}): ${data.error ?? "unknown"}`);
  }

  return data;
}

async function fetchGoogleUserInfo(accessToken) {
  const response = await fetch("https://openidconnect.googleapis.com/v1/userinfo", {
    headers: {
      Authorization: `Bearer ${accessToken}`
    }
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(`Google userinfo failed (${response.status})`);
  }

  return data;
}

function upsertUserFromGoogle(userInfo) {
  const now = new Date().toISOString();
  let user = db.users.find((entry) => entry.googleSub === userInfo.sub || entry.email === userInfo.email);

  if (!user) {
    user = {
      id: newId("user"),
      provider: "google",
      googleSub: userInfo.sub,
      email: userInfo.email,
      name: userInfo.name ?? userInfo.email,
      picture: userInfo.picture ?? null,
      createdAt: now,
      updatedAt: now
    };

    db.users.push(user);
  } else {
    user.googleSub = userInfo.sub;
    user.email = userInfo.email;
    user.name = userInfo.name ?? user.name;
    user.picture = userInfo.picture ?? user.picture;
    user.updatedAt = now;
  }

  return user;
}

function upsertGoogleConnection(user, tokenData) {
  const now = new Date().toISOString();
  const expiresAt = new Date(Date.now() + Number(tokenData.expires_in ?? 3600) * 1000).toISOString();
  let connection = db.gmailConnections.find((entry) => entry.userId === user.id);

  const refreshTokenEncrypted = tokenData.refresh_token
    ? encryptValue(tokenData.refresh_token)
    : connection?.refreshTokenEncrypted ?? null;

  if (!connection) {
    connection = {
      id: newId("conn"),
      userId: user.id,
      email: user.email,
      googleSub: user.googleSub,
      scope: tokenData.scope ?? googleScopes,
      accessTokenEncrypted: encryptValue(tokenData.access_token),
      refreshTokenEncrypted,
      expiresAt,
      tokenUpdatedAt: now,
      createdAt: now
    };
    db.gmailConnections.push(connection);
  } else {
    connection.email = user.email;
    connection.googleSub = user.googleSub;
    connection.scope = tokenData.scope ?? connection.scope;
    connection.accessTokenEncrypted = encryptValue(tokenData.access_token);
    connection.refreshTokenEncrypted = refreshTokenEncrypted;
    connection.expiresAt = expiresAt;
    connection.tokenUpdatedAt = now;
  }

  return connection;
}

function createSessionForUser(userId) {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
  const sessionId = newId("sess");

  db.sessions.push({
    id: sessionId,
    userId,
    createdAt: now.toISOString(),
    expiresAt: expiresAt.toISOString(),
    lastSeenAt: now.toISOString(),
    revokedAt: null
  });

  return {
    sessionId,
    token: createSessionToken(userId, sessionId)
  };
}

async function getValidGoogleAccessToken(connection) {
  const expiresAt = new Date(connection.expiresAt).getTime();
  if (expiresAt - Date.now() > 60_000) {
    return decryptValue(connection.accessTokenEncrypted);
  }

  if (!connection.refreshTokenEncrypted) {
    throw new Error("Google refresh token not available. Reconnect Gmail.");
  }

  const refreshToken = decryptValue(connection.refreshTokenEncrypted);
  const refreshed = await refreshGoogleTokens(refreshToken);

  connection.accessTokenEncrypted = encryptValue(refreshed.access_token);
  connection.expiresAt = new Date(Date.now() + Number(refreshed.expires_in ?? 3600) * 1000).toISOString();
  connection.tokenUpdatedAt = new Date().toISOString();
  if (refreshed.scope) {
    connection.scope = refreshed.scope;
  }

  await persistDb();
  return refreshed.access_token;
}

function parseFromHeader(rawFrom) {
  const from = rawFrom ?? "";
  const match = from.match(/<([^>]+)>/);
  const email = (match?.[1] ?? from).trim().toLowerCase();
  const name = from.replace(/<[^>]+>/, "").replace(/"/g, "").trim() || email;
  return { raw: from, email, name };
}

function detectBroker(fromHeader, brokerMappings) {
  const normalized = fromHeader.toLowerCase();
  for (const mapping of brokerMappings) {
    if (!mapping.enabled) {
      continue;
    }

    if (mapping.patterns.some((pattern) => normalized.includes(pattern.toLowerCase().trim()))) {
      return mapping.broker;
    }
  }

  return "Unmapped Broker";
}

async function gmailApiRequest(accessToken, endpoint, options = {}) {
  const url = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`);
  const query = options.query;
  if (query && typeof query === "object") {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === "") {
        continue;
      }

      if (Array.isArray(value)) {
        for (const entry of value) {
          if (entry === undefined || entry === null || entry === "") {
            continue;
          }
          url.searchParams.append(key, String(entry));
        }
        continue;
      }

      url.searchParams.set(key, String(value));
    }
  }

  const method = options.method || "GET";
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    ...(options.headers || {})
  };
  const request = {
    method,
    headers
  };

  if (options.body !== undefined) {
    request.body = JSON.stringify(options.body);
    if (!request.headers["Content-Type"]) {
      request.headers["Content-Type"] = "application/json";
    }
  }

  const response = await fetch(url, request);

  const data = await response.json();
  if (!response.ok) {
    throw new Error(`Gmail API failed (${response.status}) at ${endpoint}: ${data.error?.message ?? "unknown"}`);
  }

  return data;
}

async function gmailApiGet(accessToken, endpoint, query = undefined) {
  return gmailApiRequest(accessToken, endpoint, {
    method: "GET",
    query
  });
}

function getHeader(headers, name) {
  const lower = name.toLowerCase();
  for (const header of headers ?? []) {
    if (String(header.name ?? "").toLowerCase() === lower) {
      return String(header.value ?? "");
    }
  }
  return "";
}

function walkParts(part, callback) {
  callback(part);
  for (const child of part.parts ?? []) {
    walkParts(child, callback);
  }
}

function decodeGoogleBase64(rawData) {
  return base64UrlDecode(String(rawData ?? ""));
}

function extractBodyPreview(payload, fallbackSnippet) {
  let selected = "";

  walkParts(payload, (part) => {
    if (selected) {
      return;
    }

    const mimeType = String(part.mimeType ?? "").toLowerCase();
    const data = part.body?.data;
    if (!data) {
      return;
    }

    if (mimeType === "text/plain") {
      selected = decodeGoogleBase64(data).toString("utf8");
      return;
    }

    if (mimeType === "text/html") {
      const html = decodeGoogleBase64(data).toString("utf8");
      selected = stripHtmlTags(html);
    }
  });

  return (selected || fallbackSnippet || "").replace(/\s+/g, " ").trim().slice(0, 4000);
}

function stripHtmlTags(value) {
  return value
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function extractAttachmentParts(payload) {
  const attachments = [];

  walkParts(payload, (part) => {
    const attachmentId = part.body?.attachmentId;
    if (!attachmentId) {
      return;
    }

    const filename = String(part.filename ?? "").trim();
    attachments.push({
      attachmentId,
      filename,
      mimeType: String(part.mimeType ?? "application/octet-stream"),
      size: Number(part.body?.size ?? 0)
    });
  });

  return attachments;
}

function sanitizeFileName(value) {
  const cleaned = value.replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+|_+$/g, "");
  return cleaned || "file";
}

function makeArchivePublicRecord(record) {
  const sender = parseFromHeader(record.from);
  const redactionOptions = {
    senderName: sender.name,
    senderEmail: sender.email
  };

  return {
    id: record.id,
    broker: record.broker,
    from: record.broker || redactPiiText(record.from, redactionOptions),
    subject: redactPiiText(record.subject, redactionOptions),
    snippet: redactPiiText(record.snippet, redactionOptions),
    bodyPreview: redactPiiText(record.bodyPreview, redactionOptions),
    dateHeader: record.dateHeader,
    ingestedAt: record.ingestedAt,
    gmailMessageId: record.gmailMessageId,
    gmailThreadId: record.gmailThreadId,
    gmailMessageUrl: record.gmailMessageUrl,
    duplicateOfArchiveId: record.duplicateOfArchiveId ?? null,
    downloadUrl: `/api/email-archives/${record.id}/raw`,
    attachments: (record.attachments ?? []).map((attachment, index) => ({
      index,
      filename: attachment.filename,
      mimeType: attachment.mimeType,
      sizeBytes: attachment.sizeBytes,
      downloadUrl: `/api/email-archives/${record.id}/attachments/${index}`
    }))
  };
}

async function archiveGmailMessage(user, gmailMessage, rawData, includeAttachments, accessToken, broker) {
  const now = new Date();
  const nowIso = now.toISOString();
  const gmailMessageId = String(gmailMessage.id ?? "");

  const existing = db.emailArchives.find(
    (entry) => entry.userId === user.id && String(entry.gmailMessageId) === gmailMessageId
  );
  if (existing) {
    return {
      archived: false,
      skippedReason: "already_archived",
      record: existing
    };
  }

  const payload = gmailMessage.payload ?? {};
  const from = getHeader(payload.headers, "From");
  const subject = getHeader(payload.headers, "Subject") || "(No Subject)";
  const dateHeader = getHeader(payload.headers, "Date");
  const messageIdHeader = getHeader(payload.headers, "Message-ID");
  const bodyPreview = extractBodyPreview(payload, gmailMessage.snippet ?? "");

  const archiveId = newId("arc");
  const rawBuffer = decodeGoogleBase64(rawData.raw ?? "");

  const attachments = [];
  if (includeAttachments) {
    const attachmentParts = extractAttachmentParts(payload);

    for (let index = 0; index < attachmentParts.length; index += 1) {
      const part = attachmentParts[index];
      const attachmentPayload = await gmailApiGet(
        accessToken,
        `messages/${gmailMessageId}/attachments/${part.attachmentId}`
      );
      const fileData = decodeGoogleBase64(attachmentPayload.data ?? "");
      const baseName = sanitizeFileName(part.filename || `attachment_${index + 1}`);

      attachments.push({
        filename: part.filename || baseName,
        mimeType: part.mimeType,
        sizeBytes: fileData.length,
        contentBase64: fileData.toString("base64")
      });
    }
  }

  const record = {
    id: archiveId,
    userId: user.id,
    broker,
    from,
    subject,
    snippet: String(gmailMessage.snippet ?? ""),
    bodyPreview,
    dateHeader,
    messageIdHeader,
    gmailMessageId,
    gmailThreadId: String(gmailMessage.threadId ?? ""),
    gmailMessageUrl: `https://mail.google.com/mail/u/0/#inbox/${gmailMessageId}`,
    internalDateMs: Number(gmailMessage.internalDate ?? Date.now()),
    emlRelativePath: null,
    emlContentBase64: rawBuffer.toString("base64"),
    attachments,
    ingestedAt: nowIso,
    duplicateOfArchiveId: null
  };

  db.emailArchives.push(record);

  return {
    archived: true,
    skippedReason: null,
    record
  };
}

function findArchiveForUser(userId, archiveId) {
  return db.emailArchives.find((entry) => entry.userId === userId && entry.id === archiveId) || null;
}

function findArchiveById(archiveId) {
  return db.emailArchives.find((entry) => entry.id === archiveId) || null;
}

async function sendStoredFile(req, res, absolutePath, options) {
  try {
    const fileInfo = await stat(absolutePath);
    if (!fileInfo.isFile()) {
      sendJson(req, res, 404, { error: "File not found" });
      return;
    }

    applyCorsHeaders(req, res);
    res.statusCode = 200;
    res.setHeader("Content-Type", options.contentType || "application/octet-stream");
    if (options.downloadName) {
      res.setHeader("Content-Disposition", `attachment; filename="${sanitizeFileName(options.downloadName)}"`);
    }
    res.setHeader("Content-Length", String(fileInfo.size));

    const stream = createReadStream(absolutePath);
    stream.on("error", () => {
      if (!res.headersSent) {
        sendJson(req, res, 500, { error: "Failed to stream file" });
      } else {
        res.destroy();
      }
    });
    stream.pipe(res);
  } catch {
    sendJson(req, res, 404, { error: "File not found" });
  }
}

function sendBufferFile(req, res, buffer, options) {
  applyCorsHeaders(req, res);
  res.statusCode = 200;
  res.setHeader("Content-Type", options.contentType || "application/octet-stream");
  if (options.downloadName) {
    res.setHeader("Content-Disposition", `attachment; filename="${sanitizeFileName(options.downloadName)}"`);
  }
  res.setHeader("Content-Length", String(buffer.length));
  res.end(buffer);
}

function getPublicApiBase(req) {
  if (publicApiBase) {
    return publicApiBase;
  }

  const host = req.headers.host || "localhost";
  const proto = req.headers["x-forwarded-proto"] || "http";
  return `${proto}://${host}`;
}

function handleHealth(req, res) {
  sendJson(req, res, 200, {
    ok: true,
    service: appName,
    env: appEnv,
    time: new Date().toISOString(),
    authConfigured: isGoogleConfigured(),
    databaseConfigured: Boolean(databaseUrl),
    databaseEnabled: isDatabaseEnabled()
  });
}

function handleStatus(req, res) {
  sendJson(req, res, 200, {
    service: appName,
    env: appEnv,
    serverTime: new Date().toISOString(),
    notesCount: db.notes.length,
    cronRunCount: db.cronState.runCount,
    lastCronRunAt: db.cronState.lastRunAt,
    workerHeartbeatCount: db.cronState.workerHeartbeatCount,
    lastWorkerHeartbeatAt: db.cronState.lastWorkerHeartbeatAt,
    totalUsers: db.users.length,
    totalArchives: db.emailArchives.length,
    totalExtractionRuns: db.extractionRuns.length,
    totalExtractedReports: db.extractedReports.length,
    databaseConfigured: Boolean(databaseUrl),
    databaseEnabled: isDatabaseEnabled()
  });
}

function handleGetNotes(req, res) {
  sendJson(req, res, 200, {
    notes: db.notes,
    total: db.notes.length
  });
}

async function handleCreateNote(req, res) {
  const body = await readJsonBody(req);
  const text = String(body.text ?? "").trim();

  if (!text) {
    sendJson(req, res, 400, { error: "Field 'text' is required." });
    return;
  }

  const note = {
    id: db.counters.noteId,
    text,
    createdAt: new Date().toISOString()
  };

  db.counters.noteId += 1;
  db.notes.unshift(note);
  await persistDb();

  sendJson(req, res, 201, {
    note,
    total: db.notes.length
  });
}

async function handleCronRun(req, res) {
  if (cronSecret) {
    const providedSecret = req.headers["x-cron-secret"];
    if (providedSecret !== cronSecret) {
      sendJson(req, res, 401, { error: "Invalid cron secret." });
      return;
    }
  }

  const body = await readJsonBody(req);

  db.cronState.runCount += 1;
  db.cronState.lastRunAt = new Date().toISOString();
  const scheduleSummary = await runScheduledIngests(body.trigger ?? "cron");
  await persistDb();

  log("Cron run received", {
    runCount: db.cronState.runCount,
    trigger: body.trigger ?? "unknown"
  });

  sendJson(req, res, 200, {
    ok: true,
    runCount: db.cronState.runCount,
    lastCronRunAt: db.cronState.lastRunAt,
    scheduleSummary
  });
}

async function handleWorkerHeartbeat(req, res) {
  if (cronSecret) {
    const providedSecret = req.headers["x-cron-secret"];
    if (providedSecret !== cronSecret) {
      sendJson(req, res, 401, { error: "Invalid heartbeat secret." });
      return;
    }
  }

  db.cronState.workerHeartbeatCount += 1;
  db.cronState.lastWorkerHeartbeatAt = new Date().toISOString();
  await persistDb();

  sendJson(req, res, 200, {
    ok: true,
    workerHeartbeatCount: db.cronState.workerHeartbeatCount,
    lastWorkerHeartbeatAt: db.cronState.lastWorkerHeartbeatAt
  });
}

async function handleGoogleAuthUrl(req, res, requestUrl) {
  if (!isGoogleConfigured()) {
    sendJson(req, res, 503, {
      error:
        "Google OAuth is not configured. Set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_REDIRECT_URI."
    });
    return;
  }

  const requestedRedirect = requestUrl.searchParams.get("redirect_uri") || requestUrl.searchParams.get("redirectUri");
  const redirectUri = resolveRedirect(requestedRedirect);
  const stateToken = createOAuthStateToken(redirectUri);

  const url = new URL("https://accounts.google.com/o/oauth2/v2/auth");
  url.searchParams.set("client_id", googleClientId);
  url.searchParams.set("redirect_uri", googleRedirectUri);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", googleScopes);
  url.searchParams.set("access_type", "offline");
  url.searchParams.set("include_granted_scopes", "true");
  url.searchParams.set("prompt", "consent");
  url.searchParams.set("state", stateToken);

  sendJson(req, res, 200, {
    authUrl: url.toString(),
    redirectUri
  });
}

async function handleGoogleAuthCallback(req, res, requestUrl) {
  const code = requestUrl.searchParams.get("code");
  const stateToken = requestUrl.searchParams.get("state") || "";
  const oauthError = requestUrl.searchParams.get("error");

  const defaultRedirect = resolveRedirect(null);

  if (!stateToken) {
    sendText(req, res, 400, "Missing OAuth state token.");
    return;
  }

  const statePayload = verifySignedPayload(stateToken);
  const redirectUri = resolveRedirect(statePayload?.redirectUri || defaultRedirect);

  if (!statePayload || statePayload.typ !== "oauth_state") {
    sendRedirect(req, res, `${redirectUri}#auth=error&message=${encodeURIComponent("Invalid OAuth state")}`);
    return;
  }

  if (oauthError) {
    sendRedirect(req, res, `${redirectUri}#auth=error&message=${encodeURIComponent(oauthError)}`);
    return;
  }

  if (!code) {
    sendRedirect(req, res, `${redirectUri}#auth=error&message=${encodeURIComponent("Missing authorization code")}`);
    return;
  }

  try {
    const tokenData = await exchangeAuthCodeForTokens(code);
    const userInfo = await fetchGoogleUserInfo(tokenData.access_token);

    if (!userInfo.email || !userInfo.sub) {
      throw new Error("Google user info missing required identity fields");
    }

    const user = upsertUserFromGoogle(userInfo);
    upsertGoogleConnection(user, tokenData);

    const session = createSessionForUser(user.id);
    await persistDb();

    const successUrl = `${redirectUri}#auth=success&token=${encodeURIComponent(session.token)}&email=${encodeURIComponent(user.email)}`;
    sendRedirect(req, res, successUrl);
  } catch (error) {
    const message = error instanceof Error ? error.message : "OAuth callback failed";
    log("OAuth callback failed", { message });
    sendRedirect(req, res, `${redirectUri}#auth=error&message=${encodeURIComponent(message)}`);
  }
}

async function handleAuthMe(req, res, auth) {
  const connection = getGoogleConnection(auth.user.id);
  const prefs = getOrCreateGmailPreferences(auth.user.id);

  sendJson(req, res, 200, {
    user: {
      id: auth.user.id,
      email: auth.user.email,
      name: auth.user.name,
      picture: auth.user.picture
    },
    gmail: {
      connected: Boolean(connection),
      email: connection?.email ?? null,
      scope: connection?.scope ?? null,
      tokenUpdatedAt: connection?.tokenUpdatedAt ?? null,
      expiresAt: connection?.expiresAt ?? null
    },
    ingestionPreferences: {
      maxResults: prefs.maxResults,
      query: prefs.query,
      includeUnmapped: prefs.includeUnmapped,
      brokerMappings: prefs.brokerMappings,
      trackedLabelIds: prefs.trackedLabelIds,
      trackedLabelNames: prefs.trackedLabelNames,
      scheduleEnabled: prefs.scheduleEnabled,
      scheduleHour: prefs.scheduleHour,
      scheduleMinute: prefs.scheduleMinute,
      scheduleTimezone: prefs.scheduleTimezone,
      ingestFromDate: prefs.ingestFromDate,
      ingestToDate: prefs.ingestToDate,
      lastIngestAfterEpoch: prefs.lastIngestAfterEpoch,
      lastIngestAt: prefs.lastIngestAt,
      lastScheduledRunDate: prefs.lastScheduledRunDate
    }
  });
}

async function handleAuthLogout(req, res, auth) {
  auth.session.revokedAt = new Date().toISOString();
  await persistDb();
  sendJson(req, res, 200, { ok: true });
}

async function handleGetGmailConnection(req, res, auth) {
  const connection = getGoogleConnection(auth.user.id);
  if (!connection) {
    sendJson(req, res, 200, { connected: false });
    return;
  }

  sendJson(req, res, 200, {
    connected: true,
    email: connection.email,
    scope: connection.scope,
    tokenUpdatedAt: connection.tokenUpdatedAt,
    expiresAt: connection.expiresAt
  });
}

async function handleGetGmailLabels(req, res, auth) {
  const connection = getGoogleConnection(auth.user.id);
  if (!connection) {
    sendJson(req, res, 400, { error: "Gmail is not connected for this user." });
    return;
  }

  const accessToken = await getValidGoogleAccessToken(connection);
  const labelResponse = await gmailApiGet(accessToken, "labels");
  const labels = Array.isArray(labelResponse.labels) ? labelResponse.labels : [];
  const sorted = labels
    .map((label) => ({
      id: String(label.id ?? ""),
      name: String(label.name ?? ""),
      type: String(label.type ?? "user").toLowerCase(),
      messagesTotal: Number(label.messagesTotal ?? 0),
      threadsTotal: Number(label.threadsTotal ?? 0)
    }))
    .filter((label) => label.id && label.name)
    .sort((a, b) => a.name.localeCompare(b.name));

  sendJson(req, res, 200, { labels: sorted, total: sorted.length });
}

async function handleGetGmailPreferences(req, res, auth) {
  const prefs = getOrCreateGmailPreferences(auth.user.id);
  await persistDb();
  sendJson(req, res, 200, prefs);
}

async function handlePutGmailPreferences(req, res, auth) {
  const body = await readJsonBody(req);
  const prefs = getOrCreateGmailPreferences(auth.user.id);

  const maxResults = Number(body.maxResults ?? prefs.maxResults);
  const includeUnmapped = Boolean(body.includeUnmapped ?? prefs.includeUnmapped);
  const query = typeof body.query === "string" ? body.query.trim() : prefs.query;
  const trackedLabelIds = sanitizeLabelList(body.trackedLabelIds ?? prefs.trackedLabelIds);
  const trackedLabelNames = sanitizeLabelList(body.trackedLabelNames ?? prefs.trackedLabelNames);
  const scheduleEnabled = Boolean(body.scheduleEnabled ?? prefs.scheduleEnabled);
  const scheduleHour = Number.parseInt(String(body.scheduleHour ?? prefs.scheduleHour), 10);
  const scheduleMinute = Number.parseInt(String(body.scheduleMinute ?? prefs.scheduleMinute), 10);
  const scheduleTimezone = resolveTimeZone(body.scheduleTimezone ?? prefs.scheduleTimezone);
  const hasIngestFromDate = Object.prototype.hasOwnProperty.call(body, "ingestFromDate");
  const hasIngestToDate = Object.prototype.hasOwnProperty.call(body, "ingestToDate");
  const rawIngestFromDate = hasIngestFromDate ? body.ingestFromDate : prefs.ingestFromDate;
  const rawIngestToDate = hasIngestToDate ? body.ingestToDate : prefs.ingestToDate;
  const ingestFromDate =
    rawIngestFromDate === null || String(rawIngestFromDate).trim() === ""
      ? null
      : normalizeDateOnly(rawIngestFromDate);
  const ingestToDate =
    rawIngestToDate === null || String(rawIngestToDate).trim() === ""
      ? null
      : normalizeDateOnly(rawIngestToDate);
  const resetCursorToNow = body.startFromNow === true;
  const resetCursorToStart = body.resetCursor === true;

  const brokerMappings = Array.isArray(body.brokerMappings) ? body.brokerMappings : prefs.brokerMappings;
  if (
    (hasIngestFromDate && rawIngestFromDate !== null && String(rawIngestFromDate).trim() !== "" && !ingestFromDate) ||
    (hasIngestToDate && rawIngestToDate !== null && String(rawIngestToDate).trim() !== "" && !ingestToDate)
  ) {
    sendJson(req, res, 400, { error: "Invalid ingest date format. Use YYYY-MM-DD." });
    return;
  }
  if (ingestFromDate && ingestToDate && ingestFromDate > ingestToDate) {
    sendJson(req, res, 400, { error: "Ingest start date cannot be after end date." });
    return;
  }

  prefs.maxResults = Math.max(1, Math.min(100, Number.isFinite(maxResults) ? maxResults : 25));
  prefs.query = query;
  prefs.includeUnmapped = includeUnmapped;
  prefs.brokerMappings = mergeBrokerMappingsWithDefaults(brokerMappings);
  prefs.trackedLabelIds = trackedLabelIds;
  prefs.trackedLabelNames = trackedLabelNames;
  prefs.scheduleEnabled = scheduleEnabled;
  prefs.scheduleHour = Number.isInteger(scheduleHour) ? Math.max(0, Math.min(23, scheduleHour)) : 7;
  prefs.scheduleMinute = Number.isInteger(scheduleMinute) ? Math.max(0, Math.min(59, scheduleMinute)) : 30;
  prefs.scheduleTimezone = scheduleTimezone;
  prefs.ingestFromDate = ingestFromDate;
  prefs.ingestToDate = ingestToDate;
  if (resetCursorToStart) {
    prefs.lastIngestAfterEpoch = 0;
    prefs.lastScheduledRunDate = null;
  } else if (resetCursorToNow) {
    prefs.lastIngestAfterEpoch = Math.floor(Date.now() / 1000);
  }
  prefs.updatedAt = new Date().toISOString();

  await persistDb();
  sendJson(req, res, 200, prefs);
}

async function runGmailIngestForUser(params) {
  const labelIds = sanitizeLabelList(params.labelIds ?? params.prefs.trackedLabelIds);
  const trackedLabelIds = sanitizeLabelList(params.prefs.trackedLabelIds);
  const trackedLabelNames = sanitizeLabelList(params.prefs.trackedLabelNames);
  const labelBrokerMap = new Map();
  for (let index = 0; index < trackedLabelIds.length; index += 1) {
    const labelId = trackedLabelIds[index];
    const labelName = trackedLabelNames[index];
    labelBrokerMap.set(labelId, labelName || labelId);
  }

  if (labelIds.length === 0) {
    throw new Error("No tracked labels selected. Configure labels in Ingest setup.");
  }

  const includeAttachments = params.includeAttachments !== false;
  const maxResults = Math.max(
    1,
    Math.min(100, Number.isFinite(Number(params.maxResults)) ? Number(params.maxResults) : params.prefs.maxResults)
  );

  const explicitQuery = typeof params.query === "string" && params.query.trim() ? params.query.trim() : params.prefs.query;
  const hasExplicitAfterEpoch =
    Object.prototype.hasOwnProperty.call(params, "afterEpoch") &&
    params.afterEpoch !== undefined &&
    params.afterEpoch !== null &&
    String(params.afterEpoch).trim() !== "";
  const parsedAfterEpoch = hasExplicitAfterEpoch ? Number(params.afterEpoch) : NaN;
  const startAfterEpoch = hasExplicitAfterEpoch
    ? Number.isFinite(parsedAfterEpoch)
      ? Math.max(0, Math.floor(parsedAfterEpoch))
      : Math.max(0, Number(params.prefs.lastIngestAfterEpoch ?? 0))
    : Math.max(0, Number(params.prefs.lastIngestAfterEpoch ?? 0));
  const ingestFromDate = normalizeDateOnly(params.ingestFromDate ?? params.prefs.ingestFromDate);
  const ingestToDate = normalizeDateOnly(params.ingestToDate ?? params.prefs.ingestToDate);
  const query = composeIngestQuery(explicitQuery, startAfterEpoch, ingestFromDate, ingestToDate);
  const accessToken = await getValidGoogleAccessToken(params.connection);
  const pageSize = Math.max(10, Math.min(100, maxResults * 2));
  const maxMessagesToInspect = Math.max(maxResults, Math.min(2000, maxResults * 20));
  const maxPages = 20;
  const existingArchiveByMessageId = new Map(
    db.emailArchives
      .filter((entry) => entry.userId === params.user.id)
      .map((entry) => [String(entry.gmailMessageId ?? ""), entry])
  );

  let pageToken = null;
  let pagesScanned = 0;
  let fetchedMessages = 0;
  const results = [];
  let archivedCount = 0;
  let skippedCount = 0;
  let skippedUnmappedCount = 0;
  let alreadyArchivedCount = 0;
  let attachmentCount = 0;
  let newestInternalEpoch = 0;
  let nextPageToken = null;
  let reachedArchiveLimit = false;
  let reachedInspectLimit = false;

  while (pagesScanned < maxPages && fetchedMessages < maxMessagesToInspect && archivedCount < maxResults) {
    const listResponse = await gmailApiGet(accessToken, "messages", {
      maxResults: pageSize,
      q: query,
      labelIds,
      pageToken
    });
    const messages = Array.isArray(listResponse.messages) ? listResponse.messages : [];
    nextPageToken =
      typeof listResponse.nextPageToken === "string" && listResponse.nextPageToken.trim()
        ? listResponse.nextPageToken
        : null;
    pagesScanned += 1;

    if (messages.length === 0) {
      break;
    }

    for (const messageStub of messages) {
      fetchedMessages += 1;
      const gmailMessageId = String(messageStub.id ?? "");
      const existing = existingArchiveByMessageId.get(gmailMessageId);
      if (existing) {
        skippedCount += 1;
        alreadyArchivedCount += 1;
        const existingEpoch = Math.floor(Number(existing.internalDateMs ?? 0) / 1000);
        if (Number.isFinite(existingEpoch) && existingEpoch > 0) {
          newestInternalEpoch = Math.max(newestInternalEpoch, existingEpoch);
        }
      } else {
        const full = await gmailApiGet(accessToken, `messages/${gmailMessageId}`, { format: "full" });
        const raw = await gmailApiGet(accessToken, `messages/${gmailMessageId}`, { format: "raw" });

        const from = getHeader(full.payload?.headers, "From");
        const sender = parseFromHeader(from);
        const messageLabelIds = Array.isArray(full.labelIds) ? full.labelIds.map((value) => String(value)) : [];
        let broker = null;
        for (const labelId of labelIds) {
          if (messageLabelIds.includes(labelId)) {
            broker = labelBrokerMap.get(labelId) || labelId;
            break;
          }
        }
        if (!broker) {
          broker = detectBroker(`${from} ${sender.email}`, params.prefs.brokerMappings);
        }
        const internalEpoch = Math.floor(Number(full.internalDate ?? Date.now()) / 1000);
        newestInternalEpoch = Math.max(newestInternalEpoch, internalEpoch);

        const archived = await archiveGmailMessage(params.user, full, raw, includeAttachments, accessToken, broker);

        if (!archived.archived) {
          skippedCount += 1;
          if (archived.skippedReason === "already_archived") {
            alreadyArchivedCount += 1;
          }
          continue;
        }

        archivedCount += 1;
        attachmentCount += archived.record.attachments.length;
        results.push(makeArchivePublicRecord(archived.record));
        existingArchiveByMessageId.set(gmailMessageId, archived.record);
      }

      if (archivedCount >= maxResults) {
        reachedArchiveLimit = true;
        break;
      }
      if (fetchedMessages >= maxMessagesToInspect) {
        reachedInspectLimit = true;
        break;
      }
    }

    if (reachedArchiveLimit || reachedInspectLimit || !nextPageToken) {
      break;
    }
    pageToken = nextPageToken;
  }

  const skippedOnlyUnmapped = fetchedMessages > 0 && archivedCount === 0 && skippedUnmappedCount === fetchedMessages;
  const cursorAdvanced = !skippedOnlyUnmapped && fetchedMessages > 0;
  if (cursorAdvanced) {
    params.prefs.lastIngestAfterEpoch = Math.max(startAfterEpoch, newestInternalEpoch);
  }
  params.prefs.lastIngestAt = new Date().toISOString();
  params.prefs.updatedAt = new Date().toISOString();

  return {
    summary: {
      queryUsed: query,
      requestedMaxResults: maxResults,
      fetchedMessages,
      archivedCount,
      skippedCount,
      skippedUnmappedCount,
      alreadyArchivedCount,
      pagesScanned,
      pageSize,
      nextPageAvailable: Boolean(nextPageToken),
      reachedArchiveLimit,
      reachedInspectLimit,
      maxMessagesToInspect,
      cursorAdvanced,
      attachmentCount,
      trackedLabels: labelIds,
      ingestFromDate,
      ingestToDate,
      cursorAfterEpoch: params.prefs.lastIngestAfterEpoch
    },
    items: results
  };
}

function toArchiveIdList(items) {
  if (!Array.isArray(items)) {
    return [];
  }

  const unique = new Set();
  for (const item of items) {
    const archiveId = String(item?.id ?? "").trim();
    if (archiveId) {
      unique.add(archiveId);
    }
  }

  return [...unique];
}

async function queueExtractionFromArchiveIds(userId, archiveIds, trigger) {
  const normalizedUserId = String(userId ?? "").trim();
  if (!normalizedUserId) {
    return {
      queued: false,
      reason: "missing_user",
      run: null
    };
  }

  const ids = Array.isArray(archiveIds)
    ? [...new Set(archiveIds.map((entry) => String(entry ?? "").trim()).filter(Boolean))]
    : [];
  if (ids.length === 0) {
    return {
      queued: false,
      reason: "no_archives",
      run: null
    };
  }

  const run = await extractionOrchestrator.triggerRun({
    userId: normalizedUserId,
    archiveIds: ids,
    limit: Math.max(1, Math.min(1000, ids.length)),
    includeAlreadyExtracted: false,
    trigger: String(trigger || "ingest_auto")
  });

  return {
    queued: true,
    reason: "queued",
    run
  };
}

async function runScheduledIngests(trigger = "scheduled") {
  const now = new Date();
  const details = [];
  let dueUsers = 0;
  let successfulRuns = 0;
  let failedRuns = 0;
  let archivedCount = 0;
  let aiProcessedEmails = 0;
  let aiFailedEmails = 0;
  let extractedCompanyUpdates = 0;
  let dedupeMarkedDuplicates = 0;

  for (const connection of db.gmailConnections) {
    const user = getUserById(connection.userId);
    if (!user) {
      continue;
    }

    const prefs = getOrCreateGmailPreferences(user.id);
    if (!prefs.scheduleEnabled || prefs.trackedLabelIds.length === 0) {
      continue;
    }

    if (!isScheduleDueNow(prefs, now)) {
      continue;
    }

    dueUsers += 1;
    const zoneParts = getZonedDateParts(now, prefs.scheduleTimezone);

    try {
      const ingest = await runGmailIngestForUser({
        user,
        prefs,
        connection,
        includeAttachments: true
      });
      let companyPipeline = {
        ingestedEmails: 0,
        aiProcessedEmails: 0,
        aiFailedEmails: 0,
        aiFailureSamples: [],
        extractedCompanyUpdates: 0,
        duplicatesMarked: 0,
        source: companyUpdateExtractor.source,
        error: null
      };
      try {
        const ingestedRecords = [];
        for (const item of ingest.items) {
          const row = await upsertIngestedEmailRecord(user, item);
          if (row) {
            ingestedRecords.push(row);
          }
        }
        companyPipeline = await runCompanyUpdatePipelineForIngestedRecords(user, ingestedRecords);
      } catch (error) {
        companyPipeline.error = error instanceof Error ? error.message : "Failed to run company update pipeline";
      }

      prefs.lastScheduledRunDate = zoneParts.dateKey;
      successfulRuns += 1;
      archivedCount += ingest.summary.archivedCount;
      aiProcessedEmails += Number(companyPipeline.aiProcessedEmails || 0);
      aiFailedEmails += Number(companyPipeline.aiFailedEmails || 0);
      extractedCompanyUpdates += Number(companyPipeline.extractedCompanyUpdates || 0);
      dedupeMarkedDuplicates += Number(companyPipeline.duplicatesMarked || 0);
      details.push({
        userId: user.id,
        email: user.email,
        status: "ok",
        archivedCount: ingest.summary.archivedCount,
        fetchedMessages: ingest.summary.fetchedMessages,
        companyUpdatePipeline: companyPipeline
      });
    } catch (error) {
      failedRuns += 1;
      details.push({
        userId: user.id,
        email: user.email,
        status: "failed",
        error: error instanceof Error ? error.message : "Unknown ingest error"
      });
    }
  }

  await persistDb();

  return {
    trigger,
    dueUsers,
    successfulRuns,
    failedRuns,
    archivedCount,
    aiProcessedEmails,
    aiFailedEmails,
    extractedCompanyUpdates,
    dedupeMarkedDuplicates,
    details
  };
}

async function handleGmailIngest(req, res, auth) {
  const connection = getGoogleConnection(auth.user.id);
  if (!connection) {
    sendJson(req, res, 400, { error: "Gmail is not connected for this user." });
    return;
  }

  const prefs = getOrCreateGmailPreferences(auth.user.id);
  const body = await readJsonBody(req);
  const ingest = await runGmailIngestForUser({
    user: auth.user,
    prefs,
    connection,
    includeAttachments: body.includeAttachments !== false,
    maxResults: body.maxResults,
    query: body.query,
    labelIds: body.labelIds,
    afterEpoch: body.afterEpoch,
    ingestFromDate: body.ingestFromDate,
    ingestToDate: body.ingestToDate
  });

  let reprocessedArchivedCount = 0;
  const archivedForPipelineCandidates = [];
  for (const item of ingest.items) {
    archivedForPipelineCandidates.push(item);
  }
  if (archivedForPipelineCandidates.length === 0 && Number(ingest.summary.alreadyArchivedCount || 0) > 0) {
    const reprocessArchived = body.reprocessArchived !== false;
    if (reprocessArchived) {
      const trackedBrokers = new Set(
        sanitizeLabelList(prefs.trackedLabelNames).map((value) => normalizeText(value)).filter(Boolean)
      );
      const fromEpoch = dateOnlyToEpochStart(ingest.summary.ingestFromDate);
      const toEpochExclusive = dateOnlyToEpochEndExclusive(ingest.summary.ingestToDate);
      const fallbackLimitRaw = Number.parseInt(String(body.reprocessArchivedLimit ?? "25"), 10);
      const fallbackLimit = Number.isFinite(fallbackLimitRaw) ? Math.max(1, Math.min(100, fallbackLimitRaw)) : 25;
      const reprocessCompleted = body.reprocessCompleted === true;

      const archivedCandidates = db.emailArchives
        .filter((entry) => entry.userId === auth.user.id)
        .filter((entry) => {
          if (trackedBrokers.size === 0) {
            return true;
          }
          return trackedBrokers.has(normalizeText(entry.broker));
        })
        .filter((entry) => {
          const internalEpoch = Math.floor(Number(entry.internalDateMs ?? 0) / 1000);
          if (Number.isFinite(fromEpoch) && internalEpoch < fromEpoch) {
            return false;
          }
          if (Number.isFinite(toEpochExclusive) && internalEpoch >= toEpochExclusive) {
            return false;
          }
          return true;
        })
        .sort((left, right) => Number(right.internalDateMs ?? 0) - Number(left.internalDateMs ?? 0))
        .map((entry) => toPipelineArchiveItem(entry));

      const needsPipeline = await filterRecordsNeedingCompanyPipeline(auth.user.id, archivedCandidates, {
        limit: fallbackLimit,
        includeCompleted: reprocessCompleted
      });
      for (const item of needsPipeline) {
        archivedForPipelineCandidates.push(item);
      }
      reprocessedArchivedCount = needsPipeline.length;
    }
  }

  let companyUpdatePipeline = {
    ingestedEmails: 0,
    aiProcessedEmails: 0,
    aiFailedEmails: 0,
    aiFailureSamples: [],
    extractedCompanyUpdates: 0,
    duplicatesMarked: 0,
    source: companyUpdateExtractor.source,
    error: null
  };
  try {
    const ingestedRecords = [];
    for (const item of archivedForPipelineCandidates) {
      const row = await upsertIngestedEmailRecord(auth.user, item);
      if (row) {
        ingestedRecords.push(row);
      }
    }
    companyUpdatePipeline = await runCompanyUpdatePipelineForIngestedRecords(auth.user, ingestedRecords);
  } catch (error) {
    companyUpdatePipeline.error = error instanceof Error ? error.message : "Failed to run company update pipeline";
  }

  await persistDb();

  sendJson(req, res, 200, {
    ok: true,
    summary: ingest.summary,
    items: ingest.items,
    extraction: {
      queued: false,
      reason: "db_first_pipeline_active",
      run: null,
      error: null
    },
    companyUpdatePipeline,
    ingestDebug: {
      reprocessedArchivedCount
    }
  });
}

async function handleResetPipelineData(req, res, auth) {
  const body = await readJsonBody(req);
  const clearArchives = body.clearArchives !== false;
  const clearExtraction = body.clearExtraction !== false;
  const resetCursor = body.resetCursor !== false;
  const force = body.force === true;
  const activeStatuses = new Set(["queued", "running"]);
  const activeRuns = db.extractionRuns.filter(
    (entry) =>
      entry.userId === auth.user.id &&
      activeStatuses.has(String(entry.status || "").toLowerCase()) &&
      !entry.abortRequestedAt
  );

  if (activeRuns.length > 0 && !force) {
    sendJson(req, res, 409, {
      error: "Extraction run is currently active. Wait for completion or retry with force=true.",
      activeRunCount: activeRuns.length
    });
    return;
  }

  let forcedAbortCount = 0;
  if (activeRuns.length > 0 && force) {
    for (const run of activeRuns) {
      const result = await extractionOrchestrator.abortRun({
        userId: auth.user.id,
        runId: run.id,
        reason: "Forced reset requested by user"
      });
      if (result?.accepted) {
        forcedAbortCount += 1;
      }
    }
  }

  let removedArchives = 0;
  let removedExtractedReports = 0;
  let removedExtractionRuns = 0;
  let removedIngestedEmails = 0;
  let removedCompanyUpdates = 0;

  if (clearArchives) {
    const before = db.emailArchives.length;
    db.emailArchives = db.emailArchives.filter((entry) => entry.userId !== auth.user.id);
    removedArchives = before - db.emailArchives.length;
  }

  if (isDatabaseEnabled() && (clearArchives || clearExtraction)) {
    if (clearArchives) {
      const ingestedCountResult = await executePipelineDbQuery(
        `SELECT COUNT(*)::int AS total FROM ${ingestedEmailsTable} WHERE user_id = $1`,
        [auth.user.id]
      );
      const updatesCountResult = await executePipelineDbQuery(
        `SELECT COUNT(*)::int AS total FROM ${companyUpdatesTable} WHERE user_id = $1`,
        [auth.user.id]
      );

      removedIngestedEmails = Number(ingestedCountResult.rows?.[0]?.total ?? 0);
      removedCompanyUpdates = Number(updatesCountResult.rows?.[0]?.total ?? 0);

      await executePipelineDbQuery(`DELETE FROM ${ingestedEmailsTable} WHERE user_id = $1`, [auth.user.id]);
    } else if (clearExtraction) {
      const updatesCountResult = await executePipelineDbQuery(
        `SELECT COUNT(*)::int AS total FROM ${companyUpdatesTable} WHERE user_id = $1`,
        [auth.user.id]
      );
      removedCompanyUpdates = Number(updatesCountResult.rows?.[0]?.total ?? 0);
      await executePipelineDbQuery(`DELETE FROM ${companyUpdatesTable} WHERE user_id = $1`, [auth.user.id]);
      await executePipelineDbQuery(
        `UPDATE ${ingestedEmailsTable}
            SET ai_status = 'pending',
                ai_error = NULL,
                ai_extracted_at = NULL,
                dedupe_checked_at = NULL,
                updated_at = NOW()
          WHERE user_id = $1`,
        [auth.user.id]
      );
    }
  }

  if (clearExtraction) {
    const beforeReports = db.extractedReports.length;
    const beforeRuns = db.extractionRuns.length;
    db.extractedReports = db.extractedReports.filter((entry) => entry.userId !== auth.user.id);
    db.extractionRuns = db.extractionRuns.filter((entry) => entry.userId !== auth.user.id);
    removedExtractedReports = beforeReports - db.extractedReports.length;
    removedExtractionRuns = beforeRuns - db.extractionRuns.length;
  }

  const prefs = getOrCreateGmailPreferences(auth.user.id);
  if (resetCursor) {
    prefs.lastIngestAfterEpoch = 0;
    prefs.lastScheduledRunDate = null;
    prefs.lastIngestAt = null;
  }
  prefs.updatedAt = new Date().toISOString();

  await persistDb();

  sendJson(req, res, 200, {
    ok: true,
    clearArchives,
    clearExtraction,
    resetCursor,
    force,
    forcedAbortCount,
    removedArchives,
    removedExtractedReports,
    removedExtractionRuns,
    removedIngestedEmails,
    removedCompanyUpdates
  });
}

function parsePaginationParams(requestUrl, fallbackLimit = 30) {
  const limitRaw = Number(requestUrl.searchParams.get("limit") ?? String(fallbackLimit));
  const offsetRaw = Number(requestUrl.searchParams.get("offset") ?? "0");
  return {
    limit: Math.max(1, Math.min(100, Number.isFinite(limitRaw) ? Math.floor(limitRaw) : fallbackLimit)),
    offset: Math.max(0, Number.isFinite(offsetRaw) ? Math.floor(offsetRaw) : 0)
  };
}

function parseArchiveIdList(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (!Array.isArray(value)) {
    return null;
  }

  return value.map((entry) => String(entry ?? "").trim()).filter(Boolean);
}

function parseStringList(value, fallback = []) {
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry ?? "").trim()).filter(Boolean);
  }

  if (typeof value === "string") {
    return value
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  return fallback;
}

function isLikelyEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value ?? "").trim());
}

function normalizeCompanyKey(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildDigestSubject(now = new Date()) {
  const dateLabel = now.toLocaleDateString("en-IN", {
    year: "numeric",
    month: "short",
    day: "2-digit"
  });
  return `Brokerage Digest - ${dateLabel}`;
}

function buildDigestEmailHtml(preview) {
  const sectionHtml = preview.sections
    .map((section) => {
      const listHtml = section.items
        .map(
          (item) =>
            `<li><strong>${escapeHtml(item.companyCanonical)} · ${escapeHtml(item.reportType)}</strong> - ${escapeHtml(
              item.summary
            )}</li>`
        )
        .join("");
      return `<h3>${escapeHtml(section.broker)}</h3><ul>${listHtml}</ul>`;
    })
    .join("");

  return `<!doctype html><html><body>
    <h2>Brokerage Digest</h2>
    <p><strong>Generated:</strong> ${escapeHtml(preview.generatedAt)}</p>
    <p><strong>Reports:</strong> ${preview.totalReports} (duplicates suppressed: ${preview.duplicateSuppressed})</p>
    <p><strong>Priority hits:</strong> ${preview.priorityHits}</p>
    ${sectionHtml || "<p>No extracted reports found for the selected filters.</p>"}
  </body></html>`;
}

function buildDigestEmailText(preview) {
  const lines = [
    "Brokerage Digest",
    `Generated: ${preview.generatedAt}`,
    `Reports: ${preview.totalReports} (duplicates suppressed: ${preview.duplicateSuppressed})`,
    `Priority hits: ${preview.priorityHits}`,
    ""
  ];

  for (const section of preview.sections) {
    lines.push(`${section.broker}`);
    for (const item of section.items) {
      lines.push(`- ${item.companyCanonical} | ${item.reportType} | ${item.summary}`);
    }
    lines.push("");
  }

  return lines.join("\n").trim();
}

function buildRawGmailMessage({ to, subject, textBody, htmlBody }) {
  const boundary = `digest_${randomBytes(8).toString("hex")}`;
  const lines = [
    `To: ${to.join(", ")}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/alternative; boundary=\"${boundary}\"`,
    "",
    `--${boundary}`,
    "Content-Type: text/plain; charset=UTF-8",
    "Content-Transfer-Encoding: 8bit",
    "",
    textBody,
    "",
    `--${boundary}`,
    "Content-Type: text/html; charset=UTF-8",
    "Content-Transfer-Encoding: 8bit",
    "",
    htmlBody,
    "",
    `--${boundary}--`,
    ""
  ];

  return base64UrlEncode(Buffer.from(lines.join("\r\n"), "utf8"));
}

function buildDigestPreviewForUser(userId, options = {}) {
  const limit = Math.max(1, Math.min(500, Number(options.limit ?? 200) || 200));
  const priorityCompanies = parseStringList(options.priorityCompanies);
  const ignoredCompanies = parseStringList(options.ignoredCompanies);
  const priorityRank = new Map(
    priorityCompanies.map((entry, index) => [normalizeCompanyKey(entry), index])
  );
  const ignoredSet = new Set(ignoredCompanies.map((entry) => normalizeCompanyKey(entry)));

  const canonical = extractionOrchestrator.listReports({
    userId,
    limit,
    offset: 0,
    includeDuplicates: false
  });
  const withDuplicates = extractionOrchestrator.listReports({
    userId,
    limit,
    offset: 0,
    includeDuplicates: true
  });

  const filtered = canonical.items.filter((item) => {
    const key = normalizeCompanyKey(item.companyCanonical || item.companyRaw);
    if (!key) {
      return true;
    }
    return !ignoredSet.has(key);
  });

  filtered.sort((left, right) => {
    const leftPriority = priorityRank.get(normalizeCompanyKey(left.companyCanonical || left.companyRaw));
    const rightPriority = priorityRank.get(normalizeCompanyKey(right.companyCanonical || right.companyRaw));
    const leftRank = leftPriority === undefined ? Number.MAX_SAFE_INTEGER : leftPriority;
    const rightRank = rightPriority === undefined ? Number.MAX_SAFE_INTEGER : rightPriority;
    if (leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    return new Date(right.publishedAt ?? 0).getTime() - new Date(left.publishedAt ?? 0).getTime();
  });

  const byBroker = new Map();
  for (const item of filtered) {
    const broker = String(item.broker || "Unmapped Broker");
    if (!byBroker.has(broker)) {
      byBroker.set(broker, []);
    }
    byBroker.get(broker).push(item);
  }

  const sections = [...byBroker.entries()]
    .sort((left, right) => left[0].localeCompare(right[0]))
    .map(([broker, items]) => ({
      broker,
      items
    }));

  const priorityHits = filtered.filter((item) =>
    priorityRank.has(normalizeCompanyKey(item.companyCanonical || item.companyRaw))
  ).length;
  const duplicateSuppressed = Math.max(0, withDuplicates.total - canonical.total);

  return {
    generatedAt: new Date().toISOString(),
    totalReports: filtered.length,
    priorityHits,
    duplicateSuppressed,
    sections,
    previewText: buildDigestEmailText({
      generatedAt: new Date().toISOString(),
      totalReports: filtered.length,
      priorityHits,
      duplicateSuppressed,
      sections
    })
  };
}

async function handleTriggerExtractionRun(req, res, auth) {
  const body = await readJsonBody(req);
  const parsedArchiveIds = parseArchiveIdList(body.archiveIds);
  if (body.archiveIds !== undefined && body.archiveIds !== null && parsedArchiveIds === null) {
    sendJson(req, res, 400, { error: "archiveIds must be an array of archive ids." });
    return;
  }

  if (parsedArchiveIds && parsedArchiveIds.length > 1000) {
    sendJson(req, res, 400, { error: "archiveIds cannot exceed 1000 entries per run." });
    return;
  }

  const broker = body.broker === undefined || body.broker === null ? null : String(body.broker).trim();
  if (body.broker !== undefined && body.broker !== null && !broker) {
    sendJson(req, res, 400, { error: "broker filter cannot be an empty string." });
    return;
  }

  const limitInput = body.limit;
  if (
    limitInput !== undefined &&
    (!Number.isFinite(Number(limitInput)) || Number(limitInput) < 1 || Number(limitInput) > 1000)
  ) {
    sendJson(req, res, 400, { error: "limit must be a number between 1 and 1000." });
    return;
  }

  const run = await extractionOrchestrator.triggerRun({
    userId: auth.user.id,
    archiveIds: parsedArchiveIds,
    broker,
    limit: limitInput,
    includeAlreadyExtracted: body.includeAlreadyExtracted === true,
    trigger: typeof body.trigger === "string" ? body.trigger : "manual_api"
  });

  sendJson(req, res, 202, {
    ok: true,
    run
  });
}

async function handleListExtractionRuns(req, res, auth, requestUrl) {
  const { limit, offset } = parsePaginationParams(requestUrl, 20);
  const status = requestUrl.searchParams.get("status")?.trim().toLowerCase() || null;
  if (status && !["queued", "running", "completed", "failed", "aborted"].includes(status)) {
    sendJson(req, res, 400, {
      error: "status must be one of: queued, running, completed, failed, aborted."
    });
    return;
  }

  const payload = extractionOrchestrator.listRuns({
    userId: auth.user.id,
    limit,
    offset,
    status
  });
  sendJson(req, res, 200, payload);
}

async function handleGetExtractionRunStatus(req, res, auth, runId) {
  const run = extractionOrchestrator.getRunStatus({
    userId: auth.user.id,
    runId
  });

  if (!run) {
    sendJson(req, res, 404, { error: "Extraction run not found." });
    return;
  }

  sendJson(req, res, 200, { run });
}

async function handleAbortExtractionRun(req, res, auth, runId) {
  const body = await readJsonBody(req);
  const reason =
    body.reason === undefined || body.reason === null ? undefined : String(body.reason).trim() || undefined;

  const result = await extractionOrchestrator.abortRun({
    userId: auth.user.id,
    runId,
    reason
  });

  if (!result) {
    sendJson(req, res, 404, { error: "Extraction run not found." });
    return;
  }

  const statusCode = result.accepted ? 202 : 200;
  sendJson(req, res, statusCode, {
    ok: true,
    accepted: result.accepted,
    immediate: result.immediate,
    alreadyTerminal: result.alreadyTerminal,
    run: result.run
  });
}

async function handleListExtractedReports(req, res, auth, requestUrl) {
  const { limit, offset } = parsePaginationParams(requestUrl, 30);
  const includeDuplicatesRaw = requestUrl.searchParams.get("includeDuplicates");
  const includeDuplicates =
    includeDuplicatesRaw === null
      ? true
      : !(includeDuplicatesRaw.toLowerCase() === "false" || includeDuplicatesRaw === "0");

  const payload = extractionOrchestrator.listReports({
    userId: auth.user.id,
    limit,
    offset,
    broker: requestUrl.searchParams.get("broker"),
    reportType: requestUrl.searchParams.get("reportType"),
    company: requestUrl.searchParams.get("company"),
    runId: requestUrl.searchParams.get("runId"),
    query: requestUrl.searchParams.get("q"),
    publishedFrom: requestUrl.searchParams.get("publishedFrom"),
    publishedTo: requestUrl.searchParams.get("publishedTo"),
    includeDuplicates
  });

  sendJson(req, res, 200, payload);
}

async function handleListCompanyUpdates(req, res, auth, requestUrl) {
  if (!isDatabaseEnabled()) {
    sendJson(req, res, 503, { error: "Postgres is required for DB-first company updates." });
    return;
  }

  const { limit, offset } = parsePaginationParams(requestUrl, 100);
  const includeDuplicatesRaw = String(requestUrl.searchParams.get("includeDuplicates") || "").trim().toLowerCase();
  const includeDuplicates = includeDuplicatesRaw === "true" || includeDuplicatesRaw === "1" || includeDuplicatesRaw === "yes";
  const brokerFilter = normalizeText(requestUrl.searchParams.get("broker"));
  const companyFilter = normalizeText(requestUrl.searchParams.get("company"));
  const reportTypeRaw = normalizeText(requestUrl.searchParams.get("reportType"));
  const reportTypeFilter = reportTypeRaw && reportTypeRaw.toLowerCase() !== "all" ? normalizeReportTypeLabel(reportTypeRaw) : "";
  const query = normalizeText(requestUrl.searchParams.get("q"));

  const whereClauses = ["cu.user_id = $1"];
  const values = [auth.user.id];
  let valueIndex = values.length;

  if (!includeDuplicates) {
    whereClauses.push("cu.is_duplicate = FALSE");
  }
  if (brokerFilter) {
    valueIndex += 1;
    whereClauses.push(`cu.broker = $${valueIndex}`);
    values.push(brokerFilter);
  }
  if (companyFilter) {
    valueIndex += 1;
    whereClauses.push(`cu.company_canonical ILIKE $${valueIndex}`);
    values.push(`%${companyFilter}%`);
  }
  if (reportTypeFilter) {
    valueIndex += 1;
    whereClauses.push(`cu.report_type = $${valueIndex}`);
    values.push(reportTypeFilter);
  }
  if (query) {
    valueIndex += 1;
    whereClauses.push(`(
      cu.company_canonical ILIKE $${valueIndex}
      OR cu.broker ILIKE $${valueIndex}
      OR cu.summary ILIKE $${valueIndex}
      OR cu.decision ILIKE $${valueIndex}
    )`);
    values.push(`%${query}%`);
  }

  const whereSql = whereClauses.join(" AND ");

  const totalQuery = await executePipelineDbQuery(
    `SELECT COUNT(*)::int AS total
       FROM ${companyUpdatesTable} cu
      WHERE ${whereSql}`,
    values
  );
  const total = Number(totalQuery.rows?.[0]?.total ?? 0);

  const pageValues = [...values, limit, offset];
  const pageLimitIndex = values.length + 1;
  const pageOffsetIndex = values.length + 2;
  const rowsQuery = await executePipelineDbQuery(
    `SELECT
       cu.id,
       cu.email_record_id,
       COALESCE(cu.published_at, cu.created_at) AS event_date,
       cu.company_canonical,
       cu.broker,
       cu.report_type,
       cu.summary,
       cu.decision,
       cu.is_duplicate,
       cu.duplicate_of_update_id,
       cu.archive_id AS update_archive_id,
       ie.archive_id AS email_archive_id,
       ie.subject AS email_subject,
       ie.attachments AS email_attachments,
       COALESCE(ie.raw_payload->>'gmailMessageUrl', '') AS gmail_message_url
     FROM ${companyUpdatesTable} cu
     LEFT JOIN ${ingestedEmailsTable} ie
       ON ie.id = cu.email_record_id
      AND ie.user_id = cu.user_id
     WHERE ${whereSql}
     ORDER BY COALESCE(cu.published_at, cu.created_at) DESC, cu.id DESC
     LIMIT $${pageLimitIndex}
     OFFSET $${pageOffsetIndex}`,
    pageValues
  );

  const items = (rowsQuery.rows || []).map((row) => {
    const archiveId = normalizeText(row.update_archive_id || row.email_archive_id);
    const attachmentList = Array.isArray(row.email_attachments) ? row.email_attachments : [];
    const firstAttachmentPath =
      archiveId && attachmentList.length > 0
        ? `/api/email-archives/${encodeURIComponent(archiveId)}/attachments/0`
        : null;

    return {
      id: String(row.id),
      emailRecordId: String(row.email_record_id),
      date: row.event_date ? new Date(row.event_date).toISOString() : null,
      company: row.company_canonical || "Unknown Company",
      brokerage: row.broker || "Unmapped Broker",
      reportType: row.report_type || "Other",
      summary: row.summary || "",
      decision: normalizeDecisionLabel(row.decision),
      isDuplicate: row.is_duplicate === true,
      duplicateOfUpdateId: row.duplicate_of_update_id ? String(row.duplicate_of_update_id) : null,
      sourceEmailSubject: normalizeText(row.email_subject),
      archiveId: archiveId || null,
      sourceLinks: {
        archive: archiveId ? `/api/email-archives/${encodeURIComponent(archiveId)}/raw` : null,
        attachment: firstAttachmentPath,
        gmail: normalizeText(row.gmail_message_url) || null
      }
    };
  });

  sendJson(req, res, 200, {
    total,
    limit,
    offset,
    includeDuplicates,
    items
  });
}

async function handleDigestPreview(req, res, auth, requestUrl) {
  const preview = buildDigestPreviewForUser(auth.user.id, {
    limit: requestUrl.searchParams.get("limit"),
    priorityCompanies: requestUrl.searchParams.get("priorityCompanies") || "",
    ignoredCompanies: requestUrl.searchParams.get("ignoredCompanies") || ""
  });

  sendJson(req, res, 200, {
    ...preview,
    htmlPreview: buildDigestEmailHtml(preview)
  });
}

async function handleDigestSend(req, res, auth) {
  const connection = getGoogleConnection(auth.user.id);
  if (!connection) {
    sendJson(req, res, 400, { error: "Gmail is not connected for this user." });
    return;
  }

  const body = await readJsonBody(req);
  const recipients = parseStringList(body.recipients).filter((entry) => isLikelyEmail(entry)).slice(0, 25);
  if (recipients.length === 0) {
    sendJson(req, res, 400, { error: "Provide at least one valid recipient email." });
    return;
  }

  const preview = buildDigestPreviewForUser(auth.user.id, {
    limit: body.limit,
    priorityCompanies: body.priorityCompanies,
    ignoredCompanies: body.ignoredCompanies
  });
  const htmlBody = buildDigestEmailHtml(preview);
  const textBody = buildDigestEmailText(preview);
  const subject = String(body.subject || buildDigestSubject()).trim();
  const raw = buildRawGmailMessage({
    to: recipients,
    subject: subject || buildDigestSubject(),
    textBody,
    htmlBody
  });

  try {
    const accessToken = await getValidGoogleAccessToken(connection);
    const sendResult = await gmailApiRequest(accessToken, "messages/send", {
      method: "POST",
      body: { raw }
    });

    sendJson(req, res, 200, {
      ok: true,
      recipients,
      subject,
      digest: {
        totalReports: preview.totalReports,
        duplicateSuppressed: preview.duplicateSuppressed,
        priorityHits: preview.priorityHits
      },
      gmailMessageId: sendResult.id || null,
      gmailThreadId: sendResult.threadId || null
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to send digest email";
    if (message.toLowerCase().includes("insufficient") || message.toLowerCase().includes("permission")) {
      sendJson(req, res, 403, {
        error:
          "Digest send requires Gmail send scope. Reconnect Google so new scopes are granted (gmail.send)."
      });
      return;
    }

    throw error;
  }
}

async function handleListEmailArchives(req, res, auth, requestUrl) {
  const limitRaw = Number(requestUrl.searchParams.get("limit") ?? "30");
  const offsetRaw = Number(requestUrl.searchParams.get("offset") ?? "0");
  const brokerFilter = requestUrl.searchParams.get("broker")?.trim();

  const limit = Math.max(1, Math.min(100, Number.isFinite(limitRaw) ? limitRaw : 30));
  const offset = Math.max(0, Number.isFinite(offsetRaw) ? offsetRaw : 0);

  let records = db.emailArchives
    .filter((entry) => entry.userId === auth.user.id)
    .sort((a, b) => new Date(b.ingestedAt).getTime() - new Date(a.ingestedAt).getTime());

  if (brokerFilter) {
    records = records.filter((entry) => entry.broker === brokerFilter);
  }

  const sliced = records.slice(offset, offset + limit);

  sendJson(req, res, 200, {
    total: records.length,
    limit,
    offset,
    items: sliced.map(makeArchivePublicRecord)
  });
}

async function handleDownloadArchiveRaw(req, res, auth, archiveId) {
  const record = findArchiveForUser(auth.user.id, archiveId);
  if (!record) {
    sendJson(req, res, 404, { error: "Archive not found" });
    return;
  }

  const downloadName = `${sanitizeFileName(record.subject || archiveId)}.eml`;
  if (record.emlContentBase64) {
    sendBufferFile(req, res, Buffer.from(record.emlContentBase64, "base64"), {
      contentType: "message/rfc822",
      downloadName
    });
    return;
  }

  if (!record.emlRelativePath) {
    sendJson(req, res, 404, { error: "Archive source file not available" });
    return;
  }

  const absolutePath = path.join(dataDir, record.emlRelativePath);
  await sendStoredFile(req, res, absolutePath, {
    contentType: "message/rfc822",
    downloadName
  });
}

async function handleDownloadArchiveAttachment(req, res, auth, archiveId, attachmentIndexRaw) {
  const record = findArchiveForUser(auth.user.id, archiveId);
  if (!record) {
    sendJson(req, res, 404, { error: "Archive not found" });
    return;
  }

  const index = Number(attachmentIndexRaw);
  if (!Number.isInteger(index) || index < 0 || index >= record.attachments.length) {
    sendJson(req, res, 404, { error: "Attachment not found" });
    return;
  }

  const attachment = record.attachments[index];
  if (attachment.contentBase64) {
    sendBufferFile(req, res, Buffer.from(attachment.contentBase64, "base64"), {
      contentType: attachment.mimeType || "application/octet-stream",
      downloadName: attachment.filename
    });
    return;
  }

  if (!attachment.relativePath) {
    sendJson(req, res, 404, { error: "Attachment file not available" });
    return;
  }

  const absolutePath = path.join(dataDir, attachment.relativePath);

  await sendStoredFile(req, res, absolutePath, {
    contentType: attachment.mimeType || "application/octet-stream",
    downloadName: attachment.filename || attachment.diskName
  });
}

async function handleCreateShareLinks(req, res, auth, archiveId) {
  const record = findArchiveForUser(auth.user.id, archiveId);
  if (!record) {
    sendJson(req, res, 404, { error: "Archive not found" });
    return;
  }

  const body = await readJsonBody(req);
  const expiresHours = Math.max(1, Math.min(168, Number(body.expiresHours ?? 24)));
  const base = getPublicApiBase(req);

  const rawToken = createShareToken({
    archiveId,
    kind: "raw",
    index: -1,
    expiresHours
  });

  const attachments = record.attachments.map((attachment, index) => {
    const token = createShareToken({
      archiveId,
      kind: "attachment",
      index,
      expiresHours
    });

    return {
      index,
      filename: attachment.filename,
      url: `${base}/api/shared/file?token=${encodeURIComponent(token)}`
    };
  });

  sendJson(req, res, 200, {
    expiresHours,
    raw: {
      url: `${base}/api/shared/file?token=${encodeURIComponent(rawToken)}`
    },
    attachments
  });
}

async function handleSharedFileDownload(req, res, requestUrl) {
  const token = requestUrl.searchParams.get("token") || "";
  const payload = verifySignedPayload(token);

  if (!payload || payload.typ !== "share_file") {
    sendJson(req, res, 401, { error: "Invalid share token" });
    return;
  }

  const archive = findArchiveById(payload.aid);
  if (!archive) {
    sendJson(req, res, 404, { error: "Archive not found" });
    return;
  }

  if (payload.kind === "raw") {
    const downloadName = `${sanitizeFileName(archive.subject || archive.id)}.eml`;
    if (archive.emlContentBase64) {
      sendBufferFile(req, res, Buffer.from(archive.emlContentBase64, "base64"), {
        contentType: "message/rfc822",
        downloadName
      });
      return;
    }

    if (!archive.emlRelativePath) {
      sendJson(req, res, 404, { error: "Archive source file not available" });
      return;
    }

    const absolutePath = path.join(dataDir, archive.emlRelativePath);
    await sendStoredFile(req, res, absolutePath, {
      contentType: "message/rfc822",
      downloadName
    });
    return;
  }

  if (payload.kind === "attachment") {
    const index = Number(payload.idx);
    const attachment = archive.attachments[index];
    if (!attachment) {
      sendJson(req, res, 404, { error: "Attachment not found" });
      return;
    }

    if (attachment.contentBase64) {
      sendBufferFile(req, res, Buffer.from(attachment.contentBase64, "base64"), {
        contentType: attachment.mimeType || "application/octet-stream",
        downloadName: attachment.filename
      });
      return;
    }

    if (!attachment.relativePath) {
      sendJson(req, res, 404, { error: "Attachment file not available" });
      return;
    }

    const absolutePath = path.join(dataDir, attachment.relativePath);
    await sendStoredFile(req, res, absolutePath, {
      contentType: attachment.mimeType || "application/octet-stream",
      downloadName: attachment.filename || attachment.diskName
    });
    return;
  }

  sendJson(req, res, 400, { error: "Unsupported shared file type" });
}

const server = createServer(async (req, res) => {
  const method = req.method ?? "GET";
  const requestUrl = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
  const pathName = requestUrl.pathname;
  const segments = pathName.split("/").filter(Boolean);

  if (method === "OPTIONS") {
    applyCorsHeaders(req, res);
    res.statusCode = 204;
    res.end();
    return;
  }

  try {
    if (method === "GET" && pathName === "/") {
      sendJson(req, res, 200, {
        message: "API is running",
        docs: [
          "GET /health",
          "GET /api/health",
          "GET /api/status",
          "GET /api/auth/google/url",
          "GET /api/auth/google/callback",
          "GET /api/auth/me",
          "GET /api/gmail/labels",
          "GET/PUT /api/gmail/preferences",
          "POST /api/gmail/ingest",
          "POST /api/pipeline/reset",
          "GET /api/email-archives",
          "POST /api/extraction/runs",
          "GET /api/extraction/runs",
          "GET /api/extraction/runs/:runId/status",
          "POST /api/extraction/runs/:runId/abort",
          "GET /api/extracted-reports",
          "GET /api/company-updates",
          "GET /api/digest/preview",
          "POST /api/digest/send"
        ]
      });
      return;
    }

    if (method === "GET" && (pathName === "/health" || pathName === "/api/health")) {
      handleHealth(req, res);
      return;
    }

    if (method === "GET" && pathName === "/api/status") {
      handleStatus(req, res);
      return;
    }

    if (method === "GET" && pathName === "/api/notes") {
      handleGetNotes(req, res);
      return;
    }

    if (method === "POST" && pathName === "/api/notes") {
      await handleCreateNote(req, res);
      return;
    }

    if (method === "POST" && pathName === "/api/jobs/daily") {
      await handleCronRun(req, res);
      return;
    }

    if (method === "POST" && pathName === "/api/internal/worker-heartbeat") {
      await handleWorkerHeartbeat(req, res);
      return;
    }

    if (method === "GET" && pathName === "/api/auth/google/url") {
      await handleGoogleAuthUrl(req, res, requestUrl);
      return;
    }

    if (method === "GET" && pathName === "/api/auth/google/callback") {
      await handleGoogleAuthCallback(req, res, requestUrl);
      return;
    }

    if (method === "GET" && pathName === "/api/shared/file") {
      await handleSharedFileDownload(req, res, requestUrl);
      return;
    }

    if (pathName === "/api/auth/me") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleAuthMe(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/auth/logout") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "POST") {
        await handleAuthLogout(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/gmail/connection") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleGetGmailConnection(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/gmail/labels") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleGetGmailLabels(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/gmail/preferences") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleGetGmailPreferences(req, res, auth);
        return;
      }
      if (method === "PUT") {
        await handlePutGmailPreferences(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/gmail/ingest") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "POST") {
        await handleGmailIngest(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/pipeline/reset") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "POST") {
        await handleResetPipelineData(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/extraction/runs") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "POST") {
        await handleTriggerExtractionRun(req, res, auth);
        return;
      }

      if (method === "GET") {
        await handleListExtractionRuns(req, res, auth, requestUrl);
        return;
      }
    }

    if (
      segments.length === 5 &&
      segments[0] === "api" &&
      segments[1] === "extraction" &&
      segments[2] === "runs" &&
      segments[4] === "abort"
    ) {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "POST") {
        await handleAbortExtractionRun(req, res, auth, segments[3]);
        return;
      }
    }

    if (
      segments.length === 5 &&
      segments[0] === "api" &&
      segments[1] === "extraction" &&
      segments[2] === "runs" &&
      segments[4] === "status"
    ) {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "GET") {
        await handleGetExtractionRunStatus(req, res, auth, segments[3]);
        return;
      }
    }

    if (pathName === "/api/extracted-reports") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "GET") {
        await handleListExtractedReports(req, res, auth, requestUrl);
        return;
      }
    }

    if (pathName === "/api/company-updates") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "GET") {
        await handleListCompanyUpdates(req, res, auth, requestUrl);
        return;
      }
    }

    if (pathName === "/api/digest/preview") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleDigestPreview(req, res, auth, requestUrl);
        return;
      }
    }

    if (pathName === "/api/digest/send") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "POST") {
        await handleDigestSend(req, res, auth);
        return;
      }
    }

    if (pathName === "/api/email-archives") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }
      if (method === "GET") {
        await handleListEmailArchives(req, res, auth, requestUrl);
        return;
      }
    }

    if (segments.length === 4 && segments[0] === "api" && segments[1] === "email-archives" && segments[3] === "raw") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "GET") {
        await handleDownloadArchiveRaw(req, res, auth, segments[2]);
        return;
      }
    }

    if (
      segments.length === 5 &&
      segments[0] === "api" &&
      segments[1] === "email-archives" &&
      segments[3] === "attachments"
    ) {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "GET") {
        await handleDownloadArchiveAttachment(req, res, auth, segments[2], segments[4]);
        return;
      }
    }

    if (segments.length === 4 && segments[0] === "api" && segments[1] === "email-archives" && segments[3] === "share-links") {
      const auth = requireAuth(req, res);
      if (!auth) {
        return;
      }

      if (method === "POST") {
        await handleCreateShareLinks(req, res, auth, segments[2]);
        return;
      }
    }

    sendJson(req, res, 404, { error: "Not found" });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    log("Request failed", { method, path: pathName, message });
    sendJson(req, res, 400, { error: message });
  }
});

server.listen(port, "0.0.0.0", () => {
  log(`Listening on port ${port}`);
});

process.on("SIGTERM", () => {
  log("SIGTERM received, shutting down");
  server.close(() => process.exit(0));
});

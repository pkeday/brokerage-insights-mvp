import { createExtractionAdapter } from "./extraction-adapter.js";
import { redactPiiText } from "../extraction/pii-redaction.js";

const RUN_STATUS = {
  QUEUED: "queued",
  RUNNING: "running",
  COMPLETED: "completed",
  FAILED: "failed",
  ABORTED: "aborted"
};

const ALLOWED_RUN_STATUSES = new Set(Object.values(RUN_STATUS));
const TERMINAL_RUN_STATUSES = new Set([RUN_STATUS.COMPLETED, RUN_STATUS.FAILED, RUN_STATUS.ABORTED]);
const MAX_FAILURE_SAMPLES = 25;
const SIMILARITY_STOP_WORDS = new Set([
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
  "into",
  "over",
  "under",
  "after",
  "before",
  "update",
  "report",
  "broker",
  "note"
]);

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function toIsoDate(value) {
  const parsed = new Date(String(value ?? ""));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function toDayBucket(value) {
  const iso = toIsoDate(value);
  if (!iso) {
    return "unknown-day";
  }
  return iso.slice(0, 10);
}

function normalizeKey(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function toEpoch(value) {
  const parsed = new Date(String(value ?? "")).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function toSimilarityTokens(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter((token) => token.length >= 3 && !SIMILARITY_STOP_WORDS.has(token));
}

function jaccardSimilarity(leftText, rightText) {
  const leftSet = new Set(toSimilarityTokens(leftText));
  const rightSet = new Set(toSimilarityTokens(rightText));

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

function buildDuplicateKey(report) {
  const pieces = [
    normalizeKey(report.userId),
    normalizeKey(report.broker),
    normalizeKey(report.companyCanonical),
    normalizeKey(report.title),
    normalizeKey(report.reportType),
    toDayBucket(report.publishedAt)
  ];
  return pieces.join("|");
}

function sortByNewest(items, key) {
  return [...items].sort((a, b) => {
    const left = new Date(a?.[key] ?? 0).getTime();
    const right = new Date(b?.[key] ?? 0).getTime();
    return right - left;
  });
}

function clampNumber(value, minimum, maximum, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const normalized = Math.floor(parsed);
  return Math.max(minimum, Math.min(maximum, normalized));
}

function ensureDbShape(db) {
  if (!Array.isArray(db.extractionRuns)) {
    db.extractionRuns = [];
  }
  if (!Array.isArray(db.extractedReports)) {
    db.extractedReports = [];
  }
}

function makeRunPublic(run) {
  return {
    id: run.id,
    userId: run.userId,
    status: run.status,
    trigger: run.trigger,
    adapterSource: run.adapterSource ?? null,
    filters: run.filters,
    stats: run.stats,
    error: run.error ?? null,
    failureSamples: run.failureSamples ?? [],
    createdAt: run.createdAt,
    startedAt: run.startedAt ?? null,
    abortRequestedAt: run.abortRequestedAt ?? null,
    abortedAt: run.abortedAt ?? null,
    abortReason: run.abortReason ?? null,
    abortPending: Boolean(run.abortRequestedAt && !run.abortedAt && run.status === RUN_STATUS.RUNNING),
    completedAt: run.completedAt ?? null,
    updatedAt: run.updatedAt
  };
}

function makeReportPublic(report) {
  const redactedTitle = redactPiiText(report.title);
  const redactedSummary = redactPiiText(report.summary);
  const redactedKeyPoints = Array.isArray(report.keyPoints)
    ? report.keyPoints.map((entry) => redactPiiText(entry)).filter(Boolean)
    : [];

  return {
    id: report.id,
    runId: report.runId,
    archiveId: report.archiveId,
    userId: report.userId,
    broker: report.broker,
    companyCanonical: report.companyCanonical,
    companyRaw: report.companyRaw,
    reportType: report.reportType,
    title: redactedTitle,
    summary: redactedSummary,
    keyPoints: redactedKeyPoints,
    publishedAt: report.publishedAt,
    confidence: report.confidence,
    duplicateKey: report.duplicateKey,
    duplicateOfReportId: report.duplicateOfReportId ?? null,
    dedupeMethod: report.dedupeMethod ?? null,
    createdAt: report.createdAt
  };
}

function normalizeReportPayload(raw, context) {
  const normalized = {
    id: context.newId("xrep"),
    runId: context.runId,
    archiveId: context.archive.id,
    userId: context.userId,
    broker: normalizeText(raw.broker) || "Unmapped Broker",
    companyCanonical: normalizeText(raw.companyCanonical) || "Unknown Company",
    companyRaw: normalizeText(raw.companyRaw) || "Unknown Company",
    reportType: normalizeText(raw.reportType) || "broker_note",
    title: redactPiiText(
      normalizeText(raw.title) || normalizeText(context.archive.subject) || "Untitled brokerage report"
    ),
    summary: redactPiiText(normalizeText(raw.summary) || "No summary available."),
    keyPoints: Array.isArray(raw.keyPoints)
      ? raw.keyPoints.map((entry) => redactPiiText(normalizeText(entry))).filter(Boolean).slice(0, 10)
      : [],
    publishedAt: toIsoDate(raw.publishedAt) || toIsoDate(context.archive.dateHeader) || context.nowIso,
    confidence: (() => {
      const confidence = Number(raw.confidence);
      if (!Number.isFinite(confidence)) {
        return 0.25;
      }
      return Math.max(0, Math.min(1, confidence));
    })(),
    duplicateKey: "",
    duplicateOfReportId: null,
    dedupeMethod: null,
    createdAt: context.nowIso,
    updatedAt: context.nowIso
  };

  normalized.duplicateKey = normalizeText(raw.duplicateKey) || buildDuplicateKey(normalized);

  return normalized;
}

function findSemanticDuplicate(existingReports, report) {
  const reportPublishedAt = toEpoch(report.publishedAt);

  for (const existing of existingReports) {
    if (!existing || existing.duplicateOfReportId) {
      continue;
    }

    if (normalizeKey(existing.userId) !== normalizeKey(report.userId)) {
      continue;
    }
    if (normalizeKey(existing.broker) !== normalizeKey(report.broker)) {
      continue;
    }
    if (normalizeKey(existing.companyCanonical) !== normalizeKey(report.companyCanonical)) {
      continue;
    }

    const existingPublishedAt = toEpoch(existing.publishedAt);
    const dayDistance = Math.abs(reportPublishedAt - existingPublishedAt) / (24 * 60 * 60 * 1000);
    if (dayDistance > 21) {
      continue;
    }

    const summaryScore = jaccardSimilarity(existing.summary, report.summary);
    const titleScore = jaccardSimilarity(existing.title, report.title);

    if (summaryScore >= 0.78) {
      return existing;
    }
    if (summaryScore >= 0.62 && titleScore >= 0.55) {
      return existing;
    }
  }

  return null;
}

export async function createExtractionOrchestrator(options) {
  const getDb = options?.getDb;
  const persistDb = options?.persistDb;
  const newId = options?.newId;
  const log = typeof options?.log === "function" ? options.log : () => {};

  if (typeof getDb !== "function") {
    throw new Error("createExtractionOrchestrator requires a getDb function");
  }
  if (typeof persistDb !== "function") {
    throw new Error("createExtractionOrchestrator requires a persistDb function");
  }
  if (typeof newId !== "function") {
    throw new Error("createExtractionOrchestrator requires a newId function");
  }

  const adapter =
    options.adapter && typeof options.adapter.extractFromArchive === "function"
      ? options.adapter
      : await createExtractionAdapter({ log });
  const userRunChains = new Map();

  function withDb() {
    const db = getDb();
    ensureDbShape(db);
    return db;
  }

  async function persistState() {
    await persistDb();
  }

  function markRunAborted(run, timestamp = nowIso(), reason = "Aborted by user") {
    const normalizedReason = normalizeText(reason) || "Aborted by user";
    run.status = RUN_STATUS.ABORTED;
    run.abortRequestedAt = run.abortRequestedAt || timestamp;
    run.abortReason = normalizedReason;
    run.error = normalizedReason;
    run.abortedAt = timestamp;
    run.completedAt = timestamp;
    run.updatedAt = timestamp;
  }

  async function runExtraction(runId) {
    const db = withDb();
    const run = db.extractionRuns.find((entry) => entry.id === runId);
    if (!run) {
      return;
    }

    if (TERMINAL_RUN_STATUSES.has(run.status)) {
      return;
    }

    if (run.abortRequestedAt) {
      markRunAborted(run, nowIso(), run.abortReason || run.error || "Aborted before start");
      await persistState();
      return;
    }

    run.status = RUN_STATUS.RUNNING;
    run.startedAt = nowIso();
    run.updatedAt = run.startedAt;
    await persistState();

    try {
      const archivesById = new Map(
        db.emailArchives
          .filter((entry) => entry.userId === run.userId)
          .map((entry) => [entry.id, entry])
      );

      for (const archiveId of run.archiveIds) {
        if (run.abortRequestedAt) {
          markRunAborted(run, nowIso(), run.abortReason || "Aborted by user");
          await persistState();
          return;
        }

        const processingAt = nowIso();
        const archive = archivesById.get(archiveId);
        run.stats.processedArchives += 1;
        run.updatedAt = processingAt;

        if (!archive) {
          run.stats.skippedArchives += 1;
          await persistState();
          continue;
        }

        if (!run.options.includeAlreadyExtracted) {
          const alreadyExtracted = db.extractedReports.some(
            (entry) => entry.userId === run.userId && entry.archiveId === archive.id
          );
          if (alreadyExtracted) {
            run.stats.skippedArchives += 1;
            await persistState();
            continue;
          }
        }

        try {
          const rawReport = await adapter.extractFromArchive({
            archive,
            userId: run.userId,
            runId: run.id
          });

          if (!rawReport || typeof rawReport !== "object") {
            run.stats.skippedArchives += 1;
            await persistState();
            continue;
          }

          const report = normalizeReportPayload(rawReport, {
            archive,
            userId: run.userId,
            runId: run.id,
            newId,
            nowIso: processingAt
          });

          const existingCanonical = db.extractedReports.find(
            (entry) =>
              entry.userId === report.userId &&
              entry.duplicateKey === report.duplicateKey &&
              !entry.duplicateOfReportId
          );

          const semanticDuplicate = existingCanonical
            ? null
            : findSemanticDuplicate(db.extractedReports, report);

          if (existingCanonical && run.options.includeAlreadyExtracted) {
            existingCanonical.runId = run.id;
            existingCanonical.archiveId = report.archiveId || existingCanonical.archiveId;
            existingCanonical.broker = report.broker || existingCanonical.broker;
            existingCanonical.companyCanonical = report.companyCanonical || existingCanonical.companyCanonical;
            existingCanonical.companyRaw = report.companyRaw || existingCanonical.companyRaw;
            existingCanonical.reportType = report.reportType || existingCanonical.reportType;
            existingCanonical.title = report.title || existingCanonical.title;
            existingCanonical.summary = report.summary || existingCanonical.summary;
            existingCanonical.keyPoints =
              Array.isArray(report.keyPoints) && report.keyPoints.length > 0
                ? report.keyPoints
                : existingCanonical.keyPoints;
            existingCanonical.confidence =
              Number.isFinite(Number(report.confidence)) ? Number(report.confidence) : existingCanonical.confidence;
            existingCanonical.publishedAt = report.publishedAt || existingCanonical.publishedAt;
            existingCanonical.updatedAt = processingAt;
            run.stats.extractedReports += 1;
            await persistState();
            continue;
          }

          if (existingCanonical || semanticDuplicate) {
            const canonical = existingCanonical ?? semanticDuplicate;
            report.duplicateOfReportId = canonical.id;
            report.duplicateKey = canonical.duplicateKey || report.duplicateKey;
            report.dedupeMethod = existingCanonical ? "exact_key" : "semantic_overlap";
            run.stats.duplicateReports += 1;
          }

          db.extractedReports.push(report);
          run.stats.extractedReports += 1;
        } catch (error) {
          run.stats.failedArchives += 1;
          if (run.failureSamples.length < MAX_FAILURE_SAMPLES) {
            run.failureSamples.push({
              archiveId,
              message: error instanceof Error ? error.message : String(error)
            });
          }
        }

        await persistState();
      }

      run.status = RUN_STATUS.COMPLETED;
      run.completedAt = nowIso();
      run.updatedAt = run.completedAt;
      await persistState();
    } catch (error) {
      if (run.abortRequestedAt) {
        markRunAborted(run, nowIso(), run.abortReason || "Aborted by user");
        await persistState();
        return;
      }

      run.status = RUN_STATUS.FAILED;
      run.error = error instanceof Error ? error.message : String(error);
      run.completedAt = nowIso();
      run.updatedAt = run.completedAt;
      await persistState();
      log("Extraction run failed", { runId: run.id, message: run.error });
    }
  }

  function enqueueRun(run) {
    const previous = userRunChains.get(run.userId) ?? Promise.resolve();
    const next = previous.catch(() => undefined).then(() => runExtraction(run.id));

    userRunChains.set(run.userId, next);
    next.finally(() => {
      if (userRunChains.get(run.userId) === next) {
        userRunChains.delete(run.userId);
      }
    });
  }

  async function triggerRun(params) {
    const userId = normalizeText(params?.userId);
    if (!userId) {
      throw new Error("userId is required");
    }

    const includeAlreadyExtracted = params?.includeAlreadyExtracted === true;
    const requestedLimit = clampNumber(params?.limit, 1, 1000, 250);
    const trigger = normalizeText(params?.trigger) || "manual_api";
    const brokerFilter = normalizeText(params?.broker) || null;
    const requestedArchiveIds = Array.isArray(params?.archiveIds)
      ? params.archiveIds.map((entry) => normalizeText(entry)).filter(Boolean)
      : [];

    const db = withDb();
    const now = nowIso();

    let archives = db.emailArchives
      .filter((entry) => entry.userId === userId)
      .sort((a, b) => new Date(a.ingestedAt ?? 0).getTime() - new Date(b.ingestedAt ?? 0).getTime());

    if (requestedArchiveIds.length > 0) {
      const allowlist = new Set(requestedArchiveIds);
      archives = archives.filter((entry) => allowlist.has(entry.id));
    }

    if (brokerFilter) {
      archives = archives.filter((entry) => normalizeText(entry.broker) === brokerFilter);
    }

    archives = archives.slice(0, requestedLimit);

    const run = {
      id: newId("xrun"),
      userId,
      status: RUN_STATUS.QUEUED,
      trigger,
      adapterSource: adapter.source,
      filters: {
        broker: brokerFilter,
        requestedArchiveIds: requestedArchiveIds.length > 0 ? requestedArchiveIds : null,
        limit: requestedLimit,
        includeAlreadyExtracted
      },
      options: {
        includeAlreadyExtracted
      },
      archiveIds: archives.map((entry) => entry.id),
      stats: {
        candidateArchives: archives.length,
        processedArchives: 0,
        extractedReports: 0,
        skippedArchives: 0,
        failedArchives: 0,
        duplicateReports: 0
      },
      failureSamples: [],
      error: null,
      createdAt: now,
      startedAt: null,
      abortRequestedAt: null,
      abortedAt: null,
      abortReason: null,
      completedAt: null,
      updatedAt: now
    };

    db.extractionRuns.push(run);
    await persistState();
    enqueueRun(run);

    return makeRunPublic(run);
  }

  function listRuns(params) {
    const userId = normalizeText(params?.userId);
    if (!userId) {
      throw new Error("userId is required");
    }

    const statusFilterRaw = normalizeText(params?.status).toLowerCase();
    const statusFilter = statusFilterRaw || null;
    if (statusFilter && !ALLOWED_RUN_STATUSES.has(statusFilter)) {
      throw new Error("Invalid run status filter");
    }

    const limit = clampNumber(params?.limit, 1, 100, 20);
    const offset = clampNumber(params?.offset, 0, 10_000, 0);
    const db = withDb();

    let runs = db.extractionRuns.filter((entry) => entry.userId === userId);
    if (statusFilter) {
      runs = runs.filter((entry) => entry.status === statusFilter);
    }

    const ordered = sortByNewest(runs, "createdAt");
    return {
      total: ordered.length,
      limit,
      offset,
      items: ordered.slice(offset, offset + limit).map(makeRunPublic)
    };
  }

  function getRunStatus(params) {
    const userId = normalizeText(params?.userId);
    const runId = normalizeText(params?.runId);
    if (!userId || !runId) {
      throw new Error("userId and runId are required");
    }

    const db = withDb();
    const run = db.extractionRuns.find((entry) => entry.userId === userId && entry.id === runId);
    return run ? makeRunPublic(run) : null;
  }

  async function abortRun(params) {
    const userId = normalizeText(params?.userId);
    const runId = normalizeText(params?.runId);
    if (!userId || !runId) {
      throw new Error("userId and runId are required");
    }

    const db = withDb();
    const run = db.extractionRuns.find((entry) => entry.userId === userId && entry.id === runId);
    if (!run) {
      return null;
    }

    if (TERMINAL_RUN_STATUSES.has(run.status)) {
      return {
        accepted: false,
        immediate: false,
        alreadyTerminal: true,
        run: makeRunPublic(run)
      };
    }

    const timestamp = nowIso();
    const reason = normalizeText(params?.reason) || "Aborted by user";
    run.abortRequestedAt = run.abortRequestedAt || timestamp;
    run.abortReason = reason;
    run.updatedAt = timestamp;

    let immediate = false;
    if (run.status === RUN_STATUS.QUEUED) {
      markRunAborted(run, timestamp, reason);
      immediate = true;
    }

    await persistState();
    return {
      accepted: true,
      immediate,
      alreadyTerminal: false,
      run: makeRunPublic(run)
    };
  }

  function listReports(params) {
    const userId = normalizeText(params?.userId);
    if (!userId) {
      throw new Error("userId is required");
    }

    const limit = clampNumber(params?.limit, 1, 100, 30);
    const offset = clampNumber(params?.offset, 0, 10_000, 0);
    const broker = normalizeText(params?.broker) || null;
    const reportType = normalizeText(params?.reportType) || null;
    const runId = normalizeText(params?.runId) || null;
    const company = normalizeText(params?.company).toLowerCase();
    const query = normalizeText(params?.query).toLowerCase();
    const includeDuplicates = params?.includeDuplicates !== false;
    const publishedFrom = toIsoDate(params?.publishedFrom);
    const publishedTo = toIsoDate(params?.publishedTo);

    if (params?.publishedFrom && !publishedFrom) {
      throw new Error("Invalid publishedFrom date");
    }
    if (params?.publishedTo && !publishedTo) {
      throw new Error("Invalid publishedTo date");
    }
    if (publishedFrom && publishedTo && publishedFrom > publishedTo) {
      throw new Error("publishedFrom cannot be after publishedTo");
    }

    const db = withDb();
    let reports = db.extractedReports.filter((entry) => entry.userId === userId);

    if (!includeDuplicates) {
      reports = reports.filter((entry) => !entry.duplicateOfReportId);
    }

    if (broker) {
      reports = reports.filter((entry) => normalizeText(entry.broker) === broker);
    }
    if (reportType) {
      reports = reports.filter((entry) => normalizeText(entry.reportType) === reportType);
    }
    if (runId) {
      reports = reports.filter((entry) => entry.runId === runId);
    }
    if (company) {
      reports = reports.filter((entry) => {
        const canonical = normalizeText(entry.companyCanonical).toLowerCase();
        const raw = normalizeText(entry.companyRaw).toLowerCase();
        return canonical.includes(company) || raw.includes(company);
      });
    }
    if (query) {
      reports = reports.filter((entry) => {
        const haystack = [
          normalizeText(entry.title),
          normalizeText(entry.summary),
          ...(Array.isArray(entry.keyPoints) ? entry.keyPoints.map((point) => normalizeText(point)) : [])
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(query);
      });
    }
    if (publishedFrom) {
      reports = reports.filter((entry) => normalizeText(entry.publishedAt) >= publishedFrom);
    }
    if (publishedTo) {
      reports = reports.filter((entry) => normalizeText(entry.publishedAt) <= publishedTo);
    }

    const ordered = sortByNewest(reports, "publishedAt");
    return {
      total: ordered.length,
      limit,
      offset,
      items: ordered.slice(offset, offset + limit).map(makeReportPublic)
    };
  }

  return {
    triggerRun,
    listRuns,
    getRunStatus,
    abortRun,
    listReports
  };
}

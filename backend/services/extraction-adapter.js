import { access } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const EXTERNAL_EXTRACTION_MODULE_CANDIDATES = [
  "extraction/index.js",
  "extraction/core.js",
  "extraction/extract.js"
];

const REPORT_TYPE_RULES = [
  { type: "result_update", patterns: ["result", "q1", "q2", "q3", "q4", "earnings", "outlook"] },
  { type: "initiating_coverage", patterns: ["initiating", "initiation", "coverage"] },
  { type: "target_price_change", patterns: ["target price", "tp change", "pt"] },
  { type: "rating_change", patterns: ["upgrade", "downgrade", "rating change"] },
  { type: "annual_report", patterns: ["annual report"] },
  { type: "auditors_report", patterns: ["auditor", "audit"] }
];

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeTitle(value) {
  const text = normalizeText(value);
  return text || "Untitled brokerage report";
}

function normalizeSummary(value, fallback) {
  const source = normalizeText(value) || normalizeText(fallback);
  if (!source) {
    return "No summary available.";
  }
  return source.slice(0, 3000);
}

function sanitizeKeyPoints(value, fallbackSummary) {
  if (Array.isArray(value)) {
    const cleaned = value.map((entry) => normalizeText(entry)).filter(Boolean).slice(0, 10);
    if (cleaned.length > 0) {
      return cleaned;
    }
  }

  const summary = normalizeText(fallbackSummary);
  if (!summary) {
    return [];
  }

  return summary
    .split(/(?<=[.!?])\s+/)
    .map((entry) => normalizeText(entry))
    .filter(Boolean)
    .slice(0, 3);
}

function normalizeConfidence(value, fallback = 0.25) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(0, Math.min(1, numeric));
}

function normalizeCompanyRaw(value, fallback) {
  const text = normalizeText(value) || normalizeText(fallback);
  return text || "Unknown Company";
}

function normalizeCompanyCanonical(value, fallbackRaw) {
  const seed = normalizeText(value) || normalizeText(fallbackRaw);
  if (!seed) {
    return "unknown-company";
  }

  return seed
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120) || "unknown-company";
}

function normalizePublishedAt(value, fallback) {
  const tryValues = [value, fallback, new Date().toISOString()];
  for (const candidate of tryValues) {
    const parsed = new Date(String(candidate ?? ""));
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  }
  return new Date().toISOString();
}

function detectReportType(subject, bodyPreview) {
  const haystack = `${normalizeText(subject)} ${normalizeText(bodyPreview)}`.toLowerCase();
  for (const rule of REPORT_TYPE_RULES) {
    if (rule.patterns.some((pattern) => haystack.includes(pattern))) {
      return rule.type;
    }
  }
  return "broker_note";
}

function guessCompanyFromSubject(subject) {
  const cleaned = normalizeText(subject);
  if (!cleaned) {
    return "Unknown Company";
  }

  const match =
    cleaned.match(/(?:on|of|for)\s+([A-Za-z0-9&.,()' -]{2,80})/i) ??
    cleaned.match(/^([A-Za-z0-9&.,()' -]{2,80})[:\-]/);
  if (match?.[1]) {
    return normalizeText(match[1]);
  }

  return cleaned.slice(0, 80);
}

function createFallbackExtractionResult({ archive, userId }) {
  const companyRaw = guessCompanyFromSubject(archive.subject);
  const summary = normalizeSummary(archive.bodyPreview, archive.snippet);
  return {
    archiveId: archive.id,
    userId,
    broker: normalizeText(archive.broker) || "Unmapped Broker",
    companyRaw,
    companyCanonical: normalizeCompanyCanonical(null, companyRaw),
    reportType: detectReportType(archive.subject, archive.bodyPreview || archive.snippet),
    title: normalizeTitle(archive.subject),
    summary,
    keyPoints: sanitizeKeyPoints(null, summary),
    publishedAt: normalizePublishedAt(archive.dateHeader, archive.ingestedAt),
    confidence: 0.25
  };
}

function pickExtractor(moduleValue) {
  const candidates = [
    moduleValue?.extractArchiveReport,
    moduleValue?.extractFromArchive,
    moduleValue?.extractReport,
    moduleValue?.default
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "function") {
      return candidate;
    }
  }

  if (moduleValue?.default && typeof moduleValue.default.extractArchiveReport === "function") {
    return moduleValue.default.extractArchiveReport;
  }

  return null;
}

function normalizeExtractorResult(result, context) {
  if (!result || typeof result !== "object") {
    return createFallbackExtractionResult(context);
  }

  const archive = context.archive;
  const fallback = createFallbackExtractionResult(context);
  const companyRaw = normalizeCompanyRaw(result.companyRaw, fallback.companyRaw);
  const companyCanonical = normalizeCompanyCanonical(result.companyCanonical, companyRaw);
  const summary = normalizeSummary(result.summary, fallback.summary);

  return {
    archiveId: archive.id,
    userId: context.userId,
    broker: normalizeText(result.broker) || fallback.broker,
    companyRaw,
    companyCanonical,
    reportType: normalizeText(result.reportType) || fallback.reportType,
    title: normalizeTitle(result.title || archive.subject),
    summary,
    keyPoints: sanitizeKeyPoints(result.keyPoints, summary),
    publishedAt: normalizePublishedAt(result.publishedAt, fallback.publishedAt),
    confidence: normalizeConfidence(result.confidence, fallback.confidence)
  };
}

async function tryImportModule(absolutePath) {
  try {
    await access(absolutePath);
  } catch {
    return null;
  }

  try {
    return await import(pathToFileURL(absolutePath).href);
  } catch {
    return null;
  }
}

export async function createExtractionAdapter(options = {}) {
  const log = typeof options.log === "function" ? options.log : () => {};
  const backendRootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const candidateModules = Array.isArray(options.candidateModules) && options.candidateModules.length > 0
    ? options.candidateModules
    : EXTERNAL_EXTRACTION_MODULE_CANDIDATES;

  for (const relativePath of candidateModules) {
    const absolutePath = path.resolve(backendRootDir, relativePath);
    const moduleValue = await tryImportModule(absolutePath);
    if (!moduleValue) {
      continue;
    }

    const extractor = pickExtractor(moduleValue);
    if (!extractor) {
      continue;
    }

    log("Using external extraction adapter", { modulePath: relativePath });
    return {
      source: `module:${relativePath}`,
      async extractFromArchive(context) {
        const result = await extractor({
          archive: context.archive,
          userId: context.userId,
          runId: context.runId
        });
        return normalizeExtractorResult(result, context);
      }
    };
  }

  log("No external extraction module found. Using fallback extraction adapter.");
  return {
    source: "fallback:heuristic",
    async extractFromArchive(context) {
      return createFallbackExtractionResult(context);
    }
  };
}

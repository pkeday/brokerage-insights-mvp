import { redactPiiText } from "../extraction/pii-redaction.js";
import { canonicalizeCompanyName } from "../normalization/company.js";
import { normalizeWhitespace, truncateText, uniqueStrings } from "../normalization/text.js";

const REPORT_TYPE_NORMALIZATION_RULES = [
  { value: "initiating_coverage", patterns: ["initiating", "initiation", "coverage initiation", "initiating coverage"] },
  { value: "results_update", patterns: ["results update", "result update", "earnings", "q1", "q2", "q3", "q4"] },
  { value: "general_update", patterns: ["general update", "update"] },
  { value: "sector_update", patterns: ["sector", "industry update", "universe"] },
  { value: "target_price_change", patterns: ["target price", "tp", "pt", "price target"] },
  { value: "rating_change", patterns: ["upgrade", "downgrade", "rating change", "reiterate", "reiterated"] },
  { value: "management_commentary", patterns: ["management commentary", "concall", "earnings call", "management"] },
  { value: "corporate_action", patterns: ["buyback", "split", "dividend", "merger", "demerger"] },
  { value: "other", patterns: ["other"] }
];

const DECISION_NORMALIZATION_RULES = [
  { value: "BUY", patterns: ["buy", "strong buy", "outperform", "overweight", "accumulate"] },
  { value: "SELL", patterns: ["sell", "strong sell", "underperform", "underweight"] },
  { value: "HOLD", patterns: ["hold", "neutral", "market perform", "equal weight"] },
  { value: "ADD", patterns: ["add", "increase", "add on dips"] },
  { value: "REDUCE", patterns: ["reduce", "trim", "lighten"] }
];

const DEFAULT_ALLOWED_REPORT_TYPES = new Set([
  "initiating_coverage",
  "results_update",
  "general_update",
  "sector_update",
  "target_price_change",
  "rating_change",
  "management_commentary",
  "corporate_action",
  "other"
]);

const SUMMARY_MAX_LENGTH = 320;
const TITLE_MAX_LENGTH = 220;
const KEY_INSIGHT_MAX_LENGTH = 180;
const OPENAI_BASE_URL = "https://api.openai.com/v1";
const OPENAI_PRIMARY_MODEL = "gpt-5-mini";

function parseResponseText(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }

  if (typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  if (!Array.isArray(payload.output)) {
    return "";
  }

  const chunks = [];
  for (const outputEntry of payload.output) {
    const contentParts = Array.isArray(outputEntry?.content) ? outputEntry.content : [];
    for (const content of contentParts) {
      if (content?.type === "output_text" && typeof content.text === "string") {
        chunks.push(content.text);
      }
    }
  }

  return chunks.join("\n").trim();
}

function parseJsonObject(rawText) {
  const normalized = normalizeWhitespace(rawText);
  if (!normalized) {
    return null;
  }

  const blockMatch = rawText.match(/\{[\s\S]*\}/);
  const candidate = blockMatch ? blockMatch[0] : rawText;
  try {
    const parsed = JSON.parse(candidate);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeReportType(rawValue) {
  const normalized = normalizeWhitespace(rawValue).toLowerCase();
  if (!normalized) {
    return "other";
  }

  for (const rule of REPORT_TYPE_NORMALIZATION_RULES) {
    if (rule.patterns.some((pattern) => normalized.includes(pattern))) {
      return rule.value;
    }
  }

  const compact = normalized.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  if (DEFAULT_ALLOWED_REPORT_TYPES.has(compact)) {
    return compact;
  }

  return "other";
}

function normalizeDecision(rawValue) {
  const normalized = normalizeWhitespace(rawValue).toLowerCase();
  if (!normalized) {
    return "UNKNOWN";
  }

  for (const rule of DECISION_NORMALIZATION_RULES) {
    if (rule.patterns.some((pattern) => normalized.includes(pattern))) {
      return rule.value;
    }
  }

  return "UNKNOWN";
}

function normalizeIsoDate(value) {
  const text = normalizeWhitespace(value);
  if (!text) {
    return null;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function toFiniteConfidence(value, fallback = 0.45) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(0, Math.min(1, numeric));
}

function parseSenderIdentity(rawSender) {
  const sender = normalizeWhitespace(rawSender);
  if (!sender) {
    return { senderName: "", senderEmail: "" };
  }

  const emailMatch = sender.match(/\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b/i);
  const senderEmail = normalizeWhitespace(emailMatch?.[0] || "").toLowerCase();
  const senderName = normalizeWhitespace(
    sender
      .replace(/<[^>]+>/g, " ")
      .replace(/\([^)]*\)/g, " ")
      .replace(/\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b/gi, " ")
      .replace(/"/g, " ")
  );

  return { senderName, senderEmail };
}

function redactForPipeline(text, context) {
  return redactPiiText(text, {
    senderName: normalizeWhitespace(context?.senderName),
    senderEmail: normalizeWhitespace(context?.senderEmail).toLowerCase()
  });
}

function isLowQualitySummary(value) {
  const summary = normalizeWhitespace(value);
  if (!summary) {
    return true;
  }

  if (summary.length < 36) {
    return true;
  }

  if (/id=\d{4,}|message-id|unsubscribe|click here|http[s]?:\/\/|d=[a-z0-9_-]{8,}/i.test(summary)) {
    return true;
  }

  if ((summary.match(/[<>]/g) || []).length >= 2) {
    return true;
  }

  return false;
}

function normalizeInsights(value, fallbackSummary, context) {
  const items = Array.isArray(value) ? value : [];
  const normalized = uniqueStrings(
    items
      .map((entry) => truncateText(redactForPipeline(normalizeWhitespace(entry), context), KEY_INSIGHT_MAX_LENGTH))
      .filter((entry) => entry.length >= 16),
    3
  );

  if (normalized.length > 0) {
    return normalized;
  }

  const fallback = truncateText(redactForPipeline(normalizeWhitespace(fallbackSummary), context), KEY_INSIGHT_MAX_LENGTH);
  return fallback ? [fallback] : [];
}

function normalizeCompany(record) {
  const companyRaw = normalizeWhitespace(record?.companyRaw || record?.company || "");
  const sectorName = normalizeWhitespace(record?.sectorName || record?.sector || "");
  if (companyRaw) {
    return {
      companyRaw,
      companyCanonical: canonicalizeCompanyName(record?.companyCanonical || companyRaw) || companyRaw
    };
  }

  if (sectorName) {
    const sectorCanonical = `SECTOR:${sectorName}`;
    return {
      companyRaw: sectorName,
      companyCanonical: sectorCanonical
    };
  }

  return {
    companyRaw: "Unknown Company",
    companyCanonical: "Unknown Company"
  };
}

function sanitizeRecord(record, context) {
  if (!record || typeof record !== "object") {
    return null;
  }

  const company = normalizeCompany(record);
  const reportType = normalizeReportType(record.reportType);
  const summarySource = normalizeWhitespace(record.summary || record.perspective || record.insight || "");
  const keyInsights = normalizeInsights(record.keyInsights || record.keyPoints, summarySource, context);
  let summary = truncateText(redactForPipeline(summarySource, context), SUMMARY_MAX_LENGTH);
  if (isLowQualitySummary(summary)) {
    const fallbackSummary = normalizeWhitespace(keyInsights.slice(0, 2).join(" "));
    summary = truncateText(redactForPipeline(fallbackSummary, context), SUMMARY_MAX_LENGTH);
  }
  if (!summary) {
    summary = "No summary available.";
  }
  const decision = normalizeDecision(record.decision || record.rating || record.call);

  return {
    companyRaw: company.companyRaw,
    companyCanonical: company.companyCanonical,
    reportType,
    title:
      truncateText(redactForPipeline(normalizeWhitespace(record.title), context), TITLE_MAX_LENGTH) ||
      `${company.companyCanonical} ${reportType}`.trim(),
    summary,
    keyInsights,
    decision,
    confidence: toFiniteConfidence(record.confidence, 0.45),
    publishedAt: normalizeIsoDate(record.publishedAt) || context.defaultPublishedAt,
    sectorName: normalizeWhitespace(record.sectorName || record.sector || ""),
    sourcePayload: record
  };
}

function selectBetterSummary(primary, secondary) {
  const left = normalizeWhitespace(primary);
  const right = normalizeWhitespace(secondary);
  const leftGood = !isLowQualitySummary(left);
  const rightGood = !isLowQualitySummary(right);

  if (leftGood && !rightGood) {
    return left;
  }
  if (rightGood && !leftGood) {
    return right;
  }
  if (right.length > left.length) {
    return right;
  }
  return left || right;
}

function consolidateCompanyReports(records) {
  const map = new Map();
  const ordered = Array.isArray(records) ? records : [];
  for (const record of ordered) {
    const key = normalizeWhitespace(record?.companyCanonical || record?.companyRaw || "").toLowerCase();
    if (!key) {
      continue;
    }

    if (!map.has(key)) {
      map.set(key, { ...record });
      continue;
    }

    const existing = map.get(key);
    const mergedInsights = uniqueStrings([...(existing.keyInsights || []), ...(record.keyInsights || [])], 3);
    const merged = {
      ...existing,
      keyInsights: mergedInsights,
      summary: selectBetterSummary(existing.summary, record.summary),
      confidence: Math.max(toFiniteConfidence(existing.confidence, 0.45), toFiniteConfidence(record.confidence, 0.45)),
      publishedAt: normalizeIsoDate(record.publishedAt) || existing.publishedAt
    };

    if (normalizeWhitespace(existing.reportType).toLowerCase() === "other" && normalizeWhitespace(record.reportType).toLowerCase() !== "other") {
      merged.reportType = record.reportType;
      merged.title = record.title || existing.title;
    }

    if (normalizeWhitespace(existing.decision).toUpperCase() === "UNKNOWN" && normalizeWhitespace(record.decision).toUpperCase() !== "UNKNOWN") {
      merged.decision = record.decision;
    }

    map.set(key, merged);
  }

  return Array.from(map.values()).slice(0, 25);
}

function buildSourceText(emailRecord) {
  const sections = [
    `Subject: ${normalizeWhitespace(emailRecord.subject)}`,
    `Snippet: ${normalizeWhitespace(emailRecord.snippet)}`,
    `BodyPreview: ${normalizeWhitespace(emailRecord.bodyPreview)}`,
    `Attachments: ${Array.isArray(emailRecord.attachments) ? emailRecord.attachments.map((entry) => entry?.filename).filter(Boolean).join(", ") : ""}`
  ];
  return sections.join("\n").slice(0, 12000);
}

function buildDictionaryContext(companyDictionary) {
  if (!Array.isArray(companyDictionary) || companyDictionary.length === 0) {
    return "[]";
  }
  const compact = companyDictionary
    .map((entry) => String(entry).trim())
    .filter(Boolean)
    .slice(0, 500);
  return JSON.stringify(compact);
}

export function createCompanyUpdateExtractor(options = {}) {
  const apiKey = normalizeWhitespace(options.apiKey || process.env.OPENAI_API_KEY);
  const enabled = options.enabled !== false;
  const model = normalizeWhitespace(options.model || OPENAI_PRIMARY_MODEL);
  const baseUrl = normalizeWhitespace(options.baseUrl || OPENAI_BASE_URL);
  const timeoutMsRaw = Number.parseInt(String(options.timeoutMs || "18000"), 10);
  const timeoutMs = Number.isFinite(timeoutMsRaw) ? Math.max(4000, Math.min(timeoutMsRaw, 45_000)) : 18_000;

  async function extractEmailToCompanies(params = {}) {
    const emailRecord = params.emailRecord && typeof params.emailRecord === "object" ? params.emailRecord : {};
    const defaultPublishedAt = normalizeIsoDate(emailRecord.messageDate || emailRecord.ingestedAt) || new Date().toISOString();
    const senderIdentity = parseSenderIdentity(emailRecord.sender);
    const dictionaryContext = buildDictionaryContext(params.companyDictionary);
    const sourceText = buildSourceText(emailRecord);
    const extractionContext = { defaultPublishedAt, ...senderIdentity };

    if (!enabled || !apiKey) {
      throw new Error("OPENAI_API_KEY is missing. AI extraction is required and fallback is disabled.");
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(`${baseUrl}/responses`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model,
          max_output_tokens: 1800,
          input: [
            {
              role: "system",
              content: "You extract brokerage email insights for expert users. Keep broker perspective distinct. Return strict JSON only."
            },
            {
              role: "user",
              content: `Extract all company-wise updates from this ONE brokerage email.
Return strict JSON with shape:
{"reports":[{"companyRaw":string,"companyCanonical":string,"sectorName":string,"reportType":string,"title":string,"summary":string,"keyInsights":string[],"decision":string,"confidence":number,"publishedAt":string}]}
Rules:
- One email may include multiple companies. Return one consolidated object per company (or sector).
- Allowed reportType: initiating_coverage, results_update, general_update, sector_update, target_price_change, rating_change, management_commentary, corporate_action, other.
- Decision should map to BUY/SELL/HOLD/ADD/REDUCE/UNKNOWN.
- Keep summary nuanced, specific, analytical, and short (max 280 chars). Do NOT copy email body lines verbatim.
- keyInsights max 3 bullets.
- Strip sender/receiver identities and email addresses.
- If only sector commentary exists, set companyCanonical to SECTOR:<sector>.
- If this email has no research content, return {"reports":[]}.
Broker: ${normalizeWhitespace(emailRecord.broker)}
ArchiveId: ${normalizeWhitespace(emailRecord.archiveId)}
Canonical Company Dictionary: ${dictionaryContext}
Email Content:
${sourceText}`
            }
          ]
        }),
        signal: controller.signal
      });

      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        const message = normalizeWhitespace(payload?.error?.message || payload?.error || `HTTP ${response.status}`);
        throw new Error(`OpenAI extraction failed: ${message}`);
      }

      const responseText = parseResponseText(payload);
      const parsed = parseJsonObject(responseText);
      if (!parsed || !Array.isArray(parsed.reports)) {
        throw new Error("OpenAI extraction returned invalid JSON payload.");
      }

      const sanitized = parsed.reports
        .map((record) => sanitizeRecord(record, extractionContext))
        .filter(Boolean)
        .slice(0, 50);
      const consolidated = consolidateCompanyReports(sanitized);

      if (consolidated.length === 0) {
        throw new Error("OpenAI extraction returned zero company reports.");
      }

      return {
        source: `openai:${model}`,
        reports: consolidated,
        rawResponse: parsed && typeof parsed === "object" ? parsed : {}
      };
    } finally {
      clearTimeout(timeout);
    }
  }

  return {
    enabled: enabled && Boolean(apiKey),
    model,
    source: enabled && apiKey ? `openai:${model}` : "disabled",
    extractEmailToCompanies
  };
}

import { createReportDuplicateKey } from "../dedupe/report-dedupe-key.js";
import { extractCompanyFromArchive } from "../normalization/company.js";
import { resolveArchivePublishedAt } from "../normalization/date.js";
import { normalizeWhitespace, splitSentences, truncateText, uniqueStrings } from "../normalization/text.js";
import { classifyReportType } from "./report-type-classifier.js";

const SUMMARY_NUMERIC_SIGNAL = /\b(?:\d+(?:\.\d+)?%|(?:rs\.?|inr)\s?\d[\d,.]*|\d[\d,.]*\s?(?:bps|bp|x)|q[1-4])\b/i;
const SUMMARY_BOILERPLATE_PATTERN =
  /\b(?:unsubscribe|confidential|disclaimer|intended recipient|do not reply|forwarded message|view in browser)\b/i;
const SUMMARY_ARTIFACT_PATTERN = /(?:id=|href=|src=|http|www\.|[a-z0-9_-]{20,})/i;

const REPORT_TYPE_HINTS = {
  initiation: ["initiation", "coverage", "valuation", "target", "rating"],
  results_update: ["result", "earnings", "ebitda", "margin", "guidance", "beat", "miss"],
  target_change: ["target", "valuation", "pt", "tp", "upgrade", "downgrade"],
  rating_change: ["upgrade", "downgrade", "buy", "sell", "hold"],
  general_update: ["demand", "capacity", "cost", "outlook", "update"]
};

function cleanSummarySource(record) {
  const source = normalizeWhitespace([record?.snippet, record?.bodyPreview, record?.subject].filter(Boolean).join(" "));
  return normalizeWhitespace(source.replace(/\s*[|•]\s*/g, ". "));
}

function splitSummaryCandidates(text) {
  const primary = splitSentences(text).map((entry) => normalizeWhitespace(entry)).filter(Boolean);
  if (primary.length > 1) {
    return primary;
  }

  const fallback = normalizeWhitespace(text)
    .split(/\s*[;|•]\s*|\s*,\s+/)
    .map((entry) => normalizeWhitespace(entry))
    .filter((entry) => entry.length >= 20);

  if (fallback.length > 0) {
    return fallback;
  }

  return primary;
}

function scoreSummaryCandidate(sentence, reportType) {
  if (!sentence) {
    return -10;
  }

  if (SUMMARY_BOILERPLATE_PATTERN.test(sentence)) {
    return -8;
  }

  let score = 0;
  if (SUMMARY_NUMERIC_SIGNAL.test(sentence)) {
    score += 2;
  }

  const hints = REPORT_TYPE_HINTS[reportType] ?? REPORT_TYPE_HINTS.general_update;
  if (hints.some((hint) => sentence.toLowerCase().includes(hint))) {
    score += 2;
  }

  if (SUMMARY_ARTIFACT_PATTERN.test(sentence)) {
    score -= 3;
  }

  if (sentence.length >= 45 && sentence.length <= 220) {
    score += 1;
  } else if (sentence.length < 20 || sentence.length > 280) {
    score -= 1;
  }

  return score;
}

function buildSummary(record, reportType) {
  const source = cleanSummarySource(record);
  const candidates = splitSummaryCandidates(source)
    .map((sentence) => ({
      sentence,
      score: scoreSummaryCandidate(sentence, reportType)
    }))
    .sort((left, right) => right.score - left.score);

  const best = candidates.find((entry) => entry.score >= 1)?.sentence || candidates[0]?.sentence;
  return truncateText(best || normalizeWhitespace(record?.snippet || record?.subject || ""), 360);
}

function buildKeyPoints(record, fallbackSummary, reportType, maxItems = 3) {
  const sourceText = cleanSummarySource(record) || fallbackSummary;
  const sentences = splitSummaryCandidates(sourceText)
    .map((sentence) => ({
      sentence,
      score: scoreSummaryCandidate(sentence, reportType)
    }))
    .sort((left, right) => right.score - left.score)
    .map((entry) => entry.sentence);
  const cleaned = sentences
    .map((entry) => truncateText(entry, 180))
    .filter((entry) => entry.length >= 12 && !SUMMARY_BOILERPLATE_PATTERN.test(entry));

  const unique = uniqueStrings(cleaned, maxItems);
  if (unique.length > 0) {
    return unique;
  }

  if (fallbackSummary) {
    return [truncateText(fallbackSummary, 180)];
  }

  return [];
}

function combineConfidence(typeConfidence, companyConfidence, hasSummary) {
  const summaryBoost = hasSummary ? 0.1 : 0;
  const value = typeConfidence * 0.55 + companyConfidence * 0.35 + summaryBoost;
  return Math.min(0.99, Math.max(0.2, Number(value.toFixed(2))));
}

function normalizeTitle(subject) {
  const title = normalizeWhitespace(subject);
  return title || "(No Subject)";
}

export function parseArchiveToReport(archiveRecord) {
  if (!archiveRecord || typeof archiveRecord !== "object") {
    throw new TypeError("archiveRecord must be an object");
  }

  const archiveId = String(archiveRecord.id ?? "");
  const userId = String(archiveRecord.userId ?? "");
  const broker = normalizeWhitespace(archiveRecord.broker) || "Unknown Broker";
  const title = normalizeTitle(archiveRecord.subject);
  const publishedAt = resolveArchivePublishedAt(archiveRecord);

  const reportTypeResult = classifyReportType(archiveRecord);
  const companyResult = extractCompanyFromArchive(archiveRecord);

  const summary = buildSummary(archiveRecord, reportTypeResult.reportType);
  const keyPoints = buildKeyPoints(archiveRecord, summary, reportTypeResult.reportType);
  const confidence = combineConfidence(reportTypeResult.confidence, companyResult.confidence, Boolean(summary));

  const extracted = {
    archiveId,
    userId,
    broker,
    companyCanonical: companyResult.companyCanonical || "Unknown Company",
    companyRaw: companyResult.companyRaw || "Unknown Company",
    reportType: reportTypeResult.reportType,
    title,
    summary,
    keyPoints,
    publishedAt,
    confidence,
    duplicateKey: ""
  };

  extracted.duplicateKey = createReportDuplicateKey(extracted);
  return extracted;
}

export function extractReportsFromArchives(archiveRecords) {
  if (!Array.isArray(archiveRecords)) {
    throw new TypeError("archiveRecords must be an array");
  }

  return archiveRecords.map((record) => parseArchiveToReport(record));
}

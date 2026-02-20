import { createReportDuplicateKey } from "../dedupe/report-dedupe-key.js";
import { extractCompanyFromArchive } from "../normalization/company.js";
import { resolveArchivePublishedAt } from "../normalization/date.js";
import { normalizeWhitespace, splitSentences, truncateText, uniqueStrings } from "../normalization/text.js";
import { classifyReportType } from "./report-type-classifier.js";

function buildSummary(record) {
  const source = normalizeWhitespace(record?.bodyPreview || record?.snippet || record?.subject || "");
  return truncateText(source, 380);
}

function buildKeyPoints(record, fallbackSummary, maxItems = 3) {
  const sourceText = normalizeWhitespace([record?.bodyPreview, record?.snippet].filter(Boolean).join(" "));
  const sentences = splitSentences(sourceText || fallbackSummary);
  const cleaned = sentences
    .map((entry) => truncateText(entry, 180))
    .filter((entry) => entry.length >= 12);

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

  const summary = buildSummary(archiveRecord);
  const keyPoints = buildKeyPoints(archiveRecord, summary);
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

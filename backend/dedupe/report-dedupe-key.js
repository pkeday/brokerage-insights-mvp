import { createHash } from "node:crypto";
import { toUtcDayBucket } from "../normalization/date.js";
import { normalizeForKey } from "../normalization/text.js";

function getSafePart(value, fallback) {
  const normalized = normalizeForKey(value);
  return normalized || fallback;
}

export function createReportDuplicateKey(input) {
  const dayBucket = toUtcDayBucket(input?.publishedAt);
  const rawKey = [
    getSafePart(input?.userId, "unknown-user"),
    getSafePart(input?.broker, "unknown-broker"),
    getSafePart(input?.companyCanonical, "unknown-company"),
    getSafePart(input?.reportType, "general_update"),
    getSafePart(input?.title, "untitled"),
    dayBucket
  ].join("|");

  const digest = createHash("sha256").update(rawKey).digest("hex").slice(0, 24);
  return `rpt_${dayBucket}_${digest}`;
}

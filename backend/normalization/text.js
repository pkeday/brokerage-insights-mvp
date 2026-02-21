const DEFAULT_TRUNCATE_SUFFIX = "...";

export function normalizeWhitespace(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

export function truncateText(value, maxLength = 320, suffix = DEFAULT_TRUNCATE_SUFFIX) {
  const text = normalizeWhitespace(value);
  if (!text || text.length <= maxLength) {
    return text;
  }

  const limit = Math.max(1, maxLength - suffix.length);
  const truncated = text.slice(0, limit);
  const lastSpace = truncated.lastIndexOf(" ");
  if (lastSpace > Math.floor(limit * 0.6)) {
    return `${truncated.slice(0, lastSpace)}${suffix}`;
  }
  return `${truncated}${suffix}`;
}

export function splitSentences(value) {
  const text = normalizeWhitespace(value);
  if (!text) {
    return [];
  }

  const chunks = text
    .split(/(?<=[.?!])\s+/)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);

  if (chunks.length > 0) {
    return chunks;
  }

  return text
    .split(/[;|]\s+/)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);
}

export function normalizeForKey(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\b(the|a|an|report|note|update)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function uniqueStrings(values, maxItems = 10) {
  const seen = new Set();
  const result = [];

  for (const item of values ?? []) {
    const normalized = normalizeWhitespace(item);
    if (!normalized) {
      continue;
    }
    const key = normalized.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(normalized);
    if (result.length >= maxItems) {
      break;
    }
  }

  return result;
}

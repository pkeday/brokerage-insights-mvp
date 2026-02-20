import { normalizeWhitespace } from "./text.js";

const LEGAL_SUFFIX_TOKENS = new Set([
  "co",
  "company",
  "corp",
  "corporation",
  "inc",
  "incorporated",
  "ltd",
  "limited",
  "llc",
  "llp",
  "plc",
  "pvt",
  "sa",
  "ag",
  "nv"
]);

const REPORT_NOISE_TOKENS = new Set([
  "general",
  "market",
  "morning",
  "evening",
  "weekly",
  "monthly",
  "daily",
  "domestic",
  "global",
  "macro",
  "strategy",
  "sector",
  "universe",
  "upgraded",
  "downgraded",
  "reiterated",
  "maintained",
  "initiation",
  "initiating",
  "coverage",
  "result",
  "results",
  "earnings",
  "update",
  "preview",
  "concall",
  "call",
  "target",
  "price",
  "rating",
  "buy",
  "sell",
  "hold",
  "note",
  "report"
]);

function cleanCandidate(value) {
  return normalizeWhitespace(value)
    .replace(/^[\[(\{"']+/, "")
    .replace(/[\])\}"']+$/, "")
    .replace(/\s*[-:|]\s*(?:initiation|results?|earnings?|update|coverage).*$/i, "")
    .replace(/\s*\b(?:initiation|results?|earnings?|update|coverage|target|rating)\b.*$/i, "")
    .replace(/\b(?:q[1-4](?:fy|cy)?\d{2,4}|fy\d{2,4}|cy\d{2,4})\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isPlausibleCompanyName(value) {
  if (!value) {
    return false;
  }
  if (value.length < 2 || value.length > 100) {
    return false;
  }

  const normalized = value.toLowerCase();
  if (
    /^(daily|morning|weekly|monthly|market|strategy|sector|note|report|general|update|macro)$/i.test(normalized)
  ) {
    return false;
  }

  const tokenCount = normalized.split(/\s+/).filter(Boolean).length;
  if (tokenCount > 8) {
    return false;
  }

  if (/\b(update|result|earnings|coverage|target|rating|market|strategy|sector)\b/i.test(normalized)) {
    return false;
  }

  return /[a-z]/i.test(value);
}

function extractFromSubject(subject) {
  if (!subject) {
    return null;
  }

  const subjectText = normalizeWhitespace(subject);
  const patterns = [
    /^([A-Za-z][A-Za-z0-9&.,'()\- ]{2,100})\s+(?:upgraded|downgraded|reiterated|maintained|initiated)\b/i,
    /^([^:|()-]{2,100})\s*[:|-]/,
    /\b(?:on|of|for)\s+([A-Za-z][A-Za-z0-9&.,'()\- ]{2,80}?)(?=\s+(?:with|at|in|after|before|as|for)\b|[,:;|()-]|$)/i,
    /\b([A-Za-z][A-Za-z0-9&.,'()\- ]{2,100})\s+(?:results?|earnings?|update|initiation|coverage|target|rating)\b/i
  ];

  for (const pattern of patterns) {
    const match = subjectText.match(pattern);
    const candidate = cleanCandidate(match?.[1] ?? "");
    if (isPlausibleCompanyName(candidate)) {
      return candidate;
    }
  }

  return null;
}

function extractFromBody(bodyPreview) {
  const text = normalizeWhitespace(bodyPreview);
  if (!text) {
    return null;
  }

  const match = text.match(
    /\b(?:for|on|of)\s+([A-Za-z][A-Za-z0-9&.,'()\- ]{2,80}?)(?=\s+(?:with|at|in|after|before|as|and|we|that)\b|[.,;]|$)/i
  );
  const candidate = cleanCandidate(match?.[1] ?? "");
  if (!isPlausibleCompanyName(candidate)) {
    return null;
  }

  return candidate;
}

export function canonicalizeCompanyName(rawValue) {
  const raw = normalizeWhitespace(rawValue);
  if (!raw) {
    return "";
  }

  const uppercaseHints = new Set(
    raw
      .split(/\s+/)
      .map((token) => token.replace(/[^A-Za-z0-9]/g, ""))
      .filter((token) => /^[A-Z0-9]{2,}$/.test(token))
      .map((token) => token.toLowerCase())
  );

  const tokens = raw
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !LEGAL_SUFFIX_TOKENS.has(token))
    .filter((token) => !REPORT_NOISE_TOKENS.has(token))
    .filter((token) => !/^q[1-4](?:fy|cy)?\d{2,4}$/.test(token))
    .filter((token) => !/^(?:fy|cy)\d{2,4}$/.test(token));

  if (tokens.length === 0) {
    return "";
  }

  return tokens
    .map((token) => {
      if (uppercaseHints.has(token)) {
        return token.toUpperCase();
      }
      if (/^\d+$/.test(token)) {
        return token;
      }
      return `${token[0].toUpperCase()}${token.slice(1)}`;
    })
    .join(" ");
}

export function extractCompanyFromArchive(archiveRecord) {
  const subjectCandidate = extractFromSubject(archiveRecord?.subject);
  if (subjectCandidate) {
    return {
      companyRaw: subjectCandidate,
      companyCanonical: canonicalizeCompanyName(subjectCandidate),
      confidence: 0.9,
      source: "subject"
    };
  }

  const bodyCandidate = extractFromBody(archiveRecord?.bodyPreview || archiveRecord?.snippet);
  if (bodyCandidate) {
    return {
      companyRaw: bodyCandidate,
      companyCanonical: canonicalizeCompanyName(bodyCandidate),
      confidence: 0.65,
      source: "body"
    };
  }

  return {
    companyRaw: "Unknown Company",
    companyCanonical: "Unknown Company",
    confidence: 0.25,
    source: "fallback"
  };
}

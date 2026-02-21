import { normalizeWhitespace, truncateText } from "../normalization/text.js";

const EMAIL_PATTERN = /\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b/gi;
const PHONE_PATTERN = /(?<!\w)(?:\+?\d[\d()\s.-]{7,}\d)(?!\w)/g;
const HEADER_LINE_PATTERN = /(?:^|\n)\s*(?:from|to|cc|bcc|sent|subject)\s*:[^\n]*/gi;
const FORWARDED_BLOCK_PATTERN = /(?:^|\n)\s*-{2,}\s*(?:original|forwarded)\s+message\s*-{2,}/gi;
const SIGN_OFF_PATTERN =
  /\b(?:best regards|warm regards|kind regards|regards|thanks(?: and regards)?|sincerely)\b[\s\S]{0,180}$/i;
const ORG_KEYWORDS = [
  "capital",
  "equities",
  "research",
  "desk",
  "team",
  "securities",
  "invest",
  "institutional",
  "bank",
  "finance",
  "global",
  "limited",
  "ltd"
];

function escapeRegExp(value) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseFromHeader(rawFrom) {
  const fromText = String(rawFrom ?? "");
  const emailMatch = fromText.match(/<([^>]+)>/);
  const email = normalizeWhitespace((emailMatch?.[1] ?? "").toLowerCase());
  const cleanedName = normalizeWhitespace(fromText.replace(/<[^>]+>/g, "").replace(/"/g, ""));

  return {
    email,
    name: cleanedName
  };
}

function isLikelyPersonName(value) {
  const candidate = normalizeWhitespace(value);
  if (!candidate) {
    return false;
  }

  if (candidate.length < 5 || candidate.length > 60) {
    return false;
  }

  const lower = candidate.toLowerCase();
  if (ORG_KEYWORDS.some((keyword) => lower.includes(keyword))) {
    return false;
  }

  if (!/^[a-z .'-]+$/i.test(candidate)) {
    return false;
  }

  const parts = candidate.split(" ").filter(Boolean);
  if (parts.length < 2 || parts.length > 4) {
    return false;
  }

  return parts.every((part) => /^[A-Z][a-z'-]{1,}$/.test(part));
}

function replaceToken(text, token, replacement) {
  const cleanToken = normalizeWhitespace(token);
  if (!cleanToken || cleanToken.length < 3) {
    return text;
  }
  const pattern = new RegExp(`\\b${escapeRegExp(cleanToken)}\\b`, "gi");
  return text.replace(pattern, replacement);
}

function maybeStripTailSignature(text) {
  const match = text.match(SIGN_OFF_PATTERN);
  if (!match || !match.index) {
    return text;
  }

  const prefix = text.slice(0, match.index);
  return normalizeWhitespace(prefix) || text;
}

export function redactPiiText(value, options = {}) {
  const senderName = normalizeWhitespace(options.senderName);
  const senderEmail = normalizeWhitespace(options.senderEmail).toLowerCase();
  const recipientNames = Array.isArray(options.recipientNames)
    ? options.recipientNames.map((entry) => normalizeWhitespace(entry)).filter(Boolean)
    : [];
  const recipientEmails = Array.isArray(options.recipientEmails)
    ? options.recipientEmails.map((entry) => normalizeWhitespace(entry).toLowerCase()).filter(Boolean)
    : [];

  let text = String(value ?? "");
  if (!text) {
    return "";
  }

  text = text
    .replace(FORWARDED_BLOCK_PATTERN, " ")
    .replace(HEADER_LINE_PATTERN, " ")
    .replace(EMAIL_PATTERN, "[redacted-email]")
    .replace(PHONE_PATTERN, "[redacted-phone]");

  if (senderEmail) {
    text = replaceToken(text, senderEmail, "[redacted-sender]");
  }
  if (senderName && isLikelyPersonName(senderName)) {
    text = replaceToken(text, senderName, "[redacted-sender]");
  }

  for (const email of recipientEmails) {
    text = replaceToken(text, email, "[redacted-recipient]");
  }
  for (const name of recipientNames) {
    if (isLikelyPersonName(name)) {
      text = replaceToken(text, name, "[redacted-recipient]");
    }
  }

  text = normalizeWhitespace(text);
  text = maybeStripTailSignature(text);

  return normalizeWhitespace(text);
}

export function redactArchiveForExtraction(archiveRecord) {
  const archive = archiveRecord && typeof archiveRecord === "object" ? archiveRecord : {};
  const fromHeader = parseFromHeader(archive.from);

  const redactionOptions = {
    senderName: fromHeader.name,
    senderEmail: fromHeader.email
  };

  return {
    ...archive,
    from: redactPiiText(archive.from, redactionOptions),
    subject: truncateText(redactPiiText(archive.subject, redactionOptions), 320),
    snippet: truncateText(redactPiiText(archive.snippet, redactionOptions), 1200),
    bodyPreview: truncateText(redactPiiText(archive.bodyPreview, redactionOptions), 5000)
  };
}

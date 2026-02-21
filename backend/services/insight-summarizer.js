import { normalizeWhitespace, splitSentences, truncateText, uniqueStrings } from "../normalization/text.js";
import { redactPiiText } from "../extraction/pii-redaction.js";

const NUMERIC_SIGNAL_PATTERN = /\b(?:\d+(?:\.\d+)?%|(?:rs\.?|inr)\s?\d[\d,.]*|usd\s?\d[\d,.]*|\d[\d,.]*\s?(?:bps|bp|mn|bn|cr|crore|lakh|x))\b/i;
const NEGATIVE_BOILERPLATE_PATTERN =
  /\b(?:unsubscribe|confidential|disclaimer|intended recipient|do not reply|forwarded message|view in browser|privacy policy)\b/i;
const INLINE_HEADER_SEGMENT_PATTERN =
  /\b(?:from|to|cc|bcc|sent|subject|reply-to)\s*:[\s\S]{0,180}?(?=(?:\b(?:from|to|cc|bcc|sent|subject|reply-to)\s*:)|$)/gi;
const URL_PATTERN = /\b(?:https?:\/\/|www\.)\S+\b/gi;
const LONG_MACHINE_TOKEN_PATTERN = /\b[a-z0-9_-]{24,}\b/gi;
const QUOTED_PRINTABLE_PATTERN = /=[0-9A-F]{2}/gi;
const MAX_SOURCE_TEXT = 2200;

const REPORT_TYPE_KEYWORDS = {
  initiation: ["initiation", "coverage", "thesis", "valuation", "target", "rating"],
  results_update: ["result", "earnings", "margin", "ebitda", "guidance", "beat", "miss"],
  target_change: ["target", "valuation", "tp", "pt", "upgrade", "downgrade", "rerating"],
  rating_change: ["upgrade", "downgrade", "rating", "buy", "sell", "hold"],
  general_update: ["update", "industry", "outlook", "demand", "cost", "capacity"],
  broker_note: ["update", "note", "outlook", "view"]
};

function hasNumericSignal(sentence) {
  return NUMERIC_SIGNAL_PATTERN.test(sentence);
}

function sanitizeSourceText(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return "";
  }

  return normalizeWhitespace(
    normalized
      .replace(INLINE_HEADER_SEGMENT_PATTERN, " ")
      .replace(URL_PATTERN, " ")
      .replace(QUOTED_PRINTABLE_PATTERN, " ")
      .replace(LONG_MACHINE_TOKEN_PATTERN, " ")
      .replace(/\s*[|•]\s*/g, ". ")
  );
}

function splitCandidateSentences(value) {
  const primary = splitSentences(value).map((entry) => normalizeWhitespace(entry)).filter(Boolean);
  if (primary.length > 1) {
    return primary;
  }

  const fallback = normalizeWhitespace(value)
    .split(/\s*[;|•]\s*|\s*,\s+/)
    .map((entry) => normalizeWhitespace(entry))
    .filter((entry) => entry.length >= 20);

  if (fallback.length > 0) {
    return fallback;
  }

  return primary;
}

function scoreSentence(sentence, reportType, title) {
  if (!sentence) {
    return -10;
  }

  if (NEGATIVE_BOILERPLATE_PATTERN.test(sentence)) {
    return -6;
  }

  const normalizedSentence = sentence.toLowerCase();
  const normalizedTitle = normalizeWhitespace(title).toLowerCase();
  const keywordList = REPORT_TYPE_KEYWORDS[reportType] ?? REPORT_TYPE_KEYWORDS.broker_note;

  let score = 0;
  if (hasNumericSignal(sentence)) {
    score += 3;
  }

  if (keywordList.some((keyword) => normalizedSentence.includes(keyword))) {
    score += 2;
  }

  if (normalizedTitle && normalizedSentence.includes(normalizedTitle.slice(0, 20))) {
    score += 1;
  }

  if (/(?:id=|href=|src=|http|www\.|[a-z0-9_-]{20,})/i.test(sentence)) {
    score -= 3;
  }

  if (sentence.length >= 45 && sentence.length <= 220) {
    score += 1;
  } else if (sentence.length < 20 || sentence.length > 280) {
    score -= 1;
  }

  return score;
}

function extractResponseText(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }

  if (typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  if (Array.isArray(payload.output)) {
    const chunks = [];
    for (const item of payload.output) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const content = Array.isArray(item.content) ? item.content : [];
      for (const part of content) {
        if (part?.type === "output_text" && typeof part.text === "string") {
          chunks.push(part.text);
        }
      }
    }
    if (chunks.length > 0) {
      return chunks.join("\n").trim();
    }
  }

  return "";
}

function parseSummaryJson(rawText) {
  const text = normalizeWhitespace(rawText);
  if (!text) {
    return null;
  }

  const jsonMatch = text.match(/\{[\s\S]*\}/);
  const jsonText = jsonMatch ? jsonMatch[0] : text;
  try {
    const parsed = JSON.parse(jsonText);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    return {
      summary: normalizeWhitespace(parsed.summary),
      keyPoints: Array.isArray(parsed.keyPoints)
        ? parsed.keyPoints.map((entry) => normalizeWhitespace(entry)).filter(Boolean).slice(0, 4)
        : []
    };
  } catch {
    return null;
  }
}

function buildHeuristicSummary(report, archive) {
  const title = normalizeWhitespace(report.title);
  const reportType = normalizeWhitespace(report.reportType).toLowerCase() || "general_update";
  const sourceText = sanitizeSourceText(
    [archive?.bodyPreview, archive?.snippet, report?.summary, title].filter(Boolean).join(" ")
  ).slice(0, MAX_SOURCE_TEXT);

  const candidates = splitCandidateSentences(sourceText)
    .filter(Boolean)
    .map((sentence) => ({
      sentence,
      score: scoreSentence(sentence, reportType, title)
    }))
    .sort((left, right) => right.score - left.score);

  const rankedFallback = candidates.filter((entry) => entry.score > -4).map((entry) => entry.sentence);
  const topSentences = uniqueStrings(
    (candidates.filter((entry) => entry.score >= 1).map((entry) => entry.sentence) || []).concat(rankedFallback),
    3
  );

  const fallbackSummary = normalizeWhitespace(report.summary) || normalizeWhitespace(report.title) || "No summary available.";
  const summary = truncateText(
    topSentences.slice(0, 2).join(" ") || fallbackSummary,
    420
  );

  const keyPoints = uniqueStrings(
    (topSentences.length > 0 ? topSentences : splitSentences(summary)).map((entry) => truncateText(entry, 180)),
    3
  );

  return {
    summary,
    keyPoints
  };
}

async function generateOpenAiSummary(report, archive, config) {
  if (!config.apiKey || config.enabled !== true) {
    return null;
  }

  const sourceText = sanitizeSourceText(
    [archive?.subject, archive?.bodyPreview, archive?.snippet, report?.summary].filter(Boolean).join(" ")
  ).slice(0, MAX_SOURCE_TEXT);
  if (!sourceText) {
    return null;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs);

  try {
    const response = await fetch(`${config.baseUrl}/responses`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: config.model,
        temperature: 0.15,
        max_output_tokens: 220,
        input: [
          {
            role: "system",
            content:
              "You are an equity research distillation assistant. Produce concise, non-generic summaries for expert users. Keep factual precision and avoid boilerplate. Return strict JSON only."
          },
          {
            role: "user",
            content: `Return JSON with keys {"summary": string, "keyPoints": string[]}.
Rules:
- summary max 380 chars, nuanced and specific.
- keyPoints max 3 bullets, each max 160 chars.
- Redact sender/receiver PII and all email addresses.
- Keep report author references if present in research context.
Context:
Broker: ${normalizeWhitespace(report.broker)}
Company: ${normalizeWhitespace(report.companyCanonical)}
Report Type: ${normalizeWhitespace(report.reportType)}
Title: ${normalizeWhitespace(report.title)}
Source: ${sourceText}`
          }
        ]
      }),
      signal: controller.signal
    });

    if (!response.ok) {
      return null;
    }

    const payload = await response.json();
    const text = extractResponseText(payload);
    const parsed = parseSummaryJson(text);
    if (!parsed || !parsed.summary) {
      return null;
    }

    return parsed;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

export function createInsightSummarizer(options = {}) {
  const apiKey = normalizeWhitespace(options.apiKey || process.env.OPENAI_API_KEY);
  const enabled = options.enabled !== false && Boolean(apiKey);
  const model = normalizeWhitespace(options.model || "gpt-5-nano");
  const baseUrl = normalizeWhitespace(options.baseUrl || "https://api.openai.com/v1");
  const timeoutMs = Number.parseInt(String(options.timeoutMs || "9000"), 10);

  const config = {
    apiKey,
    enabled,
    model,
    baseUrl,
    timeoutMs: Number.isFinite(timeoutMs) ? Math.max(2000, Math.min(timeoutMs, 30_000)) : 9000
  };

  async function summarize(payload) {
    const report = payload?.report ?? {};
    const archive = payload?.archive ?? {};
    const heuristic = buildHeuristicSummary(report, archive);
    const ai = await generateOpenAiSummary(report, archive, config);
    const selected = ai ?? heuristic;

    const summary = truncateText(redactPiiText(selected.summary), 420);
    const keyPoints = uniqueStrings(
      (Array.isArray(selected.keyPoints) ? selected.keyPoints : [])
        .map((entry) => truncateText(redactPiiText(entry), 180))
        .filter(Boolean),
      3
    );

    const fallbackKeyPoints = uniqueStrings(
      heuristic.keyPoints.map((entry) => truncateText(redactPiiText(entry), 180)).filter(Boolean),
      3
    );

    return {
      summary: summary || truncateText(redactPiiText(heuristic.summary), 420),
      keyPoints: keyPoints.length > 0 ? keyPoints : fallbackKeyPoints
    };
  }

  return {
    source: config.enabled && config.apiKey ? `openai:${config.model}` : "heuristic",
    summarize
  };
}

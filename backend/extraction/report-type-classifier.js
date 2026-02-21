import { normalizeWhitespace } from "../normalization/text.js";

const TYPE_RULES = [
  {
    reportType: "initiation",
    confidence: 0.92,
    patterns: [
      /\binitiating coverage\b/i,
      /\binitiation(?: of coverage)?\b/i,
      /\bcoverage initiated\b/i,
      /\bstart(?:ing)? coverage\b/i
    ]
  },
  {
    reportType: "results_update",
    confidence: 0.88,
    patterns: [
      /\bq[1-4]\s*(?:fy|cy)?\s*\d{2,4}\b/i,
      /\bresults?\s+update\b/i,
      /\bearnings?\b/i,
      /\bpost[- ]?result\b/i
    ]
  },
  {
    reportType: "target_change",
    confidence: 0.82,
    patterns: [/\btarget price\b/i, /\bprice target\b/i, /\braise[sd]?\s+target\b/i, /\bcut[s]?\s+target\b/i]
  },
  {
    reportType: "rating_change",
    confidence: 0.8,
    patterns: [/\bupgrade\b/i, /\bdowngrade\b/i, /\breiterate\b/i, /\bmaintain(?:ed)?\s+(?:buy|sell|hold)\b/i]
  },
  {
    reportType: "general_update",
    confidence: 0.62,
    patterns: [/\bgeneral update\b/i, /\bcompany update\b/i, /\bupdate\b/i]
  }
];

export function classifyReportType(record) {
  const subject = normalizeWhitespace(record?.subject);
  const snippet = normalizeWhitespace(record?.snippet);
  const bodyPreview = normalizeWhitespace(record?.bodyPreview);

  const combinedText = normalizeWhitespace([subject, snippet, bodyPreview].filter(Boolean).join(" "));

  for (const rule of TYPE_RULES) {
    for (const pattern of rule.patterns) {
      if (pattern.test(subject)) {
        return {
          reportType: rule.reportType,
          confidence: Math.min(1, rule.confidence + 0.05),
          matchedOn: "subject",
          matchedPattern: pattern.source
        };
      }

      if (pattern.test(combinedText)) {
        return {
          reportType: rule.reportType,
          confidence: rule.confidence,
          matchedOn: "body",
          matchedPattern: pattern.source
        };
      }
    }
  }

  return {
    reportType: "general_update",
    confidence: 0.45,
    matchedOn: "fallback",
    matchedPattern: "none"
  };
}

export function getReportTypeRules() {
  return TYPE_RULES.map((rule) => ({
    reportType: rule.reportType,
    confidence: rule.confidence,
    patterns: rule.patterns.map((pattern) => pattern.source)
  }));
}

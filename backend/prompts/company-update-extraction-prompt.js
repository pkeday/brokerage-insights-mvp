import { normalizeWhitespace } from "../normalization/text.js";

export const COMPANY_UPDATE_ALLOWED_REPORT_TYPES = [
  "initiating_coverage",
  "results_update",
  "general_update",
  "sector_update",
  "target_price_change",
  "rating_change",
  "management_commentary",
  "corporate_action",
  "other"
];

export const COMPANY_UPDATE_ALLOWED_DECISIONS = ["BUY", "SELL", "HOLD", "ADD", "REDUCE", "UNKNOWN"];

export const COMPANY_UPDATE_SYSTEM_PROMPT =
  "You extract brokerage research insights for expert users. Keep broker perspective distinct. Return strict JSON only.";

export const COMPANY_UPDATE_FIELD_PROMPT = {
  companyRaw:
    "Company name exactly as inferred from this email. Keep original broker wording where useful. If sector-only note, use sector label as companyRaw.",
  companyCanonical:
    "Standardized company label. Use canonical company from dictionary when confidently matched. For sector-only notes use 'SECTOR:<sectorName>'.",
  sectorName: "Only for sector coverage. Empty string for single-company updates.",
  reportType: `One of: ${COMPANY_UPDATE_ALLOWED_REPORT_TYPES.join(", ")}.`,
  title:
    "Short report title tied to this specific update (not mailing list header). Include key context like Results Update, Initiating Coverage, TP change, etc.",
  summary:
    "Analytical, non-generic broker perspective in 90-220 characters. Mention the key thesis/change only. Do not copy long text verbatim.",
  keyInsights:
    "Array of max 3 concise bullets with concrete findings (earnings, margins, valuation, drivers, guidance, risks).",
  decision: `One of: ${COMPANY_UPDATE_ALLOWED_DECISIONS.join(", ")}.`,
  confidence: "Number from 0 to 1 for extraction confidence.",
  publishedAt: "ISO datetime when available, else infer from email context."
};

export function renderCompanyUpdateExtractionPrompt(params = {}) {
  const broker = normalizeWhitespace(params.broker);
  const archiveId = normalizeWhitespace(params.archiveId);
  const dictionaryContext = normalizeWhitespace(params.dictionaryContext) || "[]";
  const sourceText = normalizeWhitespace(params.sourceText);
  const repairMode = params.repairMode === true;
  const repairInstruction = repairMode
    ? "\n- Your previous output was invalid or empty. Re-read and return corrected JSON only."
    : "";

  return `Extract all company-wise updates from this ONE brokerage email.
Return strict JSON with shape:
{"reports":[{"companyRaw":string,"companyCanonical":string,"sectorName":string,"reportType":string,"title":string,"summary":string,"keyInsights":string[],"decision":string,"confidence":number,"publishedAt":string}]}
Field rules:
- companyRaw: ${COMPANY_UPDATE_FIELD_PROMPT.companyRaw}
- companyCanonical: ${COMPANY_UPDATE_FIELD_PROMPT.companyCanonical}
- sectorName: ${COMPANY_UPDATE_FIELD_PROMPT.sectorName}
- reportType: ${COMPANY_UPDATE_FIELD_PROMPT.reportType}
- title: ${COMPANY_UPDATE_FIELD_PROMPT.title}
- summary: ${COMPANY_UPDATE_FIELD_PROMPT.summary}
- keyInsights: ${COMPANY_UPDATE_FIELD_PROMPT.keyInsights}
- decision: ${COMPANY_UPDATE_FIELD_PROMPT.decision}
- confidence: ${COMPANY_UPDATE_FIELD_PROMPT.confidence}
- publishedAt: ${COMPANY_UPDATE_FIELD_PROMPT.publishedAt}
Global rules:
- One email may include multiple companies. Return one consolidated object per company (or sector).
- Ignore recap blocks ("latest releases", "other reports"), legal disclaimers, signatures, unsubscribe, and distribution metadata.
- Focus on the primary update content and what changed in this email.
- Strip sender/receiver identities and email addresses.
- If no extractable research content exists, return {"reports":[]}.${repairInstruction}
Context:
Broker: ${broker}
ArchiveId: ${archiveId}
Canonical Company Dictionary: ${dictionaryContext}
Email Content:
${sourceText}`;
}

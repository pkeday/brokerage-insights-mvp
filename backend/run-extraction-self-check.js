import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createReportDuplicateKey } from "./dedupe/index.js";
import { extractReportsFromArchives } from "./extraction/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const fixturePath = path.join(__dirname, "extraction", "fixtures", "archive-records.fixture.json");

async function loadFixtureArchives() {
  const raw = await readFile(fixturePath, "utf8");
  const parsed = JSON.parse(raw);
  assert.ok(Array.isArray(parsed), "fixture should be an array");
  return parsed;
}

function assertReportShape(report) {
  assert.ok(typeof report.archiveId === "string" && report.archiveId, "archiveId must be non-empty string");
  assert.ok(typeof report.userId === "string" && report.userId, "userId must be non-empty string");
  assert.ok(typeof report.broker === "string" && report.broker, "broker must be non-empty string");
  assert.ok(typeof report.companyCanonical === "string" && report.companyCanonical, "companyCanonical required");
  assert.ok(typeof report.companyRaw === "string" && report.companyRaw, "companyRaw required");
  assert.ok(typeof report.reportType === "string" && report.reportType, "reportType required");
  assert.ok(typeof report.title === "string" && report.title, "title required");
  assert.ok(typeof report.summary === "string", "summary required");
  assert.ok(Array.isArray(report.keyPoints), "keyPoints must be array");
  assert.ok(typeof report.publishedAt === "string" && report.publishedAt, "publishedAt required");
  assert.ok(typeof report.confidence === "number", "confidence must be a number");
  assert.ok(typeof report.duplicateKey === "string" && report.duplicateKey, "duplicateKey required");
}

function getByArchiveId(reports, archiveId) {
  const report = reports.find((item) => item.archiveId === archiveId);
  assert.ok(report, `missing report ${archiveId}`);
  return report;
}

async function main() {
  const archives = await loadFixtureArchives();
  const reports = extractReportsFromArchives(archives);

  assert.equal(reports.length, archives.length, "report count should equal archive count");
  for (const report of reports) {
    assertReportShape(report);
    assert.equal(report.duplicateKey, createReportDuplicateKey(report), "duplicateKey must be deterministic");
  }

  const report001 = getByArchiveId(reports, "arc_001");
  const report002 = getByArchiveId(reports, "arc_002");
  const report003 = getByArchiveId(reports, "arc_003");
  const report004 = getByArchiveId(reports, "arc_004");
  const report005 = getByArchiveId(reports, "arc_005");
  const report006 = getByArchiveId(reports, "arc_006");

  assert.equal(report001.reportType, "initiation", "arc_001 should classify as initiation");
  assert.equal(report002.reportType, "results_update", "arc_002 should classify as results_update");
  assert.equal(report005.reportType, "general_update", "arc_005 should classify as general_update");
  assert.equal(report006.reportType, "target_change", "arc_006 should classify as target_change");

  assert.equal(
    report001.duplicateKey,
    report003.duplicateKey,
    "same user/broker/company/title/day should yield same dedupe key"
  );
  assert.notEqual(
    report001.duplicateKey,
    report004.duplicateKey,
    "broker changes must produce different dedupe keys"
  );

  console.log("[self-check] extraction pipeline checks passed");
  console.table(
    reports.map((item) => ({
      archiveId: item.archiveId,
      reportType: item.reportType,
      companyCanonical: item.companyCanonical,
      duplicateKey: item.duplicateKey
    }))
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  console.error(`[self-check] failed: ${message}`);
  process.exitCode = 1;
});

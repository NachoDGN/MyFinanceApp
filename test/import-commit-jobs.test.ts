import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const legacyWorkbookFixturePath = join(
  process.cwd(),
  "test/fixtures/legacy-import.xls",
);

function buildXlsTemplate(sheetName: string) {
  return {
    file_kind: "xls",
    sheet_name: sheetName,
    header_row_index: 1,
    rows_to_skip_before_header: 0,
    rows_to_skip_after_header: 0,
    default_currency: "EUR",
    column_map_json: {
      transaction_date: "Date",
      description_raw: "Description",
      amount_original_signed: "Amount",
    },
    sign_logic_json: {
      mode: "signed_amount",
      invert_sign: false,
    },
    normalization_rules_json: {
      date_day_first: true,
      start_column_index: 0,
      start_column_letter: "A",
    },
  };
}

test("deterministic import commit does not queue a standalone search reindex job", async () => {
  const { stdout } = await execFileAsync(
    "./.venv/bin/python",
    [
      "python/ingest/runner.py",
      "commit",
      "--file-path",
      legacyWorkbookFixturePath,
      "--account-id",
      "account-1",
      "--template-id",
      "template-1",
      "--reference-date",
      "2026-04-10",
      "--template-json",
      JSON.stringify(buildXlsTemplate("Movimientos")),
    ],
    {
      cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
    },
  );

  const result = JSON.parse(stdout) as {
    jobsQueued: string[];
  };

  assert.deepEqual(result.jobsQueued, [
    "classification",
    "transfer_rematch",
    "position_rebuild",
    "metric_refresh",
  ]);
});

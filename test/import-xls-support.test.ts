import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  inspectSpreadsheetWorkbook,
  validateSpreadsheetFile,
} from "../packages/ingestion/src/index.ts";

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

test("inspectSpreadsheetWorkbook recognizes legacy xls uploads", async () => {
  const preview = await inspectSpreadsheetWorkbook(legacyWorkbookFixturePath);

  assert.equal(preview.fileKind, "xls");
  assert.equal(preview.sheetPreviews[0]?.sheetName, "Movimientos");
  assert.match(
    preview.sheetPreviews[0]?.previewCsv ?? "",
    /Date,Description,Amount/,
  );
});

test("validateSpreadsheetFile keeps xls workbooks on the spreadsheet path", async () => {
  const validation = await validateSpreadsheetFile(legacyWorkbookFixturePath);

  assert.equal(validation.fileKind, "xls");
  assert.deepEqual(validation.issues.map((issue) => issue.code).sort(), [
    "mixed_date_representations",
  ]);
});

test("deterministic import previews canonical rows from xls workbooks", async () => {
  const { stdout } = await execFileAsync(
    "./.venv/bin/python",
    [
      "python/ingest/runner.py",
      "preview",
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

  const preview = JSON.parse(stdout) as {
    normalizedRows: Array<{
      transaction_date: string;
      description_raw: string;
      amount_original_signed: string;
    }>;
  };

  assert.deepEqual(
    preview.normalizedRows.map((row) => ({
      transaction_date: row.transaction_date,
      description_raw: row.description_raw,
      amount_original_signed: row.amount_original_signed,
    })),
    [
      {
        transaction_date: "2026-04-03",
        description_raw: "Coffee",
        amount_original_signed: "-3.5",
      },
      {
        transaction_date: "2026-04-04",
        description_raw: "Salary",
        amount_original_signed: "1200",
      },
    ],
  );
});

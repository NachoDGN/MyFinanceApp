import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

function buildCsvTemplate() {
  return {
    file_kind: "csv",
    header_row_index: 1,
    rows_to_skip_before_header: 0,
    rows_to_skip_after_header: 0,
    delimiter: ",",
    encoding: "utf-8",
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

function buildXlsxTemplate(sheetName: string) {
  return {
    file_kind: "xlsx",
    sheet_name: sheetName,
    header_row_index: 1,
    rows_to_skip_before_header: 0,
    rows_to_skip_after_header: 0,
    default_currency: "EUR",
    column_map_json: {
      transaction_date: "Date",
      posted_date: "Posted",
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

function buildInvestmentCsvTemplate() {
  return {
    file_kind: "csv",
    header_row_index: 1,
    rows_to_skip_before_header: 0,
    rows_to_skip_after_header: 0,
    delimiter: ";",
    encoding: "utf-8",
    default_currency: "EUR",
    column_map_json: {
      transaction_date: "Date",
      amount_original_signed: "Amount",
      transaction_type_raw: "Status",
      security_isin: "ISIN",
      quantity: "Quantity",
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

async function runPreview(filePath: string, template = buildCsvTemplate()) {
  const { stdout } = await execFileAsync(
    "./.venv/bin/python",
    [
      "python/ingest/runner.py",
      "preview",
      "--file-path",
      filePath,
      "--account-id",
      "account-1",
      "--template-id",
      "template-1",
      "--reference-date",
      "2026-04-04",
      "--template-json",
      JSON.stringify(template),
    ],
    {
      cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
    },
  );

  return JSON.parse(stdout) as {
    normalizedRows: Array<{ transaction_date: string; description_raw: string }>;
  };
}

test("deterministic import falls back to the non-future interpretation for ambiguous dates", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-import-dates-"));
  const csvPath = join(directory, "ambiguous.csv");

  try {
    await writeFile(
      csvPath,
      ["Date,Description,Amount", "03/12/2026,Compra ETF,-100.00"].join("\n"),
      "utf8",
    );

    const preview = await runPreview(csvPath);

    assert.equal(preview.normalizedRows[0]?.transaction_date, "2026-03-12");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("deterministic import uses sheet context to resolve ambiguous month-day order", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-import-dates-"));
  const csvPath = join(directory, "context.csv");

  try {
    await writeFile(
      csvPath,
      [
        "Date,Description,Amount",
        "03/13/2026,Compra ETF,-100.00",
        "03/04/2026,Compra ETF,-200.00",
      ].join("\n"),
      "utf8",
    );

    const preview = await runPreview(csvPath);

    assert.deepEqual(
      preview.normalizedRows.map((row) => row.transaction_date),
      ["2026-03-13", "2026-03-04"],
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("deterministic import reinterprets typed Excel dates through their displayed format", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-import-dates-"));
  const workbookPath = join(directory, "typed-dates.xlsx");

  try {
    await execFileAsync(
      "./.venv/bin/python",
      [
        "-c",
        [
          "from datetime import date",
          "from openpyxl import Workbook",
          `path = r'''${workbookPath}'''`,
          "wb = Workbook()",
          "ws = wb.active",
          "ws.title = 'Movimientos'",
          "ws.append(['Date', 'Posted', 'Description', 'Amount'])",
          "ws.append(['24/03/2026', '25/03/2026', 'AMD', -353])",
          "ws.append([date(2026, 12, 3), '13/03/2026', 'INTEL', -42])",
          "ws.append([date(2026, 6, 3), date(2026, 9, 3), 'LITE', -41])",
          "ws['A3'].number_format = 'mm-dd-yy'",
          "ws['A4'].number_format = 'mm-dd-yy'",
          "ws['B4'].number_format = 'mm-dd-yy'",
          "wb.save(path)",
        ].join("; "),
      ],
      {
        cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
      },
    );

    const preview = await runPreview(
      workbookPath,
      buildXlsxTemplate("Movimientos"),
    );

    assert.deepEqual(
      preview.normalizedRows.map((row) => row.transaction_date),
      ["2026-03-24", "2026-03-12", "2026-03-06"],
    );
    assert.deepEqual(
      preview.normalizedRows.map((row) => row.description_raw),
      ["AMD", "INTEL", "LITE"],
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("deterministic import preserves security ISIN fields and comma-decimal quantities", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-import-dates-"));
  const csvPath = join(directory, "fund-orders.csv");

  try {
    await writeFile(
      csvPath,
      [
        "Date;ISIN;Quantity;Amount;Status",
        "04/04/2026;IE0032126645;0,558;-38,75;Finalizada",
      ].join("\n"),
      "utf8",
    );

    const preview = await runPreview(csvPath, buildInvestmentCsvTemplate());
    const row = preview.normalizedRows[0] as
      | {
          transaction_date: string;
          description_raw: string;
          quantity?: string;
          security_isin?: string;
        }
      | undefined;

    assert.equal(row?.transaction_date, "2026-04-04");
    assert.equal(row?.description_raw, "Finalizada IE0032126645");
    assert.equal(row?.security_isin, "IE0032126645");
    assert.equal(row?.quantity, "0.558");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

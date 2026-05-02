import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

test("validate-workbook recognizes pdf statements and requires extractor credentials", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-pdf-validator-"));
  const pdfPath = join(directory, "statement.pdf");

  try {
    await writeFile(
      pdfPath,
      Buffer.from("%PDF-1.4\n%minimal placeholder for validation\n", "utf-8"),
    );

    const { stdout } = await execFileAsync(
      "./.venv/bin/python",
      ["python/ingest/runner.py", "validate-workbook", "--file-path", pdfPath],
      {
        cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
        env: {
          ...process.env,
          ADE_API_KEY: "",
          GEMINI_API_KEY: "",
          api_key: "",
        },
      },
    );

    const validation = JSON.parse(stdout) as {
      fileKind: string;
      issues: Array<{ code: string; severity: string }>;
    };

    assert.equal(validation.fileKind, "pdf");
    assert.deepEqual(
      validation.issues.map((issue) => ({
        code: issue.code,
        severity: issue.severity,
      })),
      [
        {
          code: "missing_pdf_extractor_credentials",
          severity: "error",
        },
      ],
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("IBKR pdf parser kind allows empty statement periods without fabricating rows", async () => {
  const script = String.raw`
import sys
from pathlib import Path

sys.path.insert(0, "python/ingest")

import runner
from pdf_statement import PdfStatementParseResult

runner.extract_interactive_brokers_statement_rows = lambda *args, **kwargs: PdfStatementParseResult(
    rows=[],
    markdown="Activity Statement with no ledger rows",
    extraction_method="test",
    parser_model="test",
    statement_net_total=None,
    portfolio_statement_snapshot={
        "account_number": "U22902297",
        "statement_date": "2026-05-01",
        "base_currency": "EUR",
        "cash_balance": "95.626054801",
        "dividend_accruals": "6.5865496",
        "cash_balance_including_accruals": "102.212604401",
        "open_positions": [
            {
                "symbol": "HY9H",
                "security_name": "SK HYNIX INC-GDS",
                "isin": "US78392B1070",
                "quantity": "9",
                "cost_basis": "3425",
                "close_price": "760",
            },
        ],
    },
)

frame, canonical = runner.canonicalize_pdf_statement(
    Path("statement.pdf"),
    {
        "default_currency": "EUR",
        "normalization_rules_json": {
            "parser_kind": "ibkr_activity_statement_pdf",
        },
    },
    reference_date="2026-05-01",
)

assert canonical.row_count_detected == 0
assert len(canonical.normalized.to_dict(orient="records")) == 0
assert len(frame.to_dict(orient="records")) == 0

result = runner.build_result(
    "preview",
    "account-id",
    "template-id",
    "statement.pdf",
    frame,
    canonical,
)
snapshot = result["portfolioStatementSnapshot"]
assert snapshot["account_number"] == "U22902297"
assert snapshot["cash_balance_including_accruals"] == "102.212604401"
assert snapshot["open_positions"][0]["symbol"] == "HY9H"
`;

  await execFileAsync("./.venv/bin/python", ["-c", script], {
    cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
  });
});

test("IBKR pdf parser kind preserves investment row fields", async () => {
  const script = String.raw`
import sys
from pathlib import Path

sys.path.insert(0, "python/ingest")

import runner
from pdf_statement import PdfStatementParseResult

runner.extract_interactive_brokers_statement_rows = lambda *args, **kwargs: PdfStatementParseResult(
    rows=[
        {
            "transaction_date": "2026-05-01",
            "posted_date": "2026-05-02",
            "description_raw": "Dividend HY9H",
            "amount_original_signed": "1.04",
            "currency_original": "USD",
            "transaction_type_raw": "Dividend",
            "security_symbol": "HY9H",
            "security_name": "SK HYNIX INC-GDS",
            "security_isin": "US78392B1070",
            "quantity": "9",
            "unit_price_original": "0.115555",
            "fees_original": "0",
            "fx_rate": "0.85318",
        }
    ],
    markdown="Activity Statement with one ledger row",
    extraction_method="test",
    parser_model="test",
    statement_net_total="1.04",
)

_, canonical = runner.canonicalize_pdf_statement(
    Path("statement.pdf"),
    {
        "default_currency": "EUR",
        "normalization_rules_json": {
            "parser_kind": "ibkr_activity_statement_pdf",
        },
    },
    reference_date="2026-05-02",
)

rows = canonical.normalized.to_dict(orient="records")
assert len(rows) == 1
assert rows[0]["transaction_date"] == "2026-05-01"
assert rows[0]["posted_date"] == "2026-05-02"
assert rows[0]["amount_original_signed"] == "1.04"
assert rows[0]["currency_original"] == "USD"
assert rows[0]["security_symbol"] == "HY9H"
assert rows[0]["security_name"] == "SK HYNIX INC-GDS"
assert rows[0]["security_isin"] == "US78392B1070"
assert rows[0]["quantity"] == "9"
assert rows[0]["unit_price_original"] == "0.115555"
assert rows[0]["fees_original"] == "0"
assert rows[0]["fx_rate"] == "0.85318"
`;

  await execFileAsync("./.venv/bin/python", ["-c", script], {
    cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
  });
});

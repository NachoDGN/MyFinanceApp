import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import { validateSpreadsheetFile } from "../packages/domain/src/repository.ts";

const execFileAsync = promisify(execFile);

test("validateSpreadsheetFile reports mixed date representations and suspicious text encoding", async () => {
  const directory = await mkdtemp(join(tmpdir(), "myfinance-validator-"));
  const workbookPath = join(directory, "validator-sample.xlsx");

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
          "ws.append(['Fecha de operacion', 'Concepto'])",
          "ws.append(['24/03/2026', 'Compra ETF'])",
          "ws.append([date(2026, 3, 25), 'Liquidaci√≥n intereses'])",
          "wb.save(path)",
        ].join("; "),
      ],
      {
        cwd: "/Users/ignaciodegregorionoblejas/Desktop/Projects/MyFinanceApp",
      },
    );

    const validation = await validateSpreadsheetFile(workbookPath);
    const codes = validation.issues.map((issue) => issue.code).sort();

    assert.deepEqual(codes, [
      "mixed_date_representations",
      "suspicious_text_encoding",
    ]);
    assert.equal(validation.issues[0]?.sheetName, "Movimientos");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

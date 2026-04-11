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

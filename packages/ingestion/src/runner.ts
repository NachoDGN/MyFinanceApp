import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { promisify } from "node:util";

import type {
  DomainDataset,
  FileKind,
  FundOrderHistoryRow,
  ImportCommitResult,
  ImportExecutionInput,
  ImportPreviewResult,
} from "@myfinance/domain";
import {
  parseMyInvestorFundOrderHistoryRows,
  todayIso,
} from "@myfinance/domain";

import type {
  DeterministicImportResult,
  SpreadsheetFileValidationResult,
  SpreadsheetTablePreview,
  SpreadsheetWorkbookPreview,
} from "./types";

const execFileAsync = promisify(execFile);

function normalizeRunnerError(error: unknown) {
  const stderr =
    typeof error === "object" &&
    error &&
    "stderr" in error &&
    typeof error.stderr === "string"
      ? error.stderr.trim()
      : "";

  if (stderr) {
    const lastLine = stderr
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .at(-1);
    const message = (lastLine ?? stderr).replace(/^[A-Za-z_][\w.]*:\s*/, "");
    return new Error(message || "The spreadsheet import runner failed.");
  }

  if (error instanceof Error) {
    return error;
  }

  return new Error("The spreadsheet import runner failed.");
}

export function normalizeImportExecutionInput(
  input: ImportExecutionInput,
): Required<ImportExecutionInput> {
  const originalFilename =
    input.originalFilename ??
    (input.filePath ? basename(input.filePath) : undefined) ??
    "import.csv";

  return {
    accountId: input.accountId,
    templateId: input.templateId,
    originalFilename,
    filePath: input.filePath ?? null,
  };
}

function createRunnerTemplate(dataset: DomainDataset, templateId: string) {
  const template = dataset.templates.find((row) => row.id === templateId);
  if (!template) {
    throw new Error(`Template ${templateId} not found.`);
  }

  return {
    file_kind: template.fileKind,
    sheet_name: template.sheetName,
    header_row_index: template.headerRowIndex,
    rows_to_skip_before_header: template.rowsToSkipBeforeHeader,
    rows_to_skip_after_header: template.rowsToSkipAfterHeader,
    delimiter: template.delimiter,
    encoding: template.encoding,
    decimal_separator: template.decimalSeparator,
    thousands_separator: template.thousandsSeparator,
    date_format: template.dateFormat,
    default_currency: template.defaultCurrency,
    column_map_json: template.columnMapJson,
    sign_logic_json: template.signLogicJson,
    normalization_rules_json: template.normalizationRulesJson,
  };
}

function resolveIngestRunnerPath() {
  if (process.env.PYTHON_INGEST_RUNNER) {
    return process.env.PYTHON_INGEST_RUNNER;
  }

  let currentDirectory = process.cwd();
  while (true) {
    const candidate = resolve(currentDirectory, "python/ingest/runner.py");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parentDirectory = dirname(currentDirectory);
    if (parentDirectory === currentDirectory) {
      break;
    }
    currentDirectory = parentDirectory;
  }

  return resolve(process.cwd(), "python/ingest/runner.py");
}

function resolvePythonBin() {
  if (process.env.PYTHON_BIN) {
    return process.env.PYTHON_BIN;
  }

  let currentDirectory = process.cwd();
  while (true) {
    const candidate = resolve(currentDirectory, ".venv/bin/python");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parentDirectory = dirname(currentDirectory);
    if (parentDirectory === currentDirectory) {
      break;
    }
    currentDirectory = parentDirectory;
  }

  return "python3";
}

function buildRunnerArgs(
  mode:
    | "preview"
    | "commit"
    | "inspect-workbook"
    | "preview-table"
    | "validate-workbook",
  input: Record<string, string | number | null | undefined>,
) {
  const args = [resolveIngestRunnerPath(), mode];
  for (const [key, value] of Object.entries(input)) {
    if (value === null || value === undefined || value === "") continue;
    args.push(`--${key}`);
    args.push(String(value));
  }
  return args;
}

export function columnLetterToIndex(columnLetter: string) {
  let result = 0;
  for (const character of columnLetter.trim().toUpperCase()) {
    result = result * 26 + (character.charCodeAt(0) - 64);
  }
  return result - 1;
}

export async function inspectSpreadsheetWorkbook(
  filePath: string,
): Promise<SpreadsheetWorkbookPreview> {
  try {
    const { stdout } = await execFileAsync(
      resolvePythonBin(),
      buildRunnerArgs("inspect-workbook", {
        "file-path": filePath,
      }),
    );

    return JSON.parse(stdout) as SpreadsheetWorkbookPreview;
  } catch (error) {
    throw normalizeRunnerError(error);
  }
}

export async function validateSpreadsheetFile(
  filePath: string,
): Promise<SpreadsheetFileValidationResult> {
  try {
    const { stdout } = await execFileAsync(
      resolvePythonBin(),
      buildRunnerArgs("validate-workbook", {
        "file-path": filePath,
      }),
    );

    return JSON.parse(stdout) as SpreadsheetFileValidationResult;
  } catch (error) {
    throw normalizeRunnerError(error);
  }
}

export async function previewSpreadsheetTable(input: {
  filePath: string;
  fileKind: FileKind;
  sheetName?: string | null;
  headerRowIndex: number;
  rowsToSkipBeforeHeader?: number;
  startColumnIndex?: number;
  delimiter?: string | null;
  encoding?: string | null;
}): Promise<SpreadsheetTablePreview> {
  try {
    const { stdout } = await execFileAsync(
      resolvePythonBin(),
      buildRunnerArgs("preview-table", {
        "file-path": input.filePath,
        "file-kind": input.fileKind,
        "sheet-name": input.sheetName ?? null,
        "header-row-index": input.headerRowIndex,
        "rows-to-skip-before-header": input.rowsToSkipBeforeHeader ?? 0,
        "start-column-index": input.startColumnIndex ?? 0,
        delimiter: input.delimiter ?? null,
        encoding: input.encoding ?? null,
      }),
    );

    return JSON.parse(stdout) as SpreadsheetTablePreview;
  } catch (error) {
    throw normalizeRunnerError(error);
  }
}

export async function parseMyInvestorFundOrderHistorySpreadsheet(
  filePath: string,
  accountId = "fund-order-history-preview",
): Promise<FundOrderHistoryRow[]> {
  const workbookPreview = await inspectSpreadsheetWorkbook(filePath);
  if (!["csv", "xls", "xlsx"].includes(workbookPreview.fileKind)) {
    throw new Error(
      "Fund-order spreadsheet imports only support CSV and Excel files.",
    );
  }

  const sheetName =
    workbookPreview.fileKind === "csv"
      ? null
      : (workbookPreview.sheetPreviews[0]?.sheetName ?? null);
  const template = {
    file_kind: workbookPreview.fileKind,
    sheet_name: sheetName,
    header_row_index: 1,
    rows_to_skip_before_header: 0,
    rows_to_skip_after_header: 0,
    delimiter:
      workbookPreview.fileKind === "csv"
        ? (workbookPreview.delimiter ?? ",")
        : null,
    encoding:
      workbookPreview.fileKind === "csv"
        ? (workbookPreview.encoding ?? "utf-8")
        : null,
    default_currency: "EUR",
    column_map_json: {
      transaction_date: "Fecha de la orden",
      amount_original_signed: "Importe estimado",
      transaction_type_raw: "Estado",
      security_isin: "ISIN",
      quantity: "Nº de participaciones",
    },
    sign_logic_json: {
      mode: "signed_amount",
      invert_sign: true,
    },
    normalization_rules_json: {
      date_day_first: true,
      start_column_index: 0,
      start_column_letter: "A",
    },
  };

  try {
    const { stdout } = await execFileAsync(
      resolvePythonBin(),
      buildRunnerArgs("preview", {
        "file-path": filePath,
        "account-id": accountId,
        "template-id": "fund-order-history-template",
        "reference-date": todayIso(),
        "template-json": JSON.stringify(template),
      }),
    );

    const result = JSON.parse(stdout) as DeterministicImportResult;
    return parseMyInvestorFundOrderHistoryRows(result.normalizedRows ?? []);
  } catch (error) {
    throw normalizeRunnerError(error);
  }
}

export async function runDeterministicImport(
  mode: "preview" | "commit",
  input: ImportExecutionInput,
  dataset: DomainDataset,
): Promise<DeterministicImportResult> {
  const normalizedInput = normalizeImportExecutionInput(input);
  if (!normalizedInput.filePath) {
    throw new Error(
      "A filePath is required to run the pandas ingestion wrapper.",
    );
  }

  try {
    const { stdout } = await execFileAsync(
      resolvePythonBin(),
      buildRunnerArgs(mode, {
        "file-path": normalizedInput.filePath,
        "account-id": normalizedInput.accountId,
        "template-id": normalizedInput.templateId,
        "reference-date": todayIso(),
        "template-json": JSON.stringify(
          createRunnerTemplate(dataset, normalizedInput.templateId),
        ),
      }),
    );

    return JSON.parse(stdout) as DeterministicImportResult;
  } catch (error) {
    throw normalizeRunnerError(error);
  }
}

export function sanitizeImportResult(result: DeterministicImportResult) {
  const { normalizedRows: _normalizedRows, ...publicResult } = result;
  return publicResult as ImportPreviewResult | ImportCommitResult;
}

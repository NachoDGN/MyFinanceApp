import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { promisify } from "node:util";

import { Decimal } from "decimal.js";

import { todayIso } from "./finance";
import {
  parseMyInvestorFundOrderHistoryRows,
  type FundOrderHistoryRow,
} from "./fund-order-history";
import {
  extractIsinFromText,
  normalizeSecurityIdentifier,
  normalizeSecurityText,
} from "./text";
import { isCreditCardSettlementText } from "./transaction-review";
import type {
  AddOpeningPositionInput,
  ApplyRuleDraftInput,
  AuditEvent,
  CreateAccountInput,
  CreateCategoryInput,
  CreateEntityInput,
  CreateManualInvestmentInput,
  CreditCardStatementImportInput,
  CreditCardStatementImportResult,
  CreateRuleInput,
  CreateTemplateInput,
  DeleteAccountInput,
  DeleteCategoryInput,
  DeleteEntityInput,
  DeleteHoldingAdjustmentInput,
  DeleteManualInvestmentInput,
  DeleteTemplateInput,
  DomainDataset,
  ImportCommitResult,
  ImportExecutionInput,
  ImportPreviewResult,
  JobRunResult,
  QueueRuleDraftInput,
  RecordManualInvestmentValuationInput,
  ResetWorkspaceInput,
  ResetWorkspaceResult,
  PeriodSelection,
  Scope,
  Transaction,
  UpdateAccountInput,
  UpdateManualInvestmentInput,
  UpdateEntityInput,
  UpdateWorkspaceProfileInput,
  UpdateTransactionInput,
  FileKind,
  ImportFileValidationIssue,
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

type CanonicalImportRow = {
  transaction_date: string;
  posted_date?: string | null;
  description_raw: string;
  amount_original_signed: string;
  currency_original?: string | null;
  balance_original?: string | null;
  external_reference?: string | null;
  transaction_type_raw?: string | null;
  security_isin?: string | null;
  security_symbol?: string | null;
  security_name?: string | null;
  quantity?: string | null;
  unit_price_original?: string | null;
  fees_original?: string | null;
  fx_rate?: string | null;
  raw_row_json?: string | null;
};

type DeterministicImportResult = (ImportPreviewResult | ImportCommitResult) & {
  normalizedRows?: CanonicalImportRow[];
};

export interface SpreadsheetSheetPreview {
  sheetName: string | null;
  previewCsv: string;
}

export interface SpreadsheetWorkbookPreview {
  fileKind: FileKind;
  delimiter?: string | null;
  encoding?: string | null;
  sheetPreviews: SpreadsheetSheetPreview[];
}

export interface SpreadsheetTablePreview {
  sheetName: string | null;
  previewCsv: string;
  headers: string[];
}

export interface SpreadsheetFileValidationResult {
  fileKind: FileKind;
  issues: ImportFileValidationIssue[];
}

export interface FinanceRepository {
  getDataset(): Promise<DomainDataset>;
  searchTransactions(input: {
    dataset: DomainDataset;
    scope: Scope;
    period: PeriodSelection;
    referenceDate: string;
    query: string;
  }): Promise<{
    query: string;
    rows: Array<{
      transaction: Transaction;
      originalText: string;
      contextualizedText: string;
      documentSummary: string;
      searchDiagnostics: {
        sourceBatchKey: string;
        hybridScore: number;
        semanticDistance: number | null;
        rerankScore: number | null;
        bm25Score: number | null;
        semanticRank: number | null;
        rerankRank: number | null;
        keywordRank: number | null;
        matchedBy: Array<"semantic" | "keyword">;
        direction: "debit" | "credit" | "neutral";
        reviewState: "pending_enrichment" | "needs_review" | "resolved";
      } | null;
    }>;
    semanticCandidateCount: number;
    keywordCandidateCount: number;
    warnings: string[];
    filters: {
      accountIds: string[];
      entityIds: string[];
      accountTypes: Array<
        | "checking"
        | "savings"
        | "company_bank"
        | "brokerage_cash"
        | "brokerage_account"
        | "credit_card"
        | "other"
      >;
      entityKinds: Array<"personal" | "company">;
      reviewStates: Array<
        "pending_enrichment" | "needs_review" | "resolved" | "unresolved"
      >;
      directions: Array<"credit" | "debit">;
      dateStart: string | null;
      dateEnd: string | null;
      usedScopeFallback: boolean;
      usedPeriodFallback: boolean;
      hasExplicitScopeConstraint: boolean;
      hasExplicitTimeConstraint: boolean;
      explanation: string;
    };
  }>;
  updateWorkspaceProfile(
    input: UpdateWorkspaceProfileInput,
  ): Promise<{ applied: boolean; profileId: string }>;
  createEntity(
    input: CreateEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  updateEntity(
    input: UpdateEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  deleteEntity(
    input: DeleteEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  createAccount(
    input: CreateAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  updateAccount(
    input: UpdateAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  deleteAccount(
    input: DeleteAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  resetWorkspace(input: ResetWorkspaceInput): Promise<ResetWorkspaceResult>;
  updateTransaction(input: UpdateTransactionInput): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }>;
  createRule(
    input: CreateRuleInput,
  ): Promise<{ applied: boolean; ruleId: string }>;
  createTemplate(
    input: CreateTemplateInput,
  ): Promise<{ applied: boolean; templateId: string }>;
  deleteTemplate(
    input: DeleteTemplateInput,
  ): Promise<{ applied: boolean; templateId: string }>;
  createCategory(
    input: CreateCategoryInput,
  ): Promise<{ applied: boolean; categoryCode: string }>;
  deleteCategory(
    input: DeleteCategoryInput,
  ): Promise<{ applied: boolean; categoryCode: string }>;
  addOpeningPosition(
    input: AddOpeningPositionInput,
  ): Promise<{ applied: boolean; adjustmentId: string }>;
  deleteHoldingAdjustment(
    input: DeleteHoldingAdjustmentInput,
  ): Promise<{ applied: boolean; adjustmentId: string }>;
  createManualInvestment(input: CreateManualInvestmentInput): Promise<{
    applied: boolean;
    manualInvestmentId: string;
    valuationId: string;
  }>;
  updateManualInvestment(input: UpdateManualInvestmentInput): Promise<{
    applied: boolean;
    manualInvestmentId: string;
  }>;
  recordManualInvestmentValuation(
    input: RecordManualInvestmentValuationInput,
  ): Promise<{
    applied: boolean;
    manualInvestmentId: string;
    valuationId: string;
  }>;
  deleteManualInvestment(
    input: DeleteManualInvestmentInput,
  ): Promise<{ applied: boolean; manualInvestmentId: string }>;
  queueRuleDraft(
    input: QueueRuleDraftInput,
  ): Promise<{ applied: boolean; jobId: string }>;
  applyRuleDraft(
    input: ApplyRuleDraftInput,
  ): Promise<{ applied: boolean; ruleId: string }>;
  previewImport(input: ImportExecutionInput): Promise<ImportPreviewResult>;
  commitImport(input: ImportExecutionInput): Promise<ImportCommitResult>;
  commitCreditCardStatementImport(
    input: CreditCardStatementImportInput,
  ): Promise<CreditCardStatementImportResult>;
  runPendingJobs(apply: boolean): Promise<JobRunResult>;
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
    throw new Error("Fund-order spreadsheet imports only support CSV and Excel files.");
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

function normalizeDescriptionForImport(value: string) {
  return normalizeSecurityText(value);
}

function normalizeFingerprintText(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function truncateDecimalTowardsZero(value: Decimal) {
  return value.isNegative() ? value.ceil() : value.floor();
}

function matchesRoundedWholeValue(left: string, right: string) {
  const leftValue = new Decimal(left);
  const rightValue = new Decimal(right);
  if (leftValue.eq(rightValue)) {
    return false;
  }
  if (leftValue.minus(rightValue).abs().gte(1)) {
    return false;
  }

  return (
    (leftValue.isInteger() &&
      truncateDecimalTowardsZero(rightValue).eq(leftValue)) ||
    (rightValue.isInteger() &&
      truncateDecimalTowardsZero(leftValue).eq(rightValue))
  );
}

function resolveSecurityId(
  dataset: DomainDataset,
  row: Pick<
    CanonicalImportRow,
    "external_reference" | "security_isin" | "security_symbol" | "security_name"
  >,
) {
  const securityIsin =
    normalizeSecurityIdentifier(row.security_isin) ||
    extractIsinFromText(row.external_reference);
  const symbol = normalizeFingerprintText(row.security_symbol);
  const securityName = normalizeFingerprintText(row.security_name);

  if (!securityIsin && !symbol && !securityName) {
    return null;
  }

  if (securityIsin) {
    const exactSecurity = dataset.securities.find(
      (security) =>
        normalizeSecurityIdentifier(security.isin) === securityIsin,
    );
    if (exactSecurity) {
      return exactSecurity.id;
    }
  }

  const directMatch = dataset.securities.find((security) => {
    const candidates = [
      security.providerSymbol,
      security.canonicalSymbol,
      security.displaySymbol,
      security.name,
    ].map((value) => normalizeFingerprintText(value));

    return (
      (securityIsin &&
        normalizeFingerprintText(security.isin) ===
          normalizeFingerprintText(securityIsin)) ||
      (symbol && candidates.includes(symbol)) ||
      (securityName && candidates.includes(securityName))
    );
  });
  if (directMatch) {
    return directMatch.id;
  }

  const aliasMatch = dataset.securityAliases.find((alias) => {
    const aliasText = normalizeFingerprintText(alias.aliasTextNormalized);
    return (
      (securityIsin && aliasText === normalizeFingerprintText(securityIsin)) ||
      (symbol && aliasText === symbol) ||
      (securityName && aliasText === securityName)
    );
  });

  return aliasMatch?.securityId ?? null;
}

function safeParseRawRowJson(row: CanonicalImportRow) {
  if (!row.raw_row_json) return {};
  try {
    const parsed = JSON.parse(row.raw_row_json) as Record<string, unknown>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

type InvestmentDuplicateCandidate = {
  amountOriginal: string;
  unitPriceOriginal: string | null;
};

function buildInvestmentDuplicateSignature(
  accountId: string,
  row: {
    transactionDate: string;
    postedDate: string;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    quantity: string | null;
    securityId: string | null;
    transactionTypeRaw: string | null;
  },
) {
  if (!row.securityId) {
    return null;
  }

  return [
    "investment",
    accountId,
    row.transactionDate,
    row.postedDate,
    row.currencyOriginal,
    row.securityId,
    normalizeFingerprintText(row.descriptionRaw),
    normalizeFingerprintText(row.quantity),
    normalizeFingerprintText(row.transactionTypeRaw),
    new Decimal(row.amountOriginal).isNegative() ? "outflow" : "inflow",
  ].join("|");
}

function isRoundedInvestmentDuplicate(
  candidate: InvestmentDuplicateCandidate,
  existing: InvestmentDuplicateCandidate,
) {
  if (
    candidate.unitPriceOriginal &&
    existing.unitPriceOriginal &&
    new Decimal(candidate.unitPriceOriginal)
      .minus(existing.unitPriceOriginal)
      .abs()
      .gte(1)
  ) {
    return false;
  }

  return (
    matchesRoundedWholeValue(
      candidate.amountOriginal,
      existing.amountOriginal,
    ) ||
    (candidate.unitPriceOriginal !== null &&
      existing.unitPriceOriginal !== null &&
      matchesRoundedWholeValue(
        candidate.unitPriceOriginal,
        existing.unitPriceOriginal,
      ))
  );
}

function buildImportFingerprint(
  accountId: string,
  row: {
    transactionDate: string;
    postedDate: string;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    externalReference: string;
    securityIsin: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    securitySymbol: string | null;
    securityName: string | null;
    transactionTypeRaw: string | null;
  },
) {
  const fingerprintReference =
    row.externalReference || row.securityIsin || "";
  return createHash("sha256")
    .update(
      [
        accountId,
        row.transactionDate,
        row.postedDate,
        row.amountOriginal,
        row.currencyOriginal,
        normalizeFingerprintText(row.descriptionRaw),
        normalizeFingerprintText(fingerprintReference),
        normalizeFingerprintText(row.quantity),
        normalizeFingerprintText(row.unitPriceOriginal),
        normalizeFingerprintText(row.securitySymbol),
        normalizeFingerprintText(row.securityName),
        normalizeFingerprintText(row.transactionTypeRaw),
      ].join("|"),
    )
    .digest("hex");
}

function resolveFxRateToEur(
  dataset: DomainDataset,
  currencyOriginal: string,
  transactionDate: string,
) {
  if (currencyOriginal === "EUR") {
    return "1.00000000";
  }

  const directRate = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === currencyOriginal &&
        row.quoteCurrency === "EUR" &&
        row.asOfDate <= transactionDate,
    )
    .sort((left, right) => right.asOfDate.localeCompare(left.asOfDate))[0];

  return directRate?.rate ?? null;
}

export function buildImportedTransactions(
  dataset: DomainDataset,
  input: Required<ImportExecutionInput>,
  importBatchId: string,
  rows: CanonicalImportRow[],
) {
  const account = dataset.accounts.find((row) => row.id === input.accountId);
  if (!account) {
    throw new Error(`Account ${input.accountId} not found.`);
  }

  const accountsById = new Map(
    dataset.accounts.map((datasetAccount) => [
      datasetAccount.id,
      datasetAccount,
    ]),
  );
  const existingFingerprints = new Set(
    dataset.transactions.map((transaction) => transaction.sourceFingerprint),
  );
  const existingInvestmentDuplicateCandidates = new Map<
    string,
    InvestmentDuplicateCandidate[]
  >();
  for (const transaction of dataset.transactions) {
    const transactionAccount = accountsById.get(transaction.accountId);
    if (
      transactionAccount?.assetDomain !== "investment" ||
      transaction.voidedAt ||
      transaction.excludeFromAnalytics
    ) {
      continue;
    }

    const transactionTypeRaw =
      typeof transaction.rawPayload?._import === "object" &&
      transaction.rawPayload._import &&
      "transaction_type_raw" in transaction.rawPayload._import &&
      typeof transaction.rawPayload._import.transaction_type_raw === "string"
        ? transaction.rawPayload._import.transaction_type_raw
        : null;
    const duplicateSignature = buildInvestmentDuplicateSignature(
      transaction.accountId,
      {
        transactionDate: transaction.transactionDate,
        postedDate: transaction.postedDate ?? transaction.transactionDate,
        amountOriginal: transaction.amountOriginal,
        currencyOriginal: transaction.currencyOriginal,
        descriptionRaw: transaction.descriptionRaw,
        quantity: transaction.quantity ?? null,
        securityId: transaction.securityId ?? null,
        transactionTypeRaw,
      },
    );
    if (!duplicateSignature) {
      continue;
    }

    const candidates =
      existingInvestmentDuplicateCandidates.get(duplicateSignature) ?? [];
    candidates.push({
      amountOriginal: transaction.amountOriginal,
      unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    });
    existingInvestmentDuplicateCandidates.set(duplicateSignature, candidates);
  }
  const inserted: Transaction[] = [];
  let duplicateCount = 0;
  const createdAt = new Date().toISOString();

  for (const row of rows) {
    const transactionDate = String(row.transaction_date ?? "").slice(0, 10);
    const descriptionRaw = String(row.description_raw ?? "").trim();
    if (!transactionDate || !descriptionRaw) {
      continue;
    }

    const postedDate =
      String(row.posted_date ?? "").slice(0, 10) || transactionDate;
    const amountOriginal = new Decimal(
      String(row.amount_original_signed ?? "0"),
    ).toFixed(8);
    const currencyOriginal = String(
      row.currency_original ?? account.defaultCurrency ?? "EUR",
    ).toUpperCase();
    const externalReference = String(row.external_reference ?? "");
    const quantity = row.quantity
      ? new Decimal(String(row.quantity)).toFixed(8)
      : null;
    const securityIsin =
      normalizeSecurityIdentifier(row.security_isin) ||
      extractIsinFromText(externalReference) ||
      null;
    const unitPriceOriginal = row.unit_price_original
      ? new Decimal(String(row.unit_price_original)).toFixed(8)
      : quantity &&
          !new Decimal(quantity).eq(0) &&
          !new Decimal(amountOriginal).eq(0)
        ? new Decimal(amountOriginal)
            .abs()
            .div(new Decimal(quantity).abs())
            .toFixed(8)
        : null;
    const securitySymbol = String(row.security_symbol ?? "").trim() || null;
    const securityName = String(row.security_name ?? "").trim() || null;
    const transactionTypeRaw =
      String(row.transaction_type_raw ?? "").trim() || null;
    const sourceFingerprint = buildImportFingerprint(input.accountId, {
      transactionDate,
      postedDate,
      amountOriginal,
      currencyOriginal,
      descriptionRaw,
      externalReference,
      securityIsin,
      quantity,
      unitPriceOriginal,
      securitySymbol,
      securityName,
      transactionTypeRaw,
    });

    if (existingFingerprints.has(sourceFingerprint)) {
      duplicateCount += 1;
      continue;
    }

    const importedFxRate = row.fx_rate
      ? new Decimal(String(row.fx_rate)).toFixed(8)
      : null;
    const fxRateToEur =
      importedFxRate ??
      resolveFxRateToEur(dataset, currencyOriginal, transactionDate);
    const amountBaseEur = new Decimal(amountOriginal)
      .times(new Decimal(fxRateToEur ?? "1"))
      .toFixed(8);
    const rawPayload = {
      ...safeParseRawRowJson(row),
      _import: {
        posted_date: postedDate,
        balance_original: row.balance_original ?? null,
        external_reference: externalReference || null,
        transaction_type_raw: transactionTypeRaw,
        security_isin: securityIsin,
        security_symbol: securitySymbol,
        security_name: securityName,
        quantity,
        unit_price_original: unitPriceOriginal,
        fees_original: row.fees_original ?? null,
        fx_rate: importedFxRate,
      },
    } satisfies Record<string, unknown>;
    const securityId = resolveSecurityId(dataset, row);
    if (account.assetDomain === "investment") {
      const duplicateSignature = buildInvestmentDuplicateSignature(account.id, {
        transactionDate,
        postedDate,
        amountOriginal,
        currencyOriginal,
        descriptionRaw,
        quantity,
        securityId,
        transactionTypeRaw,
      });
      if (duplicateSignature) {
        const candidate = {
          amountOriginal,
          unitPriceOriginal,
        } satisfies InvestmentDuplicateCandidate;
        const existingCandidates =
          existingInvestmentDuplicateCandidates.get(duplicateSignature) ?? [];
        if (
          existingCandidates.some((existing) =>
            isRoundedInvestmentDuplicate(candidate, existing),
          )
        ) {
          duplicateCount += 1;
          continue;
        }
        existingCandidates.push(candidate);
        existingInvestmentDuplicateCandidates.set(
          duplicateSignature,
          existingCandidates,
        );
      }
    }
    const initialReviewReasons = [
      "Queued for automatic transaction analysis.",
      isCreditCardSettlementText(descriptionRaw)
        ? "Upload the matching credit-card statement to resolve category KPIs."
        : null,
      currencyOriginal !== "EUR" && !fxRateToEur
        ? "Missing FX rate for base-currency conversion."
        : null,
      account.assetDomain === "investment" &&
      !securityId &&
      (securitySymbol || securityName)
        ? "Security mapping unresolved."
        : null,
    ].filter(Boolean);

    inserted.push({
      id: randomUUID(),
      userId: dataset.profile.id,
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      importBatchId,
      providerName: null,
      providerRecordId: null,
      sourceFingerprint,
      duplicateKey: sourceFingerprint,
      transactionDate,
      postedDate,
      amountOriginal,
      currencyOriginal,
      amountBaseEur,
      fxRateToEur,
      descriptionRaw,
      descriptionClean: normalizeDescriptionForImport(descriptionRaw),
      merchantNormalized: null,
      counterpartyName: null,
      transactionClass: "unknown",
      categoryCode:
        account.assetDomain === "investment"
          ? "uncategorized_investment"
          : null,
      subcategoryCode: null,
      transferGroupId: null,
      relatedAccountId: null,
      relatedTransactionId: null,
      transferMatchStatus: "not_transfer",
      crossEntityFlag: false,
      reimbursementStatus: "none",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: initialReviewReasons.join(" "),
      excludeFromAnalytics: false,
      correctionOfTransactionId: null,
      voidedAt: null,
      manualNotes: null,
      llmPayload: {
        analysisStatus: "pending",
        explanation: null,
        model: null,
        error: null,
        queuedAt: createdAt,
      },
      rawPayload,
      securityId,
      quantity,
      unitPriceOriginal,
      creditCardStatementStatus: isCreditCardSettlementText(descriptionRaw)
        ? "upload_required"
        : "not_applicable",
      linkedCreditCardAccountId: null,
      createdAt,
      updatedAt: createdAt,
    });
    existingFingerprints.add(sourceFingerprint);
  }

  return {
    inserted,
    duplicateCount,
  };
}

export function getAccountById(
  accounts: DomainDataset["accounts"],
  accountId: string,
) {
  return accounts.find((account) => account.id === accountId);
}

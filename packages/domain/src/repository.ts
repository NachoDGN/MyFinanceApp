import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { promisify } from "node:util";
import { Decimal } from "decimal.js";

import type {
  Account,
  AccountBalanceSnapshot,
  AddOpeningPositionInput,
  ApplyRuleDraftInput,
  AuditEvent,
  CreateRuleInput,
  CreateTemplateInput,
  DomainDataset,
  HoldingAdjustment,
  ImportExecutionInput,
  ImportCommitResult,
  ImportPreviewResult,
  JobRunResult,
  QueueRuleDraftInput,
  Transaction,
  UpdateTransactionInput,
} from "./types";
import { seedDataset, SEEDED_USER_ID, TODAY_ISO } from "./fixtures";
import { parseRuleDraftRequest } from "./rule-drafts";

const execFileAsync = promisify(execFile);

type CanonicalImportRow = {
  transaction_date: string;
  posted_date?: string | null;
  description_raw: string;
  amount_original_signed: string;
  currency_original?: string | null;
  balance_original?: string | null;
  external_reference?: string | null;
  transaction_type_raw?: string | null;
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

export interface FinanceRepository {
  getDataset(): Promise<DomainDataset>;
  updateTransaction(input: UpdateTransactionInput): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }>;
  createRule(input: CreateRuleInput): Promise<{ applied: boolean; ruleId: string }>;
  createTemplate(
    input: CreateTemplateInput,
  ): Promise<{ applied: boolean; templateId: string }>;
  addOpeningPosition(
    input: AddOpeningPositionInput,
  ): Promise<{ applied: boolean; adjustmentId: string }>;
  queueRuleDraft(input: QueueRuleDraftInput): Promise<{ applied: boolean; jobId: string }>;
  applyRuleDraft(input: ApplyRuleDraftInput): Promise<{ applied: boolean; ruleId: string }>;
  previewImport(input: ImportExecutionInput): Promise<ImportPreviewResult>;
  commitImport(input: ImportExecutionInput): Promise<ImportCommitResult>;
  runPendingJobs(apply: boolean): Promise<JobRunResult>;
}

function cloneDataset(): DomainDataset {
  return structuredClone(seedDataset);
}

function createAuditEvent(
  sourceChannel: AuditEvent["sourceChannel"],
  actorName: string,
  commandName: string,
  objectType: string,
  objectId: string,
  beforeJson: Record<string, unknown> | null,
  afterJson: Record<string, unknown> | null,
): AuditEvent {
  return {
    id: randomUUID(),
    actorType: "agent",
    actorId: SEEDED_USER_ID,
    actorName,
    sourceChannel,
    commandName,
    objectType,
    objectId,
    beforeJson,
    afterJson,
    createdAt: new Date().toISOString(),
    notes: null,
  };
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

export async function runDeterministicImport(
  mode: "preview" | "commit",
  input: ImportExecutionInput,
  dataset: DomainDataset,
): Promise<DeterministicImportResult> {
  const normalizedInput = normalizeImportExecutionInput(input);
  if (!normalizedInput.filePath) {
    throw new Error("A filePath is required to run the pandas ingestion wrapper.");
  }

  const runnerPath = resolveIngestRunnerPath();
  const pythonBin = resolvePythonBin();
  const templateJson = JSON.stringify(
    createRunnerTemplate(dataset, normalizedInput.templateId),
  );
  const { stdout } = await execFileAsync(pythonBin, [
    runnerPath,
    mode,
    "--file-path",
    normalizedInput.filePath,
    "--account-id",
    normalizedInput.accountId,
    "--template-id",
    normalizedInput.templateId,
    "--template-json",
    templateJson,
  ]);

  return JSON.parse(stdout) as ImportPreviewResult | ImportCommitResult;
}

export function sanitizeImportResult(result: DeterministicImportResult) {
  const { normalizedRows: _normalizedRows, ...publicResult } = result;
  return publicResult as ImportPreviewResult | ImportCommitResult;
}

function normalizeDescriptionForImport(value: string) {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeFingerprintText(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function resolveSecurityId(
  dataset: DomainDataset,
  row: Pick<CanonicalImportRow, "security_symbol" | "security_name">,
) {
  const symbol = normalizeFingerprintText(row.security_symbol);
  const securityName = normalizeFingerprintText(row.security_name);

  if (!symbol && !securityName) {
    return null;
  }

  const directMatch = dataset.securities.find((security) => {
    const candidates = [
      security.providerSymbol,
      security.canonicalSymbol,
      security.displaySymbol,
      security.name,
    ].map((value) => normalizeFingerprintText(value));
    return (symbol && candidates.includes(symbol)) || (securityName && candidates.includes(securityName));
  });

  if (directMatch) {
    return directMatch.id;
  }

  const aliasMatch = dataset.securityAliases.find((alias) => {
    const aliasText = normalizeFingerprintText(alias.aliasTextNormalized);
    return (symbol && aliasText === symbol) || (securityName && aliasText === securityName);
  });

  return aliasMatch?.securityId ?? null;
}

function safeParseRawRowJson(row: CanonicalImportRow) {
  if (row.raw_row_json) {
    try {
      const parsed = JSON.parse(row.raw_row_json) as Record<string, unknown>;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // Fall through to canonical payload if the Python runner returned malformed row JSON.
    }
  }

  return {
    transaction_date: row.transaction_date,
    posted_date: row.posted_date ?? null,
    description_raw: row.description_raw,
    amount_original_signed: row.amount_original_signed,
    currency_original: row.currency_original ?? null,
    balance_original: row.balance_original ?? null,
    external_reference: row.external_reference ?? null,
    transaction_type_raw: row.transaction_type_raw ?? null,
    security_symbol: row.security_symbol ?? null,
    security_name: row.security_name ?? null,
    quantity: row.quantity ?? null,
    unit_price_original: row.unit_price_original ?? null,
    fees_original: row.fees_original ?? null,
    fx_rate: row.fx_rate ?? null,
  } satisfies Record<string, unknown>;
}

function buildImportFingerprint(
  accountId: string,
  row: {
    transactionDate: string;
    postedDate: string | null;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    externalReference: string;
    quantity: string | null;
    unitPriceOriginal: string | null;
    securitySymbol: string | null;
    securityName: string | null;
    transactionTypeRaw: string | null;
  },
) {
  const components = [
    accountId,
    row.transactionDate,
    row.postedDate ?? "",
    row.amountOriginal,
    row.currencyOriginal,
    normalizeFingerprintText(row.descriptionRaw),
  ];

  const externalReference = normalizeFingerprintText(row.externalReference);
  if (externalReference) {
    components.push(externalReference);
  } else {
    components.push(
      normalizeFingerprintText(row.quantity),
      normalizeFingerprintText(row.unitPriceOriginal),
      normalizeFingerprintText(row.securitySymbol),
      normalizeFingerprintText(row.securityName),
      normalizeFingerprintText(row.transactionTypeRaw),
    );
  }

  return createHash("sha256").update(components.join("|")).digest("hex");
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

  const existingFingerprints = new Set(
    dataset.transactions.map((transaction) => transaction.sourceFingerprint),
  );
  const inserted: Transaction[] = [];
  let duplicateCount = 0;
  const createdAt = new Date().toISOString();

  for (const row of rows) {
    const transactionDate = String(row.transaction_date ?? "").slice(0, 10);
    const postedDateCandidate = String(row.posted_date ?? "").slice(0, 10);
    const postedDate = postedDateCandidate || transactionDate;
    const descriptionRaw = String(row.description_raw ?? "").trim();
    if (!transactionDate || !descriptionRaw) {
      continue;
    }

    const amountOriginal = new Decimal(String(row.amount_original_signed ?? "0")).toFixed(8);
    const currencyOriginal = String(
      row.currency_original ?? account.defaultCurrency ?? "EUR",
    ).toUpperCase();
    const externalReference = String(row.external_reference ?? "");
    const quantity = row.quantity ? new Decimal(String(row.quantity)).toFixed(8) : null;
    const unitPriceOriginal = row.unit_price_original
      ? new Decimal(String(row.unit_price_original)).toFixed(8)
      : null;
    const securitySymbol = String(row.security_symbol ?? "").trim() || null;
    const securityName = String(row.security_name ?? "").trim() || null;
    const transactionTypeRaw = String(row.transaction_type_raw ?? "").trim() || null;
    const sourceFingerprint = buildImportFingerprint(input.accountId, {
      transactionDate,
      postedDate,
      amountOriginal,
      currencyOriginal,
      descriptionRaw,
      externalReference,
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

    const importedFxRate = row.fx_rate ? new Decimal(String(row.fx_rate)).toFixed(8) : null;
    const fxRateToEur = importedFxRate ?? resolveFxRateToEur(dataset, currencyOriginal, transactionDate);
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
        security_symbol: securitySymbol,
        security_name: securityName,
        quantity,
        unit_price_original: unitPriceOriginal,
        fees_original: row.fees_original ?? null,
        fx_rate: importedFxRate,
      },
    } satisfies Record<string, unknown>;

    const categoryCode = account.assetDomain === "investment" ? "uncategorized_investment" : null;
    const securityId = resolveSecurityId(dataset, row);
    const initialReviewReasons = [
      "Pending enrichment pipeline.",
      currencyOriginal !== "EUR" && !fxRateToEur ? "Missing FX rate for base-currency conversion." : null,
      account.assetDomain === "investment" && !securityId && (securitySymbol || securityName)
        ? "Security mapping unresolved."
        : null,
    ].filter(Boolean);

    inserted.push({
      id: randomUUID(),
      userId: SEEDED_USER_ID,
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      importBatchId,
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
      categoryCode,
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

async function processRuleDraftJob(dataset: DomainDataset, job: DomainDataset["jobs"][number]) {
  const requestText = typeof job.payloadJson.requestText === "string" ? job.payloadJson.requestText : "";
  if (!requestText) {
    throw new Error("Rule draft job is missing requestText.");
  }

  const parsedRule = await parseRuleDraftRequest(requestText, dataset);
  job.payloadJson = {
    ...job.payloadJson,
    parsedRule,
  };
  job.status = "completed";
  job.finishedAt = new Date().toISOString();
  job.lastError = null;
}

export class InMemoryFinanceRepository implements FinanceRepository {
  private dataset: DomainDataset = cloneDataset();

  async getDataset(): Promise<DomainDataset> {
    return this.dataset;
  }

  async updateTransaction(input: UpdateTransactionInput) {
    const transaction = this.dataset.transactions.find(
      (item) => item.id === input.transactionId,
    );
    if (!transaction) {
      throw new Error(`Transaction ${input.transactionId} not found.`);
    }

    const nextTransaction: Transaction = {
      ...transaction,
      ...input.patch,
      updatedAt: new Date().toISOString(),
      classificationStatus:
        Object.keys(input.patch).length > 0 ? "manual_override" : transaction.classificationStatus,
      classificationSource:
        Object.keys(input.patch).length > 0 ? "manual" : transaction.classificationSource,
      classificationConfidence:
        Object.keys(input.patch).length > 0 ? "1.00" : transaction.classificationConfidence,
    };

    const auditEvent = createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "transactions.update",
      "transaction",
      transaction.id,
      transaction as unknown as Record<string, unknown>,
      nextTransaction as unknown as Record<string, unknown>,
    );

    let generatedRuleId: string | undefined;

    if (input.apply) {
      Object.assign(transaction, nextTransaction);
      this.dataset.auditEvents.unshift(auditEvent);
      if (input.createRuleFromTransaction) {
        generatedRuleId = randomUUID();
        this.dataset.rules.unshift({
          id: generatedRuleId,
          userId: SEEDED_USER_ID,
          priority: 50,
          active: true,
          scopeJson: { account_id: transaction.accountId },
          conditionsJson: {
            normalized_description_regex: transaction.descriptionClean,
          },
          outputsJson: {
            transaction_class: nextTransaction.transactionClass,
            category_code: nextTransaction.categoryCode,
            economic_entity_id_override: nextTransaction.economicEntityId,
          },
          createdFromTransactionId: transaction.id,
          autoGenerated: true,
          hitCount: 0,
          lastHitAt: null,
          createdAt: auditEvent.createdAt,
          updatedAt: auditEvent.createdAt,
        });
      }
    }

    return {
      applied: input.apply,
      transaction: nextTransaction,
      auditEvent,
      generatedRuleId,
    };
  }

  async createRule(input: CreateRuleInput) {
    const ruleId = randomUUID();
    if (input.apply) {
      this.dataset.rules.unshift({
        id: ruleId,
        userId: SEEDED_USER_ID,
        priority: input.priority,
        active: true,
        scopeJson: input.scopeJson,
        conditionsJson: input.conditionsJson,
        outputsJson: input.outputsJson,
        createdFromTransactionId: null,
        autoGenerated: false,
        hitCount: 0,
        lastHitAt: null,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });
      this.dataset.auditEvents.unshift(
        createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.create",
          "classification_rule",
          ruleId,
          null,
          input.outputsJson,
        ),
      );
    }
    return { applied: input.apply, ruleId };
  }

  async createTemplate(input: CreateTemplateInput) {
    const templateId = randomUUID();
    if (input.apply) {
      this.dataset.templates.unshift({
        ...input.template,
        id: templateId,
        version: 1,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });
      this.dataset.auditEvents.unshift(
        createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "templates.create",
          "import_template",
          templateId,
          null,
          input.template as unknown as Record<string, unknown>,
        ),
      );
    }
    return { applied: input.apply, templateId };
  }

  async addOpeningPosition(input: AddOpeningPositionInput) {
    const adjustmentId = randomUUID();
    if (input.apply) {
      const adjustment: HoldingAdjustment = {
        id: adjustmentId,
        userId: SEEDED_USER_ID,
        entityId: input.entityId,
        accountId: input.accountId,
        securityId: input.securityId,
        effectiveDate: input.effectiveDate,
        shareDelta: input.shareDelta,
        costBasisDeltaEur: input.costBasisDeltaEur ?? null,
        reason: "opening_position",
        note: "Created from CLI/web opening position flow.",
        createdAt: new Date().toISOString(),
      };
      this.dataset.holdingAdjustments.unshift(adjustment);
      this.dataset.auditEvents.unshift(
        createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "positions.add-opening",
          "holding_adjustment",
          adjustmentId,
          null,
          adjustment as unknown as Record<string, unknown>,
        ),
      );
    }
    return { applied: input.apply, adjustmentId };
  }

  async queueRuleDraft(input: QueueRuleDraftInput) {
    const jobId = randomUUID();
    if (input.apply) {
      this.dataset.jobs.unshift({
        id: jobId,
        jobType: "rule_parse",
        payloadJson: {
          requestText: input.requestText,
        },
        status: "queued",
        attempts: 0,
        availableAt: new Date().toISOString(),
        startedAt: null,
        finishedAt: null,
        lastError: null,
        lockedBy: null,
        createdAt: new Date().toISOString(),
      });
      this.dataset.auditEvents.unshift(
        createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.queue-draft",
          "job",
          jobId,
          null,
          { requestText: input.requestText },
        ),
      );
    }
    return { applied: input.apply, jobId };
  }

  async applyRuleDraft(input: ApplyRuleDraftInput) {
    const job = this.dataset.jobs.find((item) => item.id === input.jobId && item.jobType === "rule_parse");
    if (!job) {
      throw new Error(`Rule draft job ${input.jobId} not found.`);
    }

    const parsedRule = job.payloadJson.parsedRule;
    if (!parsedRule || typeof parsedRule !== "object") {
      throw new Error("Rule draft has not been parsed yet.");
    }

    const result = await this.createRule({
      priority: Number((parsedRule as { priority?: unknown }).priority ?? 60),
      scopeJson: ((parsedRule as { scopeJson?: unknown }).scopeJson ?? {}) as Record<string, unknown>,
      conditionsJson: ((parsedRule as { conditionsJson?: unknown }).conditionsJson ?? {}) as Record<string, unknown>,
      outputsJson: ((parsedRule as { outputsJson?: unknown }).outputsJson ?? {}) as Record<string, unknown>,
      actorName: input.actorName,
      sourceChannel: input.sourceChannel,
      apply: input.apply,
    });

    if (input.apply) {
      job.payloadJson = {
        ...job.payloadJson,
        appliedRuleId: result.ruleId,
      };
    }

    return { applied: input.apply, ruleId: result.ruleId };
  }

  async previewImport(input: ImportExecutionInput): Promise<ImportPreviewResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    if (normalizedInput.filePath) {
      const rawResult = await runDeterministicImport(
        "preview",
        normalizedInput,
        this.dataset,
      );
      const prepared = buildImportedTransactions(
        this.dataset,
        normalizedInput,
        "preview-batch",
        rawResult.normalizedRows ?? [],
      );
      const publicResult = sanitizeImportResult(rawResult) as ImportPreviewResult;
      return {
        ...publicResult,
        rowCountDuplicates: prepared.duplicateCount,
      };
    }

    return {
      schemaVersion: "v1",
      accountId: normalizedInput.accountId,
      templateId: normalizedInput.templateId,
      originalFilename: normalizedInput.originalFilename,
      rowCountDetected: 4,
      rowCountParsed: 4,
      rowCountDuplicates: 1,
      rowCountFailed: 0,
      dateRange: { start: "2026-04-01", end: TODAY_ISO },
      sampleRows: [
        {
          transaction_date: "2026-04-01",
          description_raw: "Sample import row",
          amount_original_signed: "-24.50",
          currency_original: "EUR",
        },
        {
          transaction_date: "2026-04-02",
          description_raw: "Transfer to IBKR",
          amount_original_signed: "-2000.00",
          currency_original: "EUR",
        },
      ],
      parseErrors: [],
    };
  }

  async commitImport(input: ImportExecutionInput): Promise<ImportCommitResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    const commitResult = normalizedInput.filePath
      ? (await runDeterministicImport(
          "commit",
          normalizedInput,
          this.dataset,
        ))
      : null;
    const importBatchId =
      (commitResult as ImportCommitResult | null)?.importBatchId ?? randomUUID();
    const preparedTransactions =
      commitResult && normalizedInput.filePath
        ? buildImportedTransactions(
            this.dataset,
            normalizedInput,
            importBatchId,
            commitResult.normalizedRows ?? [],
          )
        : null;
    const preview =
      commitResult && normalizedInput.filePath
        ? ({
            ...(sanitizeImportResult(commitResult) as ImportCommitResult),
            rowCountDuplicates: preparedTransactions?.duplicateCount ?? 0,
          } satisfies ImportCommitResult)
        : await this.previewImport(normalizedInput);
    const transactionIds =
      preparedTransactions?.inserted.map((transaction) => transaction.id) ??
      [randomUUID(), randomUUID(), randomUUID()];
    const jobsQueued =
      (commitResult as ImportCommitResult | null)?.jobsQueued ??
      ([
        "classification",
        "transfer_rematch",
        "position_rebuild",
        "metric_refresh",
        "insight_refresh",
      ] as const);
    this.dataset.importBatches.unshift({
      id: importBatchId,
      userId: SEEDED_USER_ID,
      accountId: normalizedInput.accountId,
      templateId: normalizedInput.templateId,
      storagePath: normalizedInput.filePath
        ? `private-imports/local/${normalizedInput.originalFilename}`
        : `private-imports/manual/${normalizedInput.originalFilename}`,
      originalFilename: normalizedInput.originalFilename,
      fileSha256: randomUUID().replace(/-/g, ""),
      status: "committed",
      rowCountDetected: preview.rowCountDetected,
      rowCountParsed: preview.rowCountParsed,
      rowCountInserted: preparedTransactions?.inserted.length ?? 3,
      rowCountDuplicates: preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates,
      rowCountFailed: preview.rowCountFailed,
      previewSummaryJson: {
        sampleRows: preview.sampleRows,
      },
      commitSummaryJson: {
        transactionIds,
        jobsQueued,
      },
      importedByActor: "Seeded Developer",
      importedAt: new Date().toISOString(),
      classificationTriggeredAt: new Date().toISOString(),
      notes: "Committed from in-memory preview flow.",
      detectedDateRange: preview.dateRange,
    });
    if (preparedTransactions) {
      this.dataset.transactions.unshift(...preparedTransactions.inserted);
    }
    for (const jobType of jobsQueued) {
      this.dataset.jobs.unshift({
        id: randomUUID(),
        jobType,
        payloadJson: { importBatchId, accountId: normalizedInput.accountId },
        status: "queued",
        attempts: 0,
        availableAt: new Date().toISOString(),
        startedAt: null,
        finishedAt: null,
        lastError: null,
        lockedBy: null,
        createdAt: new Date().toISOString(),
      });
    }
    this.dataset.auditEvents.unshift(
      createAuditEvent(
        "system",
        "import-commit",
        "imports.commit",
        "import_batch",
        importBatchId,
        null,
        {
          originalFilename: normalizedInput.originalFilename,
          rowCountInserted: preparedTransactions?.inserted.length ?? 3,
          transactionIds,
        },
      ),
    );
    return {
      ...preview,
      importBatchId,
      rowCountInserted: preparedTransactions?.inserted.length ?? 3,
      rowCountDuplicates: preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates,
      transactionIds,
      jobsQueued: [...jobsQueued],
    };
  }

  async runPendingJobs(apply: boolean): Promise<JobRunResult> {
    const queued = this.dataset.jobs.filter((job) => job.status === "queued");
    if (apply) {
      for (const job of queued) {
        job.status = "running";
        job.startedAt = new Date().toISOString();
        job.attempts += 1;
        try {
          if (job.jobType === "rule_parse") {
            await processRuleDraftJob(this.dataset, job);
          } else {
            job.status = "completed";
            job.finishedAt = new Date().toISOString();
          }
        } catch (error) {
          job.status = "failed";
          job.finishedAt = new Date().toISOString();
          job.lastError = error instanceof Error ? error.message : "Unknown job failure";
        }
      }
    }
    return {
      schemaVersion: "v1",
      applied: apply,
      processedJobs: queued.map((job) => ({
        id: job.id,
        jobType: job.jobType,
        status: apply ? job.status : job.status,
      })),
      generatedAt: new Date().toISOString(),
    };
  }
}

let defaultRepository: InMemoryFinanceRepository | null = null;

export function createFixtureRepository(): FinanceRepository {
  if (!defaultRepository) {
    defaultRepository = new InMemoryFinanceRepository();
  }
  return defaultRepository;
}

export function getLatestBalanceSnapshots(
  snapshots: AccountBalanceSnapshot[],
): AccountBalanceSnapshot[] {
  const byAccount = new Map<string, AccountBalanceSnapshot>();
  for (const snapshot of snapshots) {
    const current = byAccount.get(snapshot.accountId);
    if (!current || current.asOfDate < snapshot.asOfDate) {
      byAccount.set(snapshot.accountId, snapshot);
    }
  }
  return [...byAccount.values()];
}

export function getAccountById(accounts: Account[], accountId: string): Account | undefined {
  return accounts.find((account) => account.id === accountId);
}

import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import {
  buildImportedTransactions,
  normalizeImportExecutionInput,
  runDeterministicImport,
  sanitizeImportResult,
  type DomainDataset,
  type ImportCommitResult,
  type ImportPreviewResult,
  type Transaction,
} from "@myfinance/domain";

import { queueJob } from "./job-state";
import { serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";

const DEFAULT_IMPORT_JOBS_QUEUED = [
  "classification",
  "transfer_rematch",
  "position_rebuild",
  "metric_refresh",
] as const satisfies ImportCommitResult["jobsQueued"];

export type CommitPreparedImportBatchOptions = {
  importBatchId?: string;
  importedByActor?: string;
  jobsQueued?: ImportCommitResult["jobsQueued"];
  importBatchExtraValues?: Record<string, unknown>;
};

export type CommitPreparedImportBatchResult = {
  preview: ImportCommitResult;
  importBatchId: string;
  jobsQueued: ImportCommitResult["jobsQueued"];
  insertedTransactions: Transaction[];
};

export type SyntheticImportBatchCommitResult = {
  importBatchId: string;
  insertedTransactions: Transaction[];
};

function isUniqueViolation(error: unknown): error is { code: string } {
  return (
    error !== null &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}

async function insertTransactions(
  sql: SqlClient,
  transactions: readonly Transaction[],
) {
  const insertedTransactions: Transaction[] = [];

  for (const transaction of transactions) {
    try {
      const inserted = await sql`
        insert into public.transactions (
          id,
          user_id,
          account_id,
          account_entity_id,
          economic_entity_id,
          import_batch_id,
          provider_name,
          provider_record_id,
          source_fingerprint,
          duplicate_key,
          transaction_date,
          posted_date,
          amount_original,
          currency_original,
          amount_base_eur,
          fx_rate_to_eur,
          description_raw,
          description_clean,
          merchant_normalized,
          counterparty_name,
          transaction_class,
          category_code,
          subcategory_code,
          transfer_group_id,
          related_account_id,
          related_transaction_id,
          transfer_match_status,
          cross_entity_flag,
          reimbursement_status,
          classification_status,
          classification_source,
          classification_confidence,
          needs_review,
          review_reason,
          exclude_from_analytics,
          correction_of_transaction_id,
          voided_at,
          manual_notes,
          llm_payload,
          raw_payload,
          security_id,
          quantity,
          unit_price_original,
          credit_card_statement_status,
          linked_credit_card_account_id,
          created_at,
          updated_at
        ) values (
          ${transaction.id},
          ${transaction.userId},
          ${transaction.accountId},
          ${transaction.accountEntityId},
          ${transaction.economicEntityId},
          ${transaction.importBatchId ?? null},
          ${transaction.providerName ?? null},
          ${transaction.providerRecordId ?? null},
          ${transaction.sourceFingerprint},
          ${transaction.duplicateKey ?? null},
          ${transaction.transactionDate},
          ${transaction.postedDate ?? null},
          ${transaction.amountOriginal},
          ${transaction.currencyOriginal},
          ${transaction.amountBaseEur},
          ${transaction.fxRateToEur ?? null},
          ${transaction.descriptionRaw},
          ${transaction.descriptionClean},
          ${transaction.merchantNormalized ?? null},
          ${transaction.counterpartyName ?? null},
          ${transaction.transactionClass},
          ${transaction.categoryCode ?? null},
          ${transaction.subcategoryCode ?? null},
          ${transaction.transferGroupId ?? null},
          ${transaction.relatedAccountId ?? null},
          ${transaction.relatedTransactionId ?? null},
          ${transaction.transferMatchStatus},
          ${transaction.crossEntityFlag},
          ${transaction.reimbursementStatus},
          ${transaction.classificationStatus},
          ${transaction.classificationSource},
          ${transaction.classificationConfidence},
          ${transaction.needsReview},
          ${transaction.reviewReason ?? null},
          ${transaction.excludeFromAnalytics},
          ${transaction.correctionOfTransactionId ?? null},
          ${transaction.voidedAt ?? null},
          ${transaction.manualNotes ?? null},
          ${serializeJson(sql, transaction.llmPayload)}::jsonb,
          ${serializeJson(sql, transaction.rawPayload)}::jsonb,
          ${transaction.securityId ?? null},
          ${transaction.quantity ?? null},
          ${transaction.unitPriceOriginal ?? null},
          ${transaction.creditCardStatementStatus},
          ${transaction.linkedCreditCardAccountId ?? null},
          ${transaction.createdAt},
          ${transaction.updatedAt}
        )
        returning id
      `;
      if (inserted.length > 0) {
        insertedTransactions.push(transaction);
      }
    } catch (error) {
      if (isUniqueViolation(error)) {
        continue;
      }
      throw error;
    }
  }

  return insertedTransactions;
}

async function queueImportBatchJobs(
  sql: SqlClient,
  input: {
    jobsQueued: ImportCommitResult["jobsQueued"];
    importBatchId: string;
    accountId: string;
  },
) {
  const availableAt = new Date().toISOString();
  for (const jobType of input.jobsQueued) {
    await queueJob(
      sql,
      jobType,
      {
        importBatchId: input.importBatchId,
        accountId: input.accountId,
      },
      {
        availableAt,
      },
    );
  }
}

async function touchAccountLastImportedAt(
  sql: SqlClient,
  input: {
    accountId: string;
    userId: string;
    importedAt: string;
  },
) {
  await sql`
    update public.accounts
    set last_imported_at = ${input.importedAt}
    where id = ${input.accountId}
      and user_id = ${input.userId}
  `;
}

async function insertImportBatchRecord(
  sql: SqlClient,
  input: {
    importBatchId: string;
    userId: string;
    accountId: string;
    templateId: string | null;
    sourceKind: "upload" | "bank_sync";
    providerName: string | null;
    bankConnectionId: string | null;
    storagePath: string;
    originalFilename: string;
    rowCountDetected: number;
    rowCountParsed: number;
    rowCountInserted: number;
    rowCountDuplicates: number;
    rowCountFailed: number;
    previewSummary: Record<string, unknown>;
    commitSummary: Record<string, unknown>;
    importedByActor: string;
    importedAt: string;
    extraValues?: Record<string, unknown>;
  },
) {
  await sql`
    insert into public.import_batches ${sql({
      id: input.importBatchId,
      user_id: input.userId,
      account_id: input.accountId,
      template_id: input.templateId,
      source_kind: input.sourceKind,
      provider_name: input.providerName,
      bank_connection_id: input.bankConnectionId,
      storage_path: input.storagePath,
      original_filename: input.originalFilename,
      file_sha256: randomUUID().replace(/-/g, ""),
      status: "committed",
      row_count_detected: input.rowCountDetected,
      row_count_parsed: input.rowCountParsed,
      row_count_inserted: input.rowCountInserted,
      row_count_duplicates: input.rowCountDuplicates,
      row_count_failed: input.rowCountFailed,
      preview_summary_json: serializeJson(sql, input.previewSummary),
      commit_summary_json: serializeJson(sql, input.commitSummary),
      imported_by_actor: input.importedByActor,
      imported_at: input.importedAt,
      ...(input.extraValues ?? {}),
    } as Record<string, unknown>)}
  `;
}

async function finalizeImportBatchRecord(
  sql: SqlClient,
  input: {
    importBatchId: string;
    userId: string;
    rowCountInserted: number;
    rowCountDuplicates: number;
    commitSummary: Record<string, unknown>;
  },
) {
  await sql`
    update public.import_batches
    set row_count_inserted = ${input.rowCountInserted},
        row_count_duplicates = ${input.rowCountDuplicates},
        commit_summary_json = ${serializeJson(sql, input.commitSummary)}::jsonb
    where id = ${input.importBatchId}
      and user_id = ${input.userId}
  `;
}

async function persistCommittedImportBatch(
  sql: SqlClient,
  input: {
    importBatchRecord: Parameters<typeof insertImportBatchRecord>[1];
    userId: string;
    accountId: string;
    importedAt: string;
    jobsQueued: ImportCommitResult["jobsQueued"];
    preparedTransactions: Transaction[];
    queueBeforeInsert?: boolean;
  },
) {
  await insertImportBatchRecord(sql, input.importBatchRecord);

  let insertedTransactions: Transaction[] = [];
  if (input.queueBeforeInsert) {
    await queueImportBatchJobs(sql, {
      jobsQueued: input.jobsQueued,
      importBatchId: input.importBatchRecord.importBatchId,
      accountId: input.accountId,
    });
    insertedTransactions = await insertTransactions(
      sql,
      input.preparedTransactions,
    );
  } else {
    insertedTransactions = await insertTransactions(
      sql,
      input.preparedTransactions,
    );
    await queueImportBatchJobs(sql, {
      jobsQueued: input.jobsQueued,
      importBatchId: input.importBatchRecord.importBatchId,
      accountId: input.accountId,
    });
  }

  await touchAccountLastImportedAt(sql, {
    accountId: input.accountId,
    userId: input.userId,
    importedAt: input.importedAt,
  });

  return {
    insertedTransactions,
    transactionIds: insertedTransactions.map((transaction) => transaction.id),
  };
}

export function sumPreparedTransactionAmountBaseEur(
  transactions: Transaction[],
) {
  return transactions
    .reduce(
      (sum, transaction) => sum.plus(transaction.amountBaseEur),
      new Decimal(0),
    )
    .toFixed(2);
}

export async function commitPreparedImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    normalizedInput: ReturnType<typeof normalizeImportExecutionInput>;
    previewFallback?: () => Promise<ImportPreviewResult>;
    options?: CommitPreparedImportBatchOptions;
  },
): Promise<CommitPreparedImportBatchResult> {
  const commitResult = input.normalizedInput.filePath
    ? await runDeterministicImport(
        "commit",
        input.normalizedInput,
        input.dataset,
      )
    : null;
  const importBatchId = input.options?.importBatchId ?? randomUUID();
  const preparedTransactions =
    commitResult && input.normalizedInput.filePath
      ? buildImportedTransactions(
          input.dataset,
          input.normalizedInput,
          importBatchId,
          commitResult.normalizedRows ?? [],
        )
      : null;
  const preview =
    commitResult && input.normalizedInput.filePath
      ? ({
          ...(sanitizeImportResult(commitResult) as ImportCommitResult),
          rowCountDuplicates: preparedTransactions?.duplicateCount ?? 0,
        } satisfies ImportCommitResult)
      : ({
          ...((input.previewFallback
            ? await input.previewFallback()
            : await (async () => {
                throw new Error(
                  "A file path is required to commit this import flow.",
                );
              })()) as ImportCommitResult),
          importBatchId,
          rowCountInserted: 0,
          transactionIds: [],
          jobsQueued: [...DEFAULT_IMPORT_JOBS_QUEUED],
        } satisfies ImportCommitResult);
  const jobsQueued = input.options?.jobsQueued ??
    ((commitResult as ImportCommitResult | null)?.jobsQueued as
      | ImportCommitResult["jobsQueued"]
      | undefined) ?? [...DEFAULT_IMPORT_JOBS_QUEUED];
  const importedAt = new Date().toISOString();

  const { insertedTransactions, transactionIds } =
    await persistCommittedImportBatch(sql, {
      importBatchRecord: {
        importBatchId,
        userId: input.userId,
        accountId: input.normalizedInput.accountId,
        templateId: input.normalizedInput.templateId,
        sourceKind: "upload",
        providerName: null,
        bankConnectionId: null,
        storagePath: input.normalizedInput.filePath
          ? `private-imports/local/${input.normalizedInput.originalFilename}`
          : `private-imports/manual/${input.normalizedInput.originalFilename}`,
        originalFilename: input.normalizedInput.originalFilename,
        rowCountDetected: preview.rowCountDetected,
        rowCountParsed: preview.rowCountParsed,
        rowCountInserted:
          preparedTransactions?.inserted.length ?? preview.rowCountParsed,
        rowCountDuplicates:
          preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates,
        rowCountFailed: preview.rowCountFailed,
        previewSummary: {
          sampleRows: preview.sampleRows,
          parseErrors: preview.parseErrors,
          dateRange: preview.dateRange,
        },
        commitSummary: { jobsQueued },
        importedByActor: input.options?.importedByActor ?? "web-cli",
        importedAt,
        extraValues: input.options?.importBatchExtraValues,
      },
      userId: input.userId,
      accountId: input.normalizedInput.accountId,
      importedAt,
      jobsQueued,
      preparedTransactions: preparedTransactions?.inserted ?? [],
      queueBeforeInsert: true,
    });

  const rowCountInserted =
    insertedTransactions.length ||
    (preparedTransactions ? 0 : preview.rowCountParsed);
  const rowCountDuplicates =
    preparedTransactions === null
      ? preview.rowCountDuplicates
      : (preparedTransactions.duplicateCount ?? 0) +
        (preparedTransactions.inserted.length - insertedTransactions.length);

  await finalizeImportBatchRecord(sql, {
    importBatchId,
    userId: input.userId,
    rowCountInserted,
    rowCountDuplicates,
    commitSummary: {
      jobsQueued,
      transactionIds,
    },
  });

  return {
    preview: {
      ...preview,
      importBatchId,
      rowCountInserted,
      rowCountDuplicates,
      transactionIds,
      jobsQueued: [...jobsQueued],
    },
    importBatchId,
    jobsQueued: [...jobsQueued],
    insertedTransactions,
  };
}

export async function commitSyntheticImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    accountId: string;
    originalFilename: string;
    sourceKind: "bank_sync";
    providerName: string;
    bankConnectionId: string;
    preparedTransactions: Transaction[];
    importedByActor: string;
    jobsQueued?: ImportCommitResult["jobsQueued"];
    dateRange?: { start: string; end: string } | null;
  },
): Promise<SyntheticImportBatchCommitResult> {
  const importBatchId = randomUUID();
  const jobsQueued =
    input.jobsQueued ??
    (input.preparedTransactions.length > 0
      ? [...DEFAULT_IMPORT_JOBS_QUEUED]
      : (["metric_refresh"] satisfies ImportCommitResult["jobsQueued"]));
  const importedAt = new Date().toISOString();

  const preparedTransactions = input.preparedTransactions.map(
    (transaction) => ({
      ...transaction,
      importBatchId,
    }),
  );
  const { insertedTransactions, transactionIds } =
    await persistCommittedImportBatch(sql, {
      importBatchRecord: {
        importBatchId,
        userId: input.userId,
        accountId: input.accountId,
        templateId: null,
        sourceKind: input.sourceKind,
        providerName: input.providerName,
        bankConnectionId: input.bankConnectionId,
        storagePath: `bank-sync/${input.providerName}/${input.bankConnectionId}/${input.originalFilename}`,
        originalFilename: input.originalFilename,
        rowCountDetected: input.preparedTransactions.length,
        rowCountParsed: input.preparedTransactions.length,
        rowCountInserted: input.preparedTransactions.length,
        rowCountDuplicates: 0,
        rowCountFailed: 0,
        previewSummary: {
          dateRange: input.dateRange ?? null,
          sampleRows: input.preparedTransactions
            .slice(0, 3)
            .map((transaction) => ({
              providerRecordId: transaction.providerRecordId,
              transactionDate: transaction.transactionDate,
              amountOriginal: transaction.amountOriginal,
              currencyOriginal: transaction.currencyOriginal,
              descriptionRaw: transaction.descriptionRaw,
            })),
        },
        commitSummary: {
          jobsQueued,
          sourceKind: input.sourceKind,
          providerName: input.providerName,
        },
        importedByActor: input.importedByActor,
        importedAt,
      },
      userId: input.userId,
      accountId: input.accountId,
      importedAt,
      jobsQueued,
      preparedTransactions,
    });

  await finalizeImportBatchRecord(sql, {
    importBatchId,
    userId: input.userId,
    rowCountInserted: insertedTransactions.length,
    rowCountDuplicates: Math.max(
      0,
      input.preparedTransactions.length - insertedTransactions.length,
    ),
    commitSummary: {
      jobsQueued,
      sourceKind: input.sourceKind,
      providerName: input.providerName,
      transactionIds,
    },
  });

  return {
    importBatchId,
    insertedTransactions,
  };
}

import {
  getBatchEscalationReviewModel,
  getInvestmentTransactionClassifierConfig,
  getTransactionClassifierConfig,
  type SimilarAccountTransactionPromptContext,
  type TransactionBatchContext,
} from "@myfinance/classification";
import {
  getTransactionAnalysisStatus,
  type Account,
  type Transaction,
} from "@myfinance/domain";
import { isModelConfigured, type PromptProfileOverrides } from "@myfinance/llm";

import {
  buildResolvedReviewSimilarTransactionContext,
  buildResolvedSourcePrecedent,
  replaceTransactionInDataset,
} from "./review-propagation-support";
import {
  queueTransactionSearchIndexJob,
  syncTransactionSearchIndex,
} from "./transaction-search-index";
import { executeTransactionEnrichmentPipeline } from "./transaction-enrichment";
import { loadDatasetForUser } from "./dataset-loader";
import { queueJob, supportsJobType } from "./job-state";
import { mapFromSql, readOptionalRecord } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { updateTransactionRecord } from "./transaction-record";

const MAX_FIRST_PASS_CONCURRENCY = 200;
const DEFAULT_FIRST_PASS_CONCURRENCY_CAP = MAX_FIRST_PASS_CONCURRENCY;
const DEFAULT_TRUSTED_RESOLUTION_CONFIDENCE = 0.85;
const DEFAULT_ESCALATION_SIMILARITY_THRESHOLD = 0.8;
const DEFAULT_ESCALATION_CONTEXT_LIMIT = 5;
const SEARCH_EMBEDDING_CANDIDATE_MULTIPLIER = 4;

type BatchSearchContext = {
  transactionId: string;
  sourceBatchKey: string;
  batchSummary: string;
  contextualizedText: string;
};

type BatchClassificationProgress = {
  importBatchId: string;
  phase:
    | "search_index_bootstrap"
    | "parallel_first_pass"
    | "sequential_escalation"
    | "final_search_refresh";
  totalTransactions: number;
  firstPassPendingCount: number;
  firstPassProcessed: number;
  firstPassFailed: number;
  sequentialCandidateCount: number;
  sequentialProcessed: number;
  sequentialFailed: number;
  trustedResolvedCount: number;
  remainingUnresolvedCount: number;
  lastTransactionId: string | null;
  searchBootstrapStatus?: "completed" | "failed";
  searchBootstrapError?: string | null;
  finalSearchRefreshStatus?:
    | "completed"
    | "queued_background"
    | "queued_retry"
    | "failed";
  finalSearchRefreshError?: string | null;
  updatedAt: string;
};

export function getBatchClassificationFirstPassConcurrency(
  transactionCount: number,
) {
  const normalizedCount = Number.isFinite(transactionCount)
    ? Math.max(1, Math.floor(transactionCount))
    : 1;
  const configuredCap = Number(
    process.env.BATCH_TRANSACTION_CLASSIFICATION_CONCURRENCY ??
      `${DEFAULT_FIRST_PASS_CONCURRENCY_CAP}`,
  );
  const normalizedCap =
    Number.isFinite(configuredCap) && configuredCap > 0
      ? Math.max(1, Math.floor(configuredCap))
      : DEFAULT_FIRST_PASS_CONCURRENCY_CAP;
  return Math.min(
    normalizedCount,
    normalizedCap,
    MAX_FIRST_PASS_CONCURRENCY,
  );
}

export function getTrustedBatchResolutionConfidence() {
  const parsed = Number(
    process.env.BATCH_TRANSACTION_TRUSTED_CONFIDENCE ??
      `${DEFAULT_TRUSTED_RESOLUTION_CONFIDENCE}`,
  );
  return Number.isFinite(parsed) && parsed > 0
    ? Math.min(1, parsed)
    : DEFAULT_TRUSTED_RESOLUTION_CONFIDENCE;
}

function getEscalationSimilarityThreshold() {
  const parsed = Number(
    process.env.BATCH_TRANSACTION_ESCALATION_SIMILARITY_THRESHOLD ??
      `${DEFAULT_ESCALATION_SIMILARITY_THRESHOLD}`,
  );
  return Number.isFinite(parsed) && parsed > 0
    ? Math.min(0.99, parsed)
    : DEFAULT_ESCALATION_SIMILARITY_THRESHOLD;
}

function getEscalationContextLimit() {
  const parsed = Number(
    process.env.BATCH_TRANSACTION_ESCALATION_CONTEXT_LIMIT ??
      `${DEFAULT_ESCALATION_CONTEXT_LIMIT}`,
  );
  return Number.isFinite(parsed) && parsed > 0
    ? Math.max(1, Math.floor(parsed))
    : DEFAULT_ESCALATION_CONTEXT_LIMIT;
}

function getResolvedConfidence(transaction: Pick<Transaction, "classificationConfidence">) {
  const parsed = Number(transaction.classificationConfidence ?? "0");
  return Number.isFinite(parsed) ? parsed : 0;
}

export function getBatchClassificationPhase(transaction: {
  llmPayload?: unknown;
}) {
  const batchPipeline = readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.batchPipeline,
  );
  const phase = batchPipeline?.phase;
  return phase === "parallel_first_pass" || phase === "sequential_escalation"
    ? phase
    : null;
}

export function isTrustedBatchResolution(
  transaction: Pick<
    Transaction,
    "needsReview" | "classificationSource" | "classificationConfidence"
  >,
) {
  if (transaction.needsReview) {
    return false;
  }

  if (transaction.classificationSource !== "llm") {
    return true;
  }

  return (
    getResolvedConfidence(transaction) >= getTrustedBatchResolutionConfidence()
  );
}

export function shouldEscalateBatchTransaction(
  transaction: Pick<Transaction, "needsReview" | "llmPayload">,
) {
  return (
    transaction.needsReview === true &&
    getBatchClassificationPhase(transaction) !== "sequential_escalation"
  );
}

function isPendingBatchClassification(transaction: Pick<Transaction, "llmPayload">) {
  const status = getTransactionAnalysisStatus(transaction);
  return status === null || status === "pending";
}

function compareTransactionsByDate(left: Transaction, right: Transaction) {
  if (left.transactionDate !== right.transactionDate) {
    return left.transactionDate.localeCompare(right.transactionDate);
  }
  return left.createdAt.localeCompare(right.createdAt);
}

function countRemainingUnresolved(
  transactions: readonly Transaction[],
  importBatchId: string,
) {
  return transactions.filter(
    (transaction) =>
      transaction.importBatchId === importBatchId &&
      !transaction.voidedAt &&
      transaction.needsReview,
  ).length;
}

function getBatchContext(
  contextByTransactionId: Map<string, BatchSearchContext>,
  transactionId: string,
  phase: TransactionBatchContext["phase"],
  totalTransactions: number,
  trustedResolvedCount: number,
): TransactionBatchContext {
  const context = contextByTransactionId.get(transactionId);
  return {
    phase,
    sourceBatchKey: context?.sourceBatchKey ?? null,
    batchSummary: context?.batchSummary ?? null,
    retrievalContext: context?.contextualizedText ?? null,
    totalTransactions,
    trustedResolvedCount,
  };
}

async function loadBatchSearchContext(
  sql: SqlClient,
  userId: string,
  importBatchId: string,
) {
  const rows = await sql`
    select
      r.transaction_id,
      r.contextualized_text,
      b.source_batch_key,
      b.batch_summary
    from public.transaction_search_rows as r
    join public.transaction_search_batches as b
      on b.id = r.batch_id
    join public.transactions as t
      on t.id = r.transaction_id
    where r.user_id = ${userId}
      and t.import_batch_id = ${importBatchId}
  `;

  return new Map(
    rows.flatMap((row) => {
      const transactionId =
        typeof row.transaction_id === "string" ? row.transaction_id : "";
      if (!transactionId) {
        return [];
      }

      return [
        [
          transactionId,
          {
            transactionId,
            sourceBatchKey:
              typeof row.source_batch_key === "string"
                ? row.source_batch_key
                : `import_batch:${importBatchId}`,
            batchSummary:
              typeof row.batch_summary === "string" ? row.batch_summary : "",
            contextualizedText:
              typeof row.contextualized_text === "string"
                ? row.contextualized_text
                : "",
          } satisfies BatchSearchContext,
        ] as const,
      ];
    }),
  );
}

async function markTransactionClassificationFailure(
  sql: SqlClient,
  userId: string,
  input: {
    transaction: Transaction;
    account: Account;
    error: unknown;
    phase: TransactionBatchContext["phase"];
    batchContext: TransactionBatchContext;
    modelNameOverride?: string | null;
  },
) {
  const message =
    input.error instanceof Error
      ? input.error.message
      : "Transaction enrichment failed.";
  const llmPayload =
    input.transaction.llmPayload &&
    typeof input.transaction.llmPayload === "object" &&
    !Array.isArray(input.transaction.llmPayload)
      ? { ...(input.transaction.llmPayload as Record<string, unknown>) }
      : {};

  const nextLlmPayload = {
    ...llmPayload,
    analysisStatus: "failed",
    explanation: null,
    model:
      input.modelNameOverride?.trim() ||
      (input.account.assetDomain === "investment"
        ? getInvestmentTransactionClassifierConfig().model
        : getTransactionClassifierConfig().model),
    error: message,
    batchPipeline: {
      phase: input.phase,
      sourceBatchKey: input.batchContext.sourceBatchKey ?? null,
      totalTransactions: input.batchContext.totalTransactions ?? null,
      trustedResolvedCount: input.batchContext.trustedResolvedCount ?? null,
      processedAt: new Date().toISOString(),
    },
    analyzedAt: new Date().toISOString(),
  };

  const after = await updateTransactionRecord(sql, {
    userId,
    transactionId: input.transaction.id,
    updatePayload: {
      needs_review: true,
      review_reason: message,
      updated_at: new Date().toISOString(),
    },
    llmPayload: nextLlmPayload,
  });

  return after ? (after as Record<string, unknown>) : null;
}

async function mapWithConcurrency<TItem, TResult>(
  items: TItem[],
  concurrency: number,
  mapper: (item: TItem, index: number) => Promise<TResult>,
) {
  const results: TResult[] = new Array(items.length);
  let cursor = 0;

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (cursor < items.length) {
        const current = cursor;
        cursor += 1;
        results[current] = await mapper(items[current], current);
      }
    }),
  );

  return results;
}

async function findResolvedEscalationMatches(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransactionId: string;
    accountId: string;
  },
) {
  const limit = getEscalationContextLimit();
  const threshold = getEscalationSimilarityThreshold();
  const candidateLimit = Math.max(
    limit,
    limit * SEARCH_EMBEDDING_CANDIDATE_MULTIPLIER,
  );

  const rows = await sql`
    with source as (
      select embedding
      from public.transaction_search_rows
      where user_id = ${input.userId}
        and transaction_id = ${input.sourceTransactionId}
        and embedding_status = 'ready'
      limit 1
    ),
    approximate_candidates as (
      select
        r.transaction_id,
        r.embedding
      from public.transaction_search_rows as r
      join public.transactions as t
        on t.id = r.transaction_id
      cross join source
      where r.user_id = ${input.userId}
        and r.account_id = ${input.accountId}
        and r.transaction_id <> ${input.sourceTransactionId}
        and r.embedding_status = 'ready'
        and coalesce(t.needs_review, false) = false
        and t.voided_at is null
      order by
        r.embedding::halfvec(3072) <=>
        source.embedding::halfvec(3072) asc
      limit ${candidateLimit}
    )
    select
      candidate.transaction_id,
      1 - (candidate.embedding <=> source.embedding) as similarity
    from approximate_candidates as candidate
    cross join source
    where 1 - (candidate.embedding <=> source.embedding) >= ${threshold}
    order by candidate.embedding <=> source.embedding asc
    limit ${limit}
  `;

  return rows.flatMap((row) => {
    const transactionId =
      typeof row.transaction_id === "string" ? row.transaction_id : "";
    const similarity = Number(row.similarity ?? 0);
    if (!transactionId || !Number.isFinite(similarity)) {
      return [];
    }

    return [{ transactionId, similarity }];
  });
}

type ProcessClassificationJobInput = {
  importBatchId: string;
  payloadJson: Record<string, unknown>;
  promptOverrides: PromptProfileOverrides;
  onProgress?: (payloadJson: Record<string, unknown>) => Promise<void>;
};

export async function processClassificationJob(
  sql: SqlClient,
  userId: string,
  input: ProcessClassificationJobInput,
) {
  let dataset = await loadDatasetForUser(sql, userId);
  const batchTransactions = dataset.transactions
    .filter(
      (transaction) =>
        transaction.importBatchId === input.importBatchId && !transaction.voidedAt,
    )
    .sort(compareTransactionsByDate);

  const totalTransactions = batchTransactions.length;
  const pendingTransactions = batchTransactions.filter(isPendingBatchClassification);
  const investmentAccountIds = new Set(
    batchTransactions
      .flatMap((transaction) => {
        const account = dataset.accounts.find(
          (candidate) => candidate.id === transaction.accountId,
        );
        return account?.assetDomain === "investment" ? [account.id] : [];
      }),
  );

  let searchBootstrapStatus: BatchClassificationProgress["searchBootstrapStatus"];
  let searchBootstrapError: string | null = null;
  try {
    await syncTransactionSearchIndex(sql, userId, {
      importBatchIds: [input.importBatchId],
      onlyStaleOrMissing: true,
    });
    searchBootstrapStatus = "completed";
  } catch (error) {
    searchBootstrapStatus = "failed";
    searchBootstrapError =
      error instanceof Error ? error.message : "Transaction search bootstrap failed.";
    if (await supportsJobType(sql, "transaction_search_index")) {
      await queueTransactionSearchIndexJob(sql, {
        userId,
        importBatchIds: [input.importBatchId],
        trigger: "classification_bootstrap_retry",
      });
    }
  }

  const searchContextByTransactionId = await loadBatchSearchContext(
    sql,
    userId,
    input.importBatchId,
  );

  let progress: BatchClassificationProgress = {
    importBatchId: input.importBatchId,
    phase: "parallel_first_pass",
    totalTransactions,
    firstPassPendingCount: pendingTransactions.length,
    firstPassProcessed: 0,
    firstPassFailed: 0,
    sequentialCandidateCount: 0,
    sequentialProcessed: 0,
    sequentialFailed: 0,
    trustedResolvedCount: batchTransactions.filter(isTrustedBatchResolution)
      .length,
    remainingUnresolvedCount: countRemainingUnresolved(
      batchTransactions,
      input.importBatchId,
    ),
    lastTransactionId: null,
    searchBootstrapStatus,
    searchBootstrapError,
    updatedAt: new Date().toISOString(),
  };
  let firstPassProcessedCount = progress.firstPassProcessed;
  let firstPassFailedCount = progress.firstPassFailed;
  let sequentialProcessedCount = progress.sequentialProcessed;
  let sequentialFailedCount = progress.sequentialFailed;
  let trustedResolvedCount = progress.trustedResolvedCount;
  let remainingUnresolvedCount = progress.remainingUnresolvedCount;

  const reportProgress = async (
    patch: Partial<BatchClassificationProgress>,
  ) => {
    progress = {
      ...progress,
      ...patch,
      updatedAt: new Date().toISOString(),
    };
    await input.onProgress?.({
      ...input.payloadJson,
      progress,
    });
  };

  await reportProgress({
    phase: "parallel_first_pass",
  });

  const firstPassConcurrency = getBatchClassificationFirstPassConcurrency(
    pendingTransactions.length,
  );
  const firstPassResults = await mapWithConcurrency(
    pendingTransactions,
    firstPassConcurrency,
    async (transaction) => {
      const account = dataset.accounts.find(
        (candidate) => candidate.id === transaction.accountId,
      );
      if (!account) {
        throw new Error(
          `Account ${transaction.accountId} not found for classification.`,
        );
      }

        const batchContext = getBatchContext(
          searchContextByTransactionId,
          transaction.id,
          "parallel_first_pass",
          totalTransactions,
          trustedResolvedCount,
        );

      try {
        const { afterTransaction } = await executeTransactionEnrichmentPipeline(
          sql,
          userId,
          {
            dataset,
            account,
            transaction,
            enrichmentOptions: {
              trigger: "import_classification",
              promptOverrides: input.promptOverrides,
              batchContext,
              skipHistoricalReviewExamples: true,
              skipSimilarAccountTransactions: true,
              allowDeterministicLlmSkip: true,
            },
          },
        );

        firstPassProcessedCount += 1;
        if (isTrustedBatchResolution(afterTransaction)) {
          trustedResolvedCount += 1;
        }
        remainingUnresolvedCount = Math.max(
          0,
          remainingUnresolvedCount +
            (afterTransaction.needsReview ? 1 : 0) -
            (transaction.needsReview ? 1 : 0),
        );
        await reportProgress({
          firstPassProcessed: firstPassProcessedCount,
          trustedResolvedCount,
          remainingUnresolvedCount,
          lastTransactionId: transaction.id,
        });

        return { afterTransaction };
      } catch (error) {
        const failedRow = await markTransactionClassificationFailure(sql, userId, {
          transaction,
          account,
          error,
          phase: "parallel_first_pass",
          batchContext,
        });
        const failedTransaction = failedRow
          ? mapFromSql<Transaction>(failedRow)
          : transaction;

        firstPassProcessedCount += 1;
        firstPassFailedCount += 1;
        remainingUnresolvedCount = Math.max(
          0,
          remainingUnresolvedCount +
            (failedTransaction.needsReview ? 1 : 0) -
            (transaction.needsReview ? 1 : 0),
        );
        await reportProgress({
          firstPassProcessed: firstPassProcessedCount,
          firstPassFailed: firstPassFailedCount,
          remainingUnresolvedCount,
          lastTransactionId: transaction.id,
        });

        return { afterTransaction: failedTransaction };
      }
    },
  );

  for (const result of firstPassResults) {
    dataset = replaceTransactionInDataset(dataset, result.afterTransaction);
  }

  const postFirstPassBatchTransactions = dataset.transactions
    .filter(
      (transaction) =>
        transaction.importBatchId === input.importBatchId && !transaction.voidedAt,
    )
    .sort(compareTransactionsByDate);
  const escalationCandidates = postFirstPassBatchTransactions.filter(
    shouldEscalateBatchTransaction,
  );
  const escalationModel = getBatchEscalationReviewModel();
  const canEscalate = isModelConfigured(escalationModel);
  trustedResolvedCount = postFirstPassBatchTransactions.filter(
    isTrustedBatchResolution,
  ).length;
  remainingUnresolvedCount = countRemainingUnresolved(
    postFirstPassBatchTransactions,
    input.importBatchId,
  );

  await reportProgress({
    phase: "sequential_escalation",
    sequentialCandidateCount: escalationCandidates.length,
    trustedResolvedCount,
    remainingUnresolvedCount,
  });

  if (canEscalate) {
    for (const transaction of escalationCandidates) {
      const account = dataset.accounts.find(
        (candidate) => candidate.id === transaction.accountId,
      );
      if (!account) {
        throw new Error(
          `Account ${transaction.accountId} not found for escalated classification.`,
        );
      }

      const batchContext = getBatchContext(
        searchContextByTransactionId,
        transaction.id,
        "sequential_escalation",
        totalTransactions,
        trustedResolvedCount,
      );

      try {
        let similarResolvedTransactions: SimilarAccountTransactionPromptContext[] =
          [];
        let resolvedSourcePrecedent: Record<string, unknown> | null = null;

        const matches = await findResolvedEscalationMatches(sql, {
          userId,
          sourceTransactionId: transaction.id,
          accountId: transaction.accountId,
        });
        const trustedMatches = matches
          .map((match) => ({
            match,
            transaction:
              dataset.transactions.find(
                (candidate) => candidate.id === match.transactionId,
              ) ?? null,
          }))
          .filter(
            (
              entry,
            ): entry is {
              match: { transactionId: string; similarity: number };
              transaction: Transaction;
            } => {
              const candidateTransaction = entry.transaction;
              return (
                candidateTransaction !== null &&
                isTrustedBatchResolution(candidateTransaction)
              );
            },
          );

        similarResolvedTransactions = trustedMatches.map((entry) =>
          buildResolvedReviewSimilarTransactionContext(
            entry.transaction,
            entry.match.similarity,
          ),
        );

        if (trustedMatches[0]?.transaction) {
          resolvedSourcePrecedent = buildResolvedSourcePrecedent(
            trustedMatches[0].transaction,
            null,
          ) as unknown as Record<string, unknown>;
        }
        const { afterTransaction } =
          await executeTransactionEnrichmentPipeline(sql, userId, {
            dataset,
            account,
            transaction,
            enrichmentOptions: {
              trigger: "review_propagation",
              promptOverrides: input.promptOverrides,
              batchContext,
              modelNameOverride: escalationModel,
              similarAccountTransactions: similarResolvedTransactions,
              reviewContext: {
                previousReviewReason: transaction.reviewReason ?? null,
                previousUserContext: transaction.manualNotes ?? null,
                previousLlmPayload:
                  transaction.llmPayload &&
                  typeof transaction.llmPayload === "object" &&
                  !Array.isArray(transaction.llmPayload)
                    ? (transaction.llmPayload as Record<string, unknown>)
                    : null,
                resolvedSourcePrecedent,
              },
            },
          });

        dataset = replaceTransactionInDataset(dataset, afterTransaction);
        sequentialProcessedCount += 1;
        if (isTrustedBatchResolution(afterTransaction)) {
          trustedResolvedCount += 1;
        }
        remainingUnresolvedCount = countRemainingUnresolved(
          dataset.transactions,
          input.importBatchId,
        );
        await reportProgress({
          sequentialProcessed: sequentialProcessedCount,
          trustedResolvedCount,
          remainingUnresolvedCount,
          lastTransactionId: transaction.id,
        });
      } catch (error) {
        const failedRow = await markTransactionClassificationFailure(
          sql,
          userId,
          {
            transaction,
            account,
            error,
            phase: "sequential_escalation",
            batchContext,
            modelNameOverride: escalationModel,
          },
        );
        if (failedRow) {
          dataset = replaceTransactionInDataset(
            dataset,
            mapFromSql<Transaction>(failedRow),
          );
        }
        sequentialProcessedCount += 1;
        sequentialFailedCount += 1;
        remainingUnresolvedCount = countRemainingUnresolved(
          dataset.transactions,
          input.importBatchId,
        );

        await reportProgress({
          sequentialProcessed: sequentialProcessedCount,
          sequentialFailed: sequentialFailedCount,
          remainingUnresolvedCount,
          lastTransactionId: transaction.id,
        });
      }
    }
  }

  const touchedTransactionIds = [...new Set(batchTransactions.map((tx) => tx.id))];
  let finalSearchRefreshStatus: BatchClassificationProgress["finalSearchRefreshStatus"];
  let finalSearchRefreshError: string | null = null;
  await reportProgress({
    phase: "final_search_refresh",
  });
  try {
    if (touchedTransactionIds.length > 0) {
      if (await supportsJobType(sql, "transaction_search_index")) {
        await queueTransactionSearchIndexJob(sql, {
          userId,
          importBatchIds: [input.importBatchId],
          trigger: "classification_completion",
        });
        finalSearchRefreshStatus = "queued_background";
      } else {
        await syncTransactionSearchIndex(sql, userId, {
          transactionIds: touchedTransactionIds,
          onlyStaleOrMissing: false,
        });
        finalSearchRefreshStatus = "completed";
      }
    } else {
      finalSearchRefreshStatus = "completed";
    }
  } catch (error) {
    finalSearchRefreshStatus = "failed";
    finalSearchRefreshError =
      error instanceof Error ? error.message : "Final search refresh failed.";
  }

  await sql`
    update public.import_batches
    set classification_triggered_at = ${new Date().toISOString()}
    where id = ${input.importBatchId}
      and user_id = ${userId}
  `;

  for (const accountId of investmentAccountIds) {
    await queueJob(sql, "position_rebuild", {
      importBatchId: input.importBatchId,
      accountId,
      trigger: "classification_completion",
    });
  }

  const finalBatchTransactions = dataset.transactions
    .filter(
      (transaction) =>
        transaction.importBatchId === input.importBatchId && !transaction.voidedAt,
    )
    .sort(compareTransactionsByDate);

  await reportProgress({
    phase: "final_search_refresh",
    trustedResolvedCount: finalBatchTransactions.filter(isTrustedBatchResolution)
      .length,
    remainingUnresolvedCount: countRemainingUnresolved(
      finalBatchTransactions,
      input.importBatchId,
    ),
    finalSearchRefreshStatus,
    finalSearchRefreshError,
  });

  return {
    importBatchId: input.importBatchId,
    totalTransactions,
    firstPassPendingCount: pendingTransactions.length,
    firstPassProcessed: progress.firstPassProcessed,
    firstPassFailed: progress.firstPassFailed,
    sequentialCandidateCount: escalationCandidates.length,
    sequentialProcessed: progress.sequentialProcessed,
    sequentialFailed: progress.sequentialFailed,
    trustedResolvedCount: finalBatchTransactions.filter(isTrustedBatchResolution)
      .length,
    remainingUnresolvedCount: countRemainingUnresolved(
      finalBatchTransactions,
      input.importBatchId,
    ),
    searchBootstrapStatus,
    searchBootstrapError,
    finalSearchRefreshStatus,
    finalSearchRefreshError,
    queuedFollowUpPositionRebuilds: investmentAccountIds.size,
    escalationModel: canEscalate ? escalationModel : null,
  };
}

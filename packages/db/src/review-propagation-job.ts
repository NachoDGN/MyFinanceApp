import {
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";
import { type PromptProfileOverrides } from "@myfinance/llm";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import { loadDatasetForUser } from "./dataset-loader";
import { applyInvestmentRebuildWithinLock } from "./investment-rebuild-runner";
import { withInvestmentMutationLock } from "./investment-mutation-lock";
import { queueJob } from "./job-state";
import {
  buildResolvedSourcePrecedent,
  buildResolvedSourcePropagatedContextEntry,
  buildResolvedReviewSeedTransaction,
  buildUnresolvedSourcePropagatedContextEntry,
  mergePropagatedContextHistory,
  replaceTransactionInDataset,
  selectReviewPropagationCandidateMatches,
  shouldRunInvestmentRebuildAfterReviewPropagation,
  type PropagatedContextEntry,
  type ResolvedSourcePrecedent,
} from "./review-propagation-support";
import {
  mapFromSql,
  readOptionalRecord,
} from "./sql-json";
import { type SqlClient } from "./sql-runtime";
import {
  readTransactionReviewContext,
} from "./transaction-embedding-search";
import {
  executeTransactionEnrichmentPipeline,
  selectTransactionRowById,
} from "./transaction-enrichment";
import { updateTransactionRecord } from "./transaction-record";

type ReviewPropagationMode =
  | "unresolved_source_context"
  | "resolved_source_rereview";

const REVIEW_PROPAGATION_CANDIDATE_LIMIT = 200;

type ReviewPropagationCandidateResult = {
  afterTransaction: Transaction | null;
  shouldRunInvestmentRebuild: boolean;
  shouldQueueMetricRefresh: boolean;
};

async function applyReviewPropagationToCandidate(
  sql: SqlClient,
  input: {
    userId: string;
    mode: ReviewPropagationMode;
    dataset: DomainDataset;
    account: DomainDataset["accounts"][number];
    currentCandidate: Transaction;
    nextPropagatedContexts: PropagatedContextEntry[];
    promptOverrides: PromptProfileOverrides;
    resolvedSourcePrecedent: ResolvedSourcePrecedent | null;
  },
): Promise<ReviewPropagationCandidateResult> {
  if (input.mode === "unresolved_source_context") {
    const currentLlmPayload =
      readOptionalRecord(input.currentCandidate.llmPayload) ?? {};
    const nextLlmPayload = {
      ...currentLlmPayload,
      reviewContext: {
        ...(readOptionalRecord(currentLlmPayload.reviewContext) ?? {}),
        propagatedContexts: input.nextPropagatedContexts,
      },
    };

    if (JSON.stringify(currentLlmPayload) === JSON.stringify(nextLlmPayload)) {
      return {
        afterTransaction: null,
        shouldRunInvestmentRebuild: false,
        shouldQueueMetricRefresh: false,
      };
    }

    const after = await updateTransactionRecord(sql, {
      userId: input.userId,
      transactionId: input.currentCandidate.id,
      updatePayload: {
        updated_at: new Date().toISOString(),
      },
      llmPayload: nextLlmPayload,
    });
    const afterTransaction = mapFromSql<Transaction>(after);
    const auditEvent = createAuditEvent(
      "worker",
      "job:review_propagation",
      "transactions.review_propagate_context",
      "transaction",
      input.currentCandidate.id,
      input.currentCandidate as unknown as Record<string, unknown>,
      after,
    );
    await insertAuditEventRecord(
      sql,
      auditEvent,
      "Appended propagated unresolved review context from a similar transaction in the same account.",
    );

    return {
      afterTransaction,
      shouldRunInvestmentRebuild: false,
      shouldQueueMetricRefresh: false,
    };
  }

  const analysisCandidate = input.currentCandidate.needsReview
    ? input.currentCandidate
    : buildResolvedReviewSeedTransaction(
        input.currentCandidate,
        input.account.assetDomain,
      );
  const { afterRow: after, afterTransaction } =
    await executeTransactionEnrichmentPipeline(sql, input.userId, {
      dataset: input.dataset,
      account: input.account,
      transaction: analysisCandidate,
      enrichmentOptions: {
        trigger: "review_propagation",
        promptOverrides: input.promptOverrides,
        reviewContext: {
          previousReviewReason: input.currentCandidate.reviewReason ?? null,
          previousUserContext: input.currentCandidate.manualNotes ?? null,
          previousLlmPayload:
            input.currentCandidate.llmPayload &&
            typeof input.currentCandidate.llmPayload === "object"
              ? (input.currentCandidate.llmPayload as Record<string, unknown>)
              : null,
          propagatedContexts: input.nextPropagatedContexts,
          resolvedSourcePrecedent: input.resolvedSourcePrecedent,
        },
      },
    });
  const shouldRunInvestmentRebuild =
    input.account.assetDomain === "investment" &&
    shouldRunInvestmentRebuildAfterReviewPropagation(
      input.currentCandidate,
      afterTransaction,
    );
  const shouldRunImmediateInvestmentRebuild =
    shouldRunInvestmentRebuild && input.currentCandidate.needsReview === false;
  let nextAfterTransaction = afterTransaction;

  if (shouldRunImmediateInvestmentRebuild) {
    await applyInvestmentRebuildWithinLock(sql, input.userId, {
      historicalLookupTransactionIds: [afterTransaction.id],
    });
    const refreshedRow = await selectTransactionRowById(
      sql,
      input.userId,
      afterTransaction.id,
    );
    if (refreshedRow) {
      nextAfterTransaction = mapFromSql<Transaction>(refreshedRow);
    }
  }
  const auditEvent = createAuditEvent(
    "worker",
    "job:review_propagation",
    "transactions.review_propagate",
    "transaction",
    input.currentCandidate.id,
    input.currentCandidate as unknown as Record<string, unknown>,
    after,
  );
  await insertAuditEventRecord(
    sql,
    auditEvent,
    "Re-ran LLM classification for a similar unresolved transaction using a resolved precedent from the same account.",
  );

  return {
    afterTransaction: nextAfterTransaction,
    shouldRunInvestmentRebuild:
      shouldRunInvestmentRebuild && !shouldRunImmediateInvestmentRebuild,
    shouldQueueMetricRefresh: true,
  };
}

export async function processReviewPropagationJob(
  sql: SqlClient,
  userId: string,
  payloadJson: Record<string, unknown>,
  promptOverrides: PromptProfileOverrides,
) {
  const sourceTransactionId =
    typeof payloadJson.sourceTransactionId === "string"
      ? payloadJson.sourceTransactionId
      : "";
  const sourceAuditEventId =
    typeof payloadJson.sourceAuditEventId === "string"
      ? payloadJson.sourceAuditEventId
      : null;
  const includeResolvedTargets = payloadJson.includeResolvedTargets === true;
  if (!sourceTransactionId) {
    throw new Error("Review propagation job is missing sourceTransactionId.");
  }

  let dataset = await loadDatasetForUser(sql, userId);
  const sourceTransaction = dataset.transactions.find(
    (candidate) => candidate.id === sourceTransactionId,
  );
  if (!sourceTransaction) {
    throw new Error(
      `Source transaction ${sourceTransactionId} was not found for review propagation.`,
    );
  }

  const account = dataset.accounts.find(
    (candidate) => candidate.id === sourceTransaction.accountId,
  );
  if (!account) {
    throw new Error(
      `Account ${sourceTransaction.accountId} was not found for review propagation.`,
    );
  }

  if (sourceTransaction.voidedAt) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode: sourceTransaction.needsReview
        ? "unresolved_source_context"
        : "resolved_source_rereview",
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason: "source_transaction_not_eligible",
    };
  }

  const mode: ReviewPropagationMode =
    sourceTransaction.needsReview === true
      ? "unresolved_source_context"
      : "resolved_source_rereview";
  const candidateTransactions = dataset.transactions.filter(
    (candidate) =>
      candidate.accountId === account.id &&
      candidate.id !== sourceTransactionId &&
      (candidate.needsReview === true || includeResolvedTargets) &&
      !candidate.voidedAt,
  );
  if (candidateTransactions.length === 0) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
    };
  }

  const sourceSearchEmbeddingRows = await sql`
    select embedding
    from public.transaction_search_rows
    where transaction_id = ${sourceTransactionId}
      and user_id = ${userId}
      and embedding is not null
    limit 1
  `;
  if (!sourceSearchEmbeddingRows[0]?.embedding) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason: "source_embedding_unavailable",
    };
  }

  const candidateIds = candidateTransactions.map((candidate) => candidate.id);
  const rows = await sql`
    with source as (
      select embedding
      from public.transaction_search_rows
      where transaction_id = ${sourceTransactionId}
        and user_id = ${userId}
        and embedding is not null
      limit 1
    ),
    approximate_candidates as (
      select
        r.transaction_id,
        r.embedding
      from public.transaction_search_rows as r
      join public.transactions as t
        on t.id = r.transaction_id
      join public.transaction_search_batches as b
        on b.id = r.batch_id
      cross join source
      where r.user_id = ${userId}
        and r.account_id = ${account.id}
        and r.transaction_id in ${sql(candidateIds)}
        and r.transaction_id <> ${sourceTransactionId}
        and (${includeResolvedTargets} or coalesce(t.needs_review, false) = true)
        and t.voided_at is null
        and r.embedding_status in ('ready', 'stale')
        and b.status in ('ready', 'processing', 'stale')
      order by
        r.embedding::halfvec(3072) <=>
        source.embedding::halfvec(3072) asc
      limit ${Math.max(REVIEW_PROPAGATION_CANDIDATE_LIMIT, candidateIds.length)}
    )
    select
      candidate.transaction_id,
      1 - (candidate.embedding <=> source.embedding) as similarity
    from approximate_candidates as candidate
    cross join source
    order by candidate.embedding <=> source.embedding asc
  `;
  const embeddingMatches = rows.flatMap((row) => {
    const transactionId =
      typeof row.transaction_id === "string" ? row.transaction_id : "";
    const similarity = Number(row.similarity ?? 0);
    if (!transactionId || !Number.isFinite(similarity)) {
      return [];
    }

    return [{ transactionId, similarity }];
  });
  const candidateMatches = await selectReviewPropagationCandidateMatches({
    dataset,
    account,
    sourceTransaction,
    embeddingMatches,
    includeResolvedTargets,
  });

  const appliedTransactionIds: string[] = [];
  const failedTransactionIds: Array<{ transactionId: string; error: string }> =
    [];
  let attemptedCount = 0;
  let appliedCount = 0;
  let skippedCount = 0;
  let shouldRunInvestmentRebuild = false;
  let shouldQueueMetricRefresh = false;
  const propagatedAt = new Date().toISOString();
  const resolvedSourcePrecedent =
    mode === "resolved_source_rereview"
      ? buildResolvedSourcePrecedent(sourceTransaction, sourceAuditEventId)
      : null;

  return withInvestmentMutationLock(sql, userId, async () => {
    for (const match of candidateMatches) {
      const currentCandidate =
        dataset.transactions.find(
          (candidate) => candidate.id === match.transactionId,
        ) ?? null;
      if (
        !currentCandidate ||
        (!currentCandidate.needsReview && !includeResolvedTargets) ||
        currentCandidate.voidedAt
      ) {
        skippedCount += 1;
        continue;
      }

      attemptedCount += 1;
      try {
        const existingReviewContext =
          readTransactionReviewContext(currentCandidate) ?? {};
        const propagationEntry =
          mode === "resolved_source_rereview" && resolvedSourcePrecedent
            ? buildResolvedSourcePropagatedContextEntry({
                sourceTransaction,
                sourceAuditEventId,
                similarity: match.similarity,
                propagatedAt,
                precedent: resolvedSourcePrecedent,
              })
            : buildUnresolvedSourcePropagatedContextEntry({
                sourceTransaction,
                sourceAuditEventId,
                similarity: match.similarity,
                propagatedAt,
              });
        const nextPropagatedContexts = mergePropagatedContextHistory(
          existingReviewContext.propagatedContexts,
          propagationEntry,
        );
        const candidateResult = await applyReviewPropagationToCandidate(sql, {
          userId,
          mode,
          dataset,
          account,
          currentCandidate,
          nextPropagatedContexts,
          promptOverrides,
          resolvedSourcePrecedent,
        });

        if (!candidateResult.afterTransaction) {
          skippedCount += 1;
          continue;
        }

        dataset = replaceTransactionInDataset(
          dataset,
          candidateResult.afterTransaction,
        );
        appliedTransactionIds.push(candidateResult.afterTransaction.id);
        appliedCount += 1;
        shouldRunInvestmentRebuild ||=
          candidateResult.shouldRunInvestmentRebuild;
        shouldQueueMetricRefresh ||= candidateResult.shouldQueueMetricRefresh;
      } catch (candidateError) {
        skippedCount += 1;
        failedTransactionIds.push({
          transactionId: currentCandidate.id,
          error:
            candidateError instanceof Error
              ? candidateError.message
              : "Review propagation failed.",
        });
      }
    }

    let rebuilt:
      | Awaited<ReturnType<typeof applyInvestmentRebuildWithinLock>>
      | null = null;
    if (mode === "resolved_source_rereview" && shouldRunInvestmentRebuild) {
      rebuilt = await applyInvestmentRebuildWithinLock(sql, userId, {
        historicalLookupTransactionIds: appliedTransactionIds,
      });
    }
    if (mode === "resolved_source_rereview" && shouldQueueMetricRefresh) {
      await queueJob(sql, "metric_refresh", {
        trigger: "review_propagation",
        sourceTransactionId,
        accountId: account.id,
        appliedTransactionIds,
      });
    }

    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      includeResolvedTargets,
      candidateCount: candidateMatches.length,
      attemptedCount,
      appliedCount,
      skippedCount,
      appliedTransactionIds,
      failedTransactionIds,
      rebuilt,
    };
  });
}

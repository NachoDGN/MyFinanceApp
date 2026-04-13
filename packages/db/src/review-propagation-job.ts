import {
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";
import {
  ProviderApiError,
  type PromptProfileOverrides,
} from "@myfinance/llm";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import { loadDatasetForUser } from "./dataset-loader";
import { applyInvestmentRebuild } from "./investment-rebuild-runner";
import { withInvestmentMutationLock } from "./investment-mutation-lock";
import { queueJob } from "./job-state";
import {
  buildResolvedSourcePrecedent,
  buildResolvedSourcePropagatedContextEntry,
  buildUnresolvedSourcePropagatedContextEntry,
  mergePropagatedContextHistory,
  replaceTransactionInDataset,
  selectReviewPropagationCandidateMatches,
  type PropagatedContextEntry,
  type ResolvedSourcePrecedent,
} from "./review-propagation-support";
import {
  mapFromSql,
  readOptionalRecord,
} from "./sql-json";
import { type SqlClient } from "./sql-runtime";
import {
  ensureTransactionDescriptionEmbeddings,
  findSimilarUnresolvedTransactionsByDescriptionEmbedding,
  getReviewPropagationSimilarityThreshold,
  normalizeStoredVectorLiteral,
  parseTransactionEmbeddingSeedRow,
  readTransactionReviewContext,
} from "./transaction-embedding-search";
import { executeTransactionEnrichmentPipeline } from "./transaction-enrichment";
import { updateTransactionRecord } from "./transaction-record";

type ReviewPropagationMode =
  | "unresolved_source_context"
  | "resolved_source_rereview";

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
      "Appended propagated unresolved review context from a similar transaction in the same investment account.",
    );

    return {
      afterTransaction,
      shouldRunInvestmentRebuild: false,
      shouldQueueMetricRefresh: false,
    };
  }

  const { afterRow: after, afterTransaction } =
    await executeTransactionEnrichmentPipeline(sql, input.userId, {
      dataset: input.dataset,
      account: input.account,
      transaction: input.currentCandidate,
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
    "Re-ran LLM classification for a similar unresolved transaction using a resolved precedent from the same investment account.",
  );

  return {
    afterTransaction,
    shouldRunInvestmentRebuild: true,
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

  if (account.assetDomain !== "investment" || sourceTransaction.voidedAt) {
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
  const candidateSeedRowsRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where user_id = ${userId}
      and account_id = ${account.id}
      and id <> ${sourceTransactionId}
      and coalesce(needs_review, false) = true
      and voided_at is null
  `;
  if (candidateSeedRowsRaw.length === 0) {
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

  const sourceSeedRowRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where id = ${sourceTransactionId}
      and user_id = ${userId}
    limit 1
  `;
  const sourceSeedRow = sourceSeedRowRaw[0]
    ? parseTransactionEmbeddingSeedRow(
        sourceSeedRowRaw[0] as Record<string, unknown>,
      )
    : null;
  if (!sourceSeedRow) {
    throw new Error(
      `Source transaction ${sourceTransactionId} is missing description embedding context.`,
    );
  }

  const candidateSeedRows = candidateSeedRowsRaw.map((row) =>
    parseTransactionEmbeddingSeedRow(row as Record<string, unknown>),
  );
  let embeddingGeneration = {
    generatedCount: 0,
    skippedCount: 0,
    skippedReason: null as string | null,
  };
  try {
    const sourceEmbeddingResult = await ensureTransactionDescriptionEmbeddings(
      sql,
      userId,
      [sourceSeedRow],
    );
    const candidateEmbeddingResult =
      await ensureTransactionDescriptionEmbeddings(
        sql,
        userId,
        candidateSeedRows,
      );
    embeddingGeneration = {
      generatedCount:
        sourceEmbeddingResult.generatedCount +
        candidateEmbeddingResult.generatedCount,
      skippedCount:
        sourceEmbeddingResult.skippedCount +
        candidateEmbeddingResult.skippedCount,
      skippedReason:
        sourceEmbeddingResult.skippedReason ??
        candidateEmbeddingResult.skippedReason,
    };
  } catch (embeddingError) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason: "embedding_generation_failed",
      embeddingError:
        embeddingError instanceof Error
          ? embeddingError.message
          : "Embedding generation failed.",
      ...(embeddingError instanceof ProviderApiError &&
      (embeddingError.providerError || embeddingError.responseJson)
        ? {
            embeddingErrorResponse:
              embeddingError.providerError ?? embeddingError.responseJson,
          }
        : {}),
    };
  }

  const sourceEmbeddingRows = await sql`
    select description_embedding
    from public.transactions
    where id = ${sourceTransactionId}
      and user_id = ${userId}
    limit 1
  `;
  const sourceEmbedding = normalizeStoredVectorLiteral(
    sourceEmbeddingRows[0]?.description_embedding,
  );
  if (!sourceEmbedding) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason:
        embeddingGeneration.skippedReason ?? "source_embedding_unavailable",
      embeddingGeneration,
    };
  }

  const embeddingMatches =
    await findSimilarUnresolvedTransactionsByDescriptionEmbedding(sql, {
      userId,
      sourceTransactionId,
      accountId: account.id,
      sourceEmbedding,
      threshold: getReviewPropagationSimilarityThreshold(),
    });
  const candidateMatches = await selectReviewPropagationCandidateMatches({
    dataset,
    account,
    sourceTransaction,
    embeddingMatches,
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
        !currentCandidate.needsReview ||
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

    let rebuilt: Awaited<ReturnType<typeof applyInvestmentRebuild>> | null =
      null;
    if (mode === "resolved_source_rereview" && shouldRunInvestmentRebuild) {
      rebuilt = await applyInvestmentRebuild(sql, userId, {
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
      candidateCount: candidateMatches.length,
      attemptedCount,
      appliedCount,
      skippedCount,
      appliedTransactionIds,
      failedTransactionIds,
      rebuilt,
      embeddingGeneration,
    };
  });
}

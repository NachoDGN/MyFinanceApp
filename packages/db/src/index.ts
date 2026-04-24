import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import {
  getInvestmentTransactionClassifierConfig,
  getTransactionClassifierConfig,
  type SimilarAccountTransactionPromptContext,
} from "@myfinance/classification";
import {
  isInvestmentAccountType,
  assertCategoryCodeAllowedForAccount,
  assertEconomicEntityAllowedForAccount,
  assertRuleOutputsAllowedForScope,
  assertTransactionClassAllowedForAccount,
  isUncategorizedCategoryCode,
  needsCreditCardStatementUpload,
  parseRuleDraftRequest,
  isCreditCardSettlementTransaction,
  UNCATEGORIZED_TRANSACTION_REVIEW_REASON,
  type AddOpeningPositionInput,
  type AnswerTransactionQuestionInput,
  type Account,
  type ApplyRuleDraftInput,
  type AuditEvent,
  type CreditCardStatementImportInput,
  type CreditCardStatementImportResult,
  type CreateEntityInput,
  type CreateAccountInput,
  type CreateCategoryInput,
  type CreateManualInvestmentInput,
  type CreateRuleInput,
  type CreateTemplateInput,
  type DeleteAccountInput,
  type DeleteCategoryInput,
  type DeleteEntityInput,
  type DeleteHoldingAdjustmentInput,
  type DeleteManualInvestmentInput,
  type DeleteTemplateInput,
  type DomainDataset,
  type FinanceRepository,
  type ImportExecutionInput,
  type ImportCommitResult,
  type ImportPreviewResult,
  type JobRunResult,
  type QueueRuleDraftInput,
  type RecordManualInvestmentValuationInput,
  type ResetWorkspaceInput,
  type ResetWorkspaceResult,
  type Transaction,
  type UpdateAccountInput,
  type UpdateEntityInput,
  type UpdateManualInvestmentInput,
  type UpdateWorkspaceProfileInput,
  type UpdateTransactionInput,
} from "@myfinance/domain";
import {
  buildImportedTransactions,
  normalizeImportExecutionInput,
  runDeterministicImport,
  sanitizeImportResult,
} from "@myfinance/ingestion";
import { withInvestmentMutationLock } from "./investment-mutation-lock";
import { applyInvestmentRebuildWithinLock } from "./investment-rebuild-runner";
import { processReviewPropagationJob } from "./review-propagation-job";
export {
  refreshOwnedStockPrices,
  selectOwnedFundNavRefreshSecurities,
  selectOwnedStockPriceRefreshSecurities,
  selectTrackedEurFxPairs,
  type RefreshOwnedStockPricesResult,
} from "./market-data-refresh";
import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import {
  normalizeCreditCardSettlementText,
  resolveOrCreateLinkedCreditCardAccount,
} from "./credit-card-statement-linking";
import { loadDatasetForUser } from "./dataset-loader";
import {
  commitPreparedImportBatch,
  sumPreparedTransactionAmountBaseEur,
} from "./import-batches";
import { queueJob, supportsJobType } from "./job-state";
import {
  deactivateLearnedReviewExample,
  learnedReviewExamplesTableExists,
  listLearnedReviewExamples,
  resolveAnalyzerPromptProfileId,
  upsertLearnedReviewExample,
} from "./learned-review-examples";
import { buildReviewQueueCategoryOptions } from "./import-review-queue";
export {
  buildReviewQueueCategoryOptions,
  getImportBatchReviewQueueState,
  resolveImportReviewQueueReadiness,
  type ImportBatchReviewQueueState,
  type ImportBatchReviewQueueTransaction,
  type ImportReviewQueueReadiness,
} from "./import-review-queue";
import { loadPromptOverrides } from "./prompt-profiles";
import { runFinanceJobQueue } from "./job-runner";
export {
  deactivateLearnedReviewExample,
  listLearnedReviewExamples,
  resolveAnalyzerPromptProfileId,
} from "./learned-review-examples";
export {
  getPromptOverrides,
  listPromptProfiles,
  updatePromptProfile,
  type PromptProfileModel,
} from "./prompt-profiles";
import {
  type ReviewReanalysisFollowUpJobRef,
  type ReviewReanalysisMode,
  type ReviewReanalysisProgress,
} from "./review-reanalysis";
export {
  getReviewReanalysisJobStatus,
  queueTransactionReviewReanalysis,
  type QueueTransactionReviewReanalysisInput,
  type ReviewReanalysisFollowUpJobRef,
  type ReviewReanalysisFollowUpJobStatus,
  type ReviewReanalysisJobStatus,
  type ReviewReanalysisMode,
  type ReviewReanalysisProgress,
} from "./review-reanalysis";
import {
  createSqlClient,
  getDbRuntimeConfig,
  withSeededUserContext,
  withSeededUserSession,
  type DbRuntimeConfig,
  type SqlClient,
} from "./sql-runtime";
import {
  camelizeValue,
  mapFromSql,
  parseJsonColumn,
  serializeJson,
} from "./sql-json";
import { getRevolutRuntimeStatus } from "./revolut";
export {
  beginRevolutAuthorization,
  completeRevolutAuthorization,
  processRevolutWebhookEvent,
  queueRevolutConnectionSync,
} from "./revolut-connection";
export {
  buildResolvedSourcePrecedent,
  buildResolvedSourcePropagatedContextEntry,
  buildReviewPropagationUserContext,
  buildUnresolvedSourcePropagatedContextEntry,
  canSeedReviewPropagationFromTransaction,
  mergeEnrichmentDecisionWithExistingTransaction,
  mergePropagatedContextHistory,
  selectReviewPropagationCandidateMatches,
  shouldQueueReviewPropagationAfterManualReview,
  shouldRunInvestmentRebuildAfterReviewPropagation,
  type PropagatedContextEntry,
  type ResolvedSourcePrecedent,
} from "./review-propagation-support";
import {
  buildResolvedReviewSeedTransaction,
  buildResolvedReviewSimilarTransactionContext,
  canSeedReviewPropagationFromTransaction,
  refreshFinanceAnalyticsArtifacts,
  replaceTransactionInDataset,
  shouldQueueReviewPropagationAfterManualReview,
} from "./review-propagation-support";
import { updateTransactionRecord } from "./transaction-record";
export {
  DEFAULT_MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT,
  DEFAULT_RESOLVED_REVIEW_SIMILARITY_THRESHOLD,
  ensureTransactionDescriptionEmbeddings,
  findSimilarResolvedTransactionsByDescriptionEmbedding,
  findSimilarUnresolvedTransactionsByDescriptionEmbedding,
  getReviewPropagationSimilarityThreshold,
  parseTransactionEmbeddingSeedRow,
  readTransactionRawOutput,
  readTransactionReviewContext,
  serializeVector,
  type SimilarResolvedTransactionMatch,
  type SimilarUnresolvedTransactionMatch,
  type TransactionEmbeddingSeedRow,
} from "./transaction-embedding-search";
import {
  DEFAULT_MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT,
  DEFAULT_RESOLVED_REVIEW_SIMILARITY_THRESHOLD,
  ensureTransactionDescriptionEmbeddings,
  findSimilarResolvedTransactionsByDescriptionEmbedding,
  normalizeStoredVectorLiteral,
  parseTransactionEmbeddingSeedRow,
} from "./transaction-embedding-search";
export {
  fuseTransactionSearchResults,
  TRANSACTION_SEARCH_KEYWORD_WEIGHT,
  TRANSACTION_SEARCH_RRF_K,
  TRANSACTION_SEARCH_SEMANTIC_WEIGHT,
  type FusedTransactionSearchHit,
  type TransactionKeywordCandidate,
  type TransactionRerankedCandidate,
  type TransactionSearchDirection,
  type TransactionSearchReviewState,
  type TransactionSemanticCandidate,
} from "./transaction-search-fusion";
import {
  markTransactionSearchRowsStale,
  queueTransactionSearchIndexJob,
} from "./transaction-search-index";
export {
  markTransactionSearchRowsStale,
  processTransactionSearchIndexJob,
  queueTransactionSearchIndexJob,
  syncTransactionSearchIndex,
  type QueueTransactionSearchIndexInput,
  type SyncTransactionSearchIndexInput,
} from "./transaction-search-index";
import { searchTransactions } from "./transaction-search";
export {
  searchTransactions,
  resolveTransactionSearchFilters,
  type ParsedTransactionSearchQuery,
  type ResolvedTransactionSearchFilters,
  type SearchTransactionsResult,
  type TransactionSearchResultRow,
} from "./transaction-search";
import { runTransactionQuestionAgent } from "./transaction-agent";
export {
  runTransactionQuestionAgent,
} from "./transaction-agent";
export type {
  TransactionAgentDecision,
  TransactionAgentSettings,
  TransactionAgentToolName,
} from "./transaction-agent-types";
export {
  TRANSACTION_SELECT_COLUMN_NAMES,
  TRANSACTION_SELECT_COLUMNS,
} from "./transaction-columns";
import { transactionColumnsSql } from "./transaction-columns";
import {
  executeTransactionEnrichmentPipeline,
  selectTransactionRowById,
} from "./transaction-enrichment";

async function selectHoldingAdjustmentRowById(
  sql: SqlClient,
  userId: string,
  adjustmentId: string,
) {
  const rows = await sql`
    select *
    from public.holding_adjustments
    where id = ${adjustmentId}
      and user_id = ${userId}
    limit 1
  `;

  return rows[0] ?? null;
}

async function selectManualInvestmentRowById(
  sql: SqlClient,
  userId: string,
  manualInvestmentId: string,
) {
  const rows = await sql`
    select *
    from public.manual_investments
    where id = ${manualInvestmentId}
      and user_id = ${userId}
    limit 1
  `;

  return rows[0] ?? null;
}

export interface ReanalyzeTransactionReviewInput {
  transactionId: string;
  reviewContext?: string;
  selectedCategoryCode?: string | null;
  propagateResolvedMatches?: boolean;
  actorName: string;
  sourceChannel: AuditEvent["sourceChannel"];
  reviewMode?: ReviewReanalysisMode;
  onProgress?: (progress: ReviewReanalysisProgress) => Promise<void> | void;
}

const REVIEW_REANALYZE_COMPARISON_FIELDS = [
  "transactionClass",
  "categoryCode",
  "merchantNormalized",
  "counterpartyName",
  "economicEntityId",
  "classificationStatus",
  "classificationSource",
  "classificationConfidence",
  "securityId",
  "quantity",
  "unitPriceOriginal",
  "needsReview",
  "reviewReason",
] as const;

function getReviewReanalyzeChangedFields(
  before: Transaction,
  after: Transaction,
) {
  return REVIEW_REANALYZE_COMPARISON_FIELDS.filter(
    (field) => before[field] !== after[field],
  );
}

function normalizeOptionalTextValue(value: string | null | undefined) {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

function assertNoDeleteBlockers(
  blockerRow: Record<string, unknown>,
  messagePrefix: string,
) {
  const blockers = Object.entries(blockerRow)
    .filter(([, count]) => Number(count) > 0)
    .map(([key, count]) => `${key.replace(/_/g, " ")} (${count})`);
  if (blockers.length > 0) {
    throw new Error(`${messagePrefix} ${blockers.join(", ")}.`);
  }
}

function buildManualReviewContext(input: {
  reviewContext: string | null;
  selectedCategory: { code: string; displayName: string } | null;
}) {
  return [
    input.selectedCategory
      ? `The user explicitly selected the category ${input.selectedCategory.code} (${input.selectedCategory.displayName}).`
      : null,
    input.reviewContext,
  ]
    .filter((value): value is string => Boolean(value))
    .join("\n\n");
}

function shouldApplySelectedCategoryOverride(input: {
  account: Account;
  transaction: Transaction;
  selectedCategoryCode: string | null;
}) {
  if (!input.selectedCategoryCode) {
    return false;
  }

  if (input.transaction.transactionClass === "unknown") {
    return false;
  }

  if (needsCreditCardStatementUpload(input.transaction)) {
    return false;
  }

  if (
    input.transaction.categoryCode === input.selectedCategoryCode &&
    input.transaction.needsReview === false
  ) {
    return false;
  }

  if (!input.transaction.needsReview) {
    return true;
  }

  return (
    input.transaction.reviewReason ===
      UNCATEGORIZED_TRANSACTION_REVIEW_REASON ||
    input.transaction.categoryCode == null ||
    isUncategorizedCategoryCode(input.transaction.categoryCode)
  );
}

async function loadSimilarResolvedTransactionsForResolvedReview(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransaction: Transaction;
    dataset: DomainDataset;
  },
): Promise<SimilarAccountTransactionPromptContext[]> {
  const candidateSeedRowsRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.sourceTransaction.accountId}
      and id <> ${input.sourceTransaction.id}
      and coalesce(needs_review, false) = false
      and voided_at is null
  `;
  if (candidateSeedRowsRaw.length === 0) {
    return [];
  }

  const sourceSeedRows = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where id = ${input.sourceTransaction.id}
      and user_id = ${input.userId}
    limit 1
  `;
  const sourceSeedRow = sourceSeedRows[0]
    ? parseTransactionEmbeddingSeedRow(
        sourceSeedRows[0] as Record<string, unknown>,
      )
    : null;
  if (!sourceSeedRow) {
    return [];
  }

  const candidateSeedRows = candidateSeedRowsRaw.map((row) =>
    parseTransactionEmbeddingSeedRow(row as Record<string, unknown>),
  );
  try {
    await ensureTransactionDescriptionEmbeddings(sql, input.userId, [
      sourceSeedRow,
    ]);
    await ensureTransactionDescriptionEmbeddings(
      sql,
      input.userId,
      candidateSeedRows,
    );
  } catch {
    return [];
  }

  const sourceEmbeddingRows = await sql`
    select description_embedding
    from public.transactions
    where id = ${input.sourceTransaction.id}
      and user_id = ${input.userId}
    limit 1
  `;
  const sourceEmbedding = normalizeStoredVectorLiteral(
    sourceEmbeddingRows[0]?.description_embedding,
  );
  if (!sourceEmbedding) {
    return [];
  }

  const matches = await findSimilarResolvedTransactionsByDescriptionEmbedding(
    sql,
    {
      userId: input.userId,
      sourceTransactionId: input.sourceTransaction.id,
      accountId: input.sourceTransaction.accountId,
      sourceEmbedding,
      threshold: DEFAULT_RESOLVED_REVIEW_SIMILARITY_THRESHOLD,
      limit: DEFAULT_MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT,
    },
  );

  return matches
    .map((match) => {
      const transaction =
        input.dataset.transactions.find(
          (candidate) => candidate.id === match.transactionId,
        ) ?? null;
      return transaction
        ? buildResolvedReviewSimilarTransactionContext(
            transaction,
            match.similarity,
          )
        : null;
    })
    .filter((match): match is SimilarAccountTransactionPromptContext =>
      Boolean(match),
    );
}

export async function reanalyzeTransactionReview(
  input: ReanalyzeTransactionReviewInput,
) {
  const userId = getDbRuntimeConfig().seededUserId;
  await input.onProgress?.({
    stage: "load_context",
    message: "Loading transaction context.",
  });

  return withSeededUserContext(async (sql) => {
    const beforeRow = await selectTransactionRowById(
      sql,
      userId,
      input.transactionId,
    );
    if (!beforeRow) {
      throw new Error(`Transaction ${input.transactionId} not found.`);
    }

    const beforeTransaction = mapFromSql<Transaction>(beforeRow);
    const dataset = await loadDatasetForUser(sql, userId);
    const account = dataset.accounts.find(
      (candidate) => candidate.id === beforeTransaction.accountId,
    );
    if (!account) {
      throw new Error(
        `Account ${beforeTransaction.accountId} not found for review reanalysis.`,
      );
    }

    const normalizedSelectedCategoryCode = normalizeOptionalTextValue(
      input.selectedCategoryCode,
    );
    const selectableCategories = buildReviewQueueCategoryOptions(
      dataset,
      account,
      beforeTransaction,
    );
    const selectedCategory = normalizedSelectedCategoryCode
      ? (selectableCategories.find(
          (category) => category.code === normalizedSelectedCategoryCode,
        ) ?? null)
      : null;
    if (normalizedSelectedCategoryCode && !selectedCategory) {
      throw new Error(
        `Category ${normalizedSelectedCategoryCode} is not allowed for ${account.displayName}.`,
      );
    }

    const normalizedReviewContext = buildManualReviewContext({
      reviewContext: normalizeOptionalTextValue(input.reviewContext),
      selectedCategory: selectedCategory
        ? {
            code: selectedCategory.code,
            displayName: selectedCategory.displayName,
          }
        : null,
    });
    if (!normalizedReviewContext) {
      throw new Error("Review input requires context or a selected category.");
    }
    const reviewMode =
      input.reviewMode ??
      (beforeTransaction.needsReview
        ? "manual_review_update"
        : "manual_resolved_review");
    const promptOverrides = await loadPromptOverrides(sql, userId);
    const wasPendingReview = beforeTransaction.needsReview;
    const followUpJobs: ReviewReanalysisFollowUpJobRef[] = [];
    const analysisTransaction =
      reviewMode === "manual_resolved_review"
        ? buildResolvedReviewSeedTransaction(
            beforeTransaction,
            account.assetDomain,
          )
        : beforeTransaction;
    const similarResolvedTransactions =
      reviewMode === "manual_resolved_review"
        ? await loadSimilarResolvedTransactionsForResolvedReview(sql, {
            userId,
            sourceTransaction: beforeTransaction,
            dataset,
          })
        : undefined;

    await input.onProgress?.({
      stage: "llm_reanalysis",
      message:
        reviewMode === "manual_resolved_review"
          ? "Running a clean transaction reanalysis with similar resolved history."
          : "Running transaction analyzer with your review context.",
    });
    let afterTransaction: Transaction | null = null;
    let changedFields: string[] = [];
    let auditEvent: AuditEvent | null = null;

    await withInvestmentMutationLock(sql, userId, async () => {
      await input.onProgress?.({
        stage: "apply_transaction_update",
        message: "Applying analyzer results to the transaction.",
      });
      const { decision, afterRow: after } =
        await executeTransactionEnrichmentPipeline(sql, userId, {
          dataset,
          account,
          transaction: analysisTransaction,
          enrichmentOptions: {
            trigger: reviewMode,
            reviewContext: {
              userProvidedContext: normalizedReviewContext,
              previousReviewReason:
                reviewMode === "manual_resolved_review"
                  ? null
                  : (beforeTransaction.reviewReason ?? null),
              previousUserContext:
                reviewMode === "manual_resolved_review"
                  ? null
                  : (beforeTransaction.manualNotes ?? null),
              previousLlmPayload:
                reviewMode === "manual_resolved_review"
                  ? null
                  : beforeTransaction.llmPayload &&
                      typeof beforeTransaction.llmPayload === "object"
                    ? (beforeTransaction.llmPayload as Record<string, unknown>)
                    : null,
            },
            promptOverrides,
            similarAccountTransactions: similarResolvedTransactions,
          },
          updateOptions: {
            manualNotes: normalizedReviewContext,
          },
        });

      if (account.assetDomain === "investment") {
        await input.onProgress?.({
          stage: "investment_rebuild",
          message: "Rebuilding investment positions and fetching dated prices.",
        });
        await applyInvestmentRebuildWithinLock(sql, userId, {
          onProgress: input.onProgress,
          historicalLookupTransactionIds: [input.transactionId],
        });
      }
      await input.onProgress?.({
        stage: "metric_refresh",
        message: "Refreshing portfolio metrics.",
      });
      const metricRefreshJobId = await queueJob(sql, "metric_refresh", {
        trigger: reviewMode,
        transactionId: input.transactionId,
        accountId: beforeTransaction.accountId,
      });
      followUpJobs.push({
        id: metricRefreshJobId,
        jobType: "metric_refresh",
      });

      const finalRow = await selectTransactionRowById(
        sql,
        userId,
        input.transactionId,
      );
      if (!finalRow) {
        throw new Error(
          `Transaction ${input.transactionId} disappeared after review reanalysis.`,
        );
      }
      afterTransaction = mapFromSql<Transaction>(finalRow);
      if (
        shouldApplySelectedCategoryOverride({
          account,
          transaction: afterTransaction,
          selectedCategoryCode: normalizedSelectedCategoryCode,
        })
      ) {
        const currentLlmPayload =
          afterTransaction.llmPayload &&
          typeof afterTransaction.llmPayload === "object" &&
          !Array.isArray(afterTransaction.llmPayload)
            ? (afterTransaction.llmPayload as Record<string, unknown>)
            : {};
        const currentReviewContext =
          currentLlmPayload.reviewContext &&
          typeof currentLlmPayload.reviewContext === "object" &&
          !Array.isArray(currentLlmPayload.reviewContext)
            ? (currentLlmPayload.reviewContext as Record<string, unknown>)
            : {};
        const currentApplied =
          currentLlmPayload.applied &&
          typeof currentLlmPayload.applied === "object" &&
          !Array.isArray(currentLlmPayload.applied)
            ? (currentLlmPayload.applied as Record<string, unknown>)
            : {};
        const categoryOverrideRow = await updateTransactionRecord(sql, {
          userId,
          transactionId: afterTransaction.id,
          updatePayload: {
            category_code: normalizedSelectedCategoryCode,
            classification_status: "manual_override",
            classification_source: "manual",
            classification_confidence: "1.00",
            needs_review: false,
            review_reason: null,
            updated_at: new Date().toISOString(),
          },
          llmPayload: {
            ...currentLlmPayload,
            reviewContext: {
              ...currentReviewContext,
              selectedCategoryCode: normalizedSelectedCategoryCode,
            },
            applied: {
              ...currentApplied,
              categoryCode: normalizedSelectedCategoryCode,
              needsReview: false,
              reviewReason: null,
              classificationStatus: "manual_override",
              classificationSource: "manual",
              classificationConfidence: "1.00",
            },
          },
        });
        afterTransaction = mapFromSql<Transaction>(categoryOverrideRow);
      }
      changedFields = getReviewReanalyzeChangedFields(
        beforeTransaction,
        afterTransaction,
      );
      await markTransactionSearchRowsStale(sql, {
        userId,
        transactionIds: [afterTransaction.id],
      });
      await queueTransactionSearchIndexJob(sql, {
        userId,
        transactionIds: [afterTransaction.id],
        trigger: reviewMode,
      });
      auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "transactions.review_reanalyze",
        "transaction",
        input.transactionId,
        beforeRow,
        after,
      );
      await insertAuditEventRecord(
        sql,
        auditEvent,
        reviewMode === "manual_resolved_review"
          ? "Re-ran LLM classification for a previously resolved transaction from a clean baseline with similar resolved precedent context."
          : "Re-ran LLM classification for a single transaction with manual review context.",
      );
      if (wasPendingReview && afterTransaction.needsReview === false) {
        await upsertLearnedReviewExample(sql, {
          userId,
          accountId: beforeTransaction.accountId,
          sourceTransaction: beforeTransaction,
          correctedTransaction: afterTransaction,
          sourceAuditEventId: auditEvent.id,
          promptProfileId: resolveAnalyzerPromptProfileId(account.assetDomain),
          userContext: normalizedReviewContext,
        });
      }
      const shouldIncludeResolvedTargets =
        input.propagateResolvedMatches === true &&
        afterTransaction.needsReview === false &&
        canSeedReviewPropagationFromTransaction(account, afterTransaction);
      if (
        (wasPendingReview &&
          shouldQueueReviewPropagationAfterManualReview(
            account,
            beforeTransaction,
          )) ||
        shouldIncludeResolvedTargets
      ) {
        await input.onProgress?.({
          stage: "review_propagation",
          message: shouldIncludeResolvedTargets
            ? "Propagating the correction to similar transactions, including already-resolved matches."
            : "Propagating the correction to similar unresolved transactions.",
        });
        const reviewPropagationPayload = {
          sourceTransactionId: afterTransaction.id,
          accountId: afterTransaction.accountId,
          sourceAuditEventId: auditEvent.id,
          includeResolvedTargets: shouldIncludeResolvedTargets,
        };
        if (await supportsJobType(sql, "review_propagation")) {
          const reviewPropagationJobId = await queueJob(
            sql,
            "review_propagation",
            reviewPropagationPayload,
          );
          followUpJobs.push({
            id: reviewPropagationJobId,
            jobType: "review_propagation",
            includeResolvedTargets: shouldIncludeResolvedTargets,
          });
        } else {
          await processReviewPropagationJob(
            sql,
            userId,
            reviewPropagationPayload,
            promptOverrides,
          );
        }
      }
    });

    if (!afterTransaction || !auditEvent) {
      throw new Error(
        `Transaction ${input.transactionId} was not finalized after review reanalysis.`,
      );
    }

    return {
      applied: true,
      changed: changedFields.length > 0,
      changedFields,
      transaction: afterTransaction,
      auditEvent,
      followUpJobs,
    };
  });
}

class SqlFinanceRepository implements FinanceRepository {
  private userId = getDbRuntimeConfig().seededUserId;

  async getDataset(): Promise<DomainDataset> {
    return withSeededUserContext((sql) => loadDatasetForUser(sql, this.userId));
  }

  async searchTransactions(input: {
    dataset: DomainDataset;
    scope:
      | { kind: "consolidated" }
      | { kind: "entity"; entityId?: string }
      | { kind: "account"; accountId?: string };
    period: {
      start: string;
      end: string;
      preset: "mtd" | "ytd" | "week" | "24m" | "custom";
    };
    referenceDate: string;
    query: string;
  }) {
    return withSeededUserSession((sql) =>
      searchTransactions(sql, this.userId, input),
    );
  }

  async answerTransactionQuestion(input: AnswerTransactionQuestionInput) {
    const dataset = await this.getDataset();
    return withSeededUserSession((sql) =>
      runTransactionQuestionAgent(sql, this.userId, {
        ...input,
        dataset,
      }),
    );
  }

  async updateWorkspaceProfile(input: UpdateWorkspaceProfileInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.profiles
        where id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Profile ${this.userId} was not found.`);
      }

      const afterJson = {
        ...mapFromSql<DomainDataset["profile"]>(beforeRow),
        displayName: input.profile.displayName,
        defaultBaseCurrency: input.profile.defaultBaseCurrency,
        timezone: input.profile.timezone,
        workspaceSettingsJson: input.profile.workspaceSettingsJson,
      };

      if (input.apply) {
        await sql`
          update public.profiles
          set
            display_name = ${input.profile.displayName},
            default_base_currency = ${input.profile.defaultBaseCurrency},
            timezone = ${input.profile.timezone},
            workspace_settings_json = ${serializeJson(
              sql,
              input.profile.workspaceSettingsJson,
            )}::jsonb
          where id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "profile.update",
          "profile",
          this.userId,
          mapFromSql<DomainDataset["profile"]>(beforeRow) as unknown as Record<
            string,
            unknown
          >,
          afterJson as unknown as Record<string, unknown>,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          "Updated workspace profile defaults.",
        );
      }

      return {
        applied: input.apply,
        profileId: this.userId,
      };
    });
  }

  async createEntity(input: CreateEntityInput) {
    return withSeededUserContext(async (sql) => {
      const existingSlugRows = await sql`
        select id
        from public.entities
        where user_id = ${this.userId}
          and slug = ${input.entity.slug}
        limit 1
      `;
      if (existingSlugRows[0]) {
        throw new Error(
          `Entity slug "${input.entity.slug}" is already in use.`,
        );
      }

      if (input.entity.entityKind === "personal") {
        const personalRows = await sql`
          select id
          from public.entities
          where user_id = ${this.userId}
            and entity_kind = 'personal'
          limit 1
        `;
        if (personalRows[0]) {
          throw new Error(
            "A personal entity already exists. Add companies instead of creating a second personal owner.",
          );
        }
      }

      const entityId = randomUUID();
      const afterJson = {
        id: entityId,
        userId: this.userId,
        slug: input.entity.slug,
        displayName: input.entity.displayName,
        legalName: input.entity.legalName ?? null,
        entityKind: input.entity.entityKind,
        baseCurrency: input.entity.baseCurrency,
        active: true,
      };

      if (input.apply) {
        await sql`
          insert into public.entities (
            id,
            user_id,
            slug,
            display_name,
            legal_name,
            entity_kind,
            base_currency,
            active
          ) values (
            ${entityId},
            ${this.userId},
            ${input.entity.slug},
            ${input.entity.displayName},
            ${input.entity.legalName ?? null},
            ${input.entity.entityKind},
            ${input.entity.baseCurrency},
            true
          )
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.create",
          "entity",
          entityId,
          null,
          afterJson,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          `Created entity ${input.entity.displayName}.`,
        );
      }

      return {
        applied: input.apply,
        entityId,
      };
    });
  }

  async updateEntity(input: UpdateEntityInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.entities
        where id = ${input.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Entity ${input.entityId} not found.`);
      }

      const nextSlug = input.patch.slug ?? beforeRow.slug;
      if (nextSlug !== beforeRow.slug) {
        const duplicateSlugRows = await sql`
          select id
          from public.entities
          where user_id = ${this.userId}
            and slug = ${nextSlug}
            and id <> ${input.entityId}
          limit 1
        `;
        if (duplicateSlugRows[0]) {
          throw new Error(`Entity slug "${nextSlug}" is already in use.`);
        }
      }

      const afterJson = {
        ...mapFromSql<DomainDataset["entities"][number]>(beforeRow),
        slug: nextSlug,
        displayName: input.patch.displayName ?? beforeRow.display_name,
        legalName: Object.prototype.hasOwnProperty.call(
          input.patch,
          "legalName",
        )
          ? (input.patch.legalName ?? null)
          : beforeRow.legal_name,
        baseCurrency: input.patch.baseCurrency ?? beforeRow.base_currency,
      };

      if (input.apply) {
        await sql`
          update public.entities
          set
            slug = ${nextSlug},
            display_name = ${input.patch.displayName ?? beforeRow.display_name},
            legal_name = ${
              Object.prototype.hasOwnProperty.call(input.patch, "legalName")
                ? (input.patch.legalName ?? null)
                : beforeRow.legal_name
            },
            base_currency = ${input.patch.baseCurrency ?? beforeRow.base_currency}
          where id = ${input.entityId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.update",
          "entity",
          input.entityId,
          mapFromSql<DomainDataset["entities"][number]>(
            beforeRow,
          ) as unknown as Record<string, unknown>,
          afterJson as unknown as Record<string, unknown>,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          `Updated entity ${afterJson.displayName}.`,
        );
        await markTransactionSearchRowsStale(sql, {
          userId: this.userId,
          entityIds: [input.entityId],
        });
        await queueTransactionSearchIndexJob(sql, {
          userId: this.userId,
          entityIds: [input.entityId],
          trigger: "entity_update",
        });
      }

      return {
        applied: input.apply,
        entityId: input.entityId,
      };
    });
  }

  async deleteEntity(input: DeleteEntityInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.entities
        where id = ${input.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Entity ${input.entityId} not found.`);
      }
      if (beforeRow.entity_kind === "personal") {
        throw new Error(
          "The personal entity cannot be deleted. Keep one personal owner and add or remove company entities around it.",
        );
      }

      const blockers = await sql`
        with target as (
          select ${input.entityId}::uuid as entity_id
        )
        select
          (select count(*)::int from public.accounts where entity_id = target.entity_id) as accounts,
          (
            select count(*)::int
            from public.transactions
            where economic_entity_id = target.entity_id
               or account_entity_id = target.entity_id
          ) as transactions,
          (
            select count(*)::int
            from public.holding_adjustments
            where entity_id = target.entity_id
          ) as holding_adjustments,
          (
            select count(*)::int
            from public.manual_investments
            where entity_id = target.entity_id
          ) as manual_investments,
          (
            select count(*)::int
            from public.manual_investment_valuations
            where manual_investment_id in (
              select id from public.manual_investments where entity_id = target.entity_id
            )
          ) as manual_investment_valuations,
          (
            select count(*)::int
            from public.investment_positions
            where entity_id = target.entity_id
          ) as investment_positions,
          (
            select count(*)::int
            from public.daily_portfolio_snapshots
            where entity_id = target.entity_id
          ) as portfolio_snapshots
        from target
      `;
      assertNoDeleteBlockers(
        blockers[0] as Record<string, unknown>,
        "Entity cannot be removed because it is still referenced by",
      );

      if (input.apply) {
        await sql`
          delete from public.entities
          where id = ${input.entityId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.delete",
          "entity",
          input.entityId,
          mapFromSql<DomainDataset["entities"][number]>(
            beforeRow,
          ) as unknown as Record<string, unknown>,
          null,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          `Removed entity ${beforeRow.display_name}.`,
        );
      }

      return {
        applied: input.apply,
        entityId: input.entityId,
      };
    });
  }

  async createAccount(input: CreateAccountInput) {
    return withSeededUserContext(async (sql) => {
      const entityRows = await sql`
        select * from public.entities
        where id = ${input.account.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      if (!entityRows[0]) {
        throw new Error(`Entity ${input.account.entityId} not found.`);
      }

      if (input.account.importTemplateDefaultId) {
        const templateRows = await sql`
          select compatible_account_type from public.import_templates
          where id = ${input.account.importTemplateDefaultId}
            and user_id = ${this.userId}
          limit 1
        `;
        const template = templateRows[0];
        if (!template) {
          throw new Error(
            `Template ${input.account.importTemplateDefaultId} not found.`,
          );
        }
        if (template.compatible_account_type !== input.account.accountType) {
          throw new Error(
            `Template ${input.account.importTemplateDefaultId} is not compatible with ${input.account.accountType}.`,
          );
        }
      }

      const accountId = randomUUID();
      const afterJson = {
        id: accountId,
        userId: this.userId,
        ...input.account,
        importTemplateDefaultId: input.account.importTemplateDefaultId ?? null,
        openingBalanceOriginal: input.account.openingBalanceOriginal ?? null,
        openingBalanceCurrency: input.account.openingBalanceCurrency ?? null,
        openingBalanceDate: input.account.openingBalanceDate ?? null,
        accountSuffix: input.account.accountSuffix ?? null,
        staleAfterDays: input.account.staleAfterDays ?? null,
        lastImportedAt: null,
      };

      if (input.apply) {
        await sql`
          insert into public.accounts (
            id,
            user_id,
            entity_id,
            institution_name,
            display_name,
            account_type,
            asset_domain,
            default_currency,
            opening_balance_original,
            opening_balance_currency,
            opening_balance_date,
            include_in_consolidation,
            is_active,
            import_template_default_id,
            matching_aliases,
            account_suffix,
            balance_mode,
            stale_after_days
          ) values (
            ${accountId},
            ${this.userId},
            ${input.account.entityId},
            ${input.account.institutionName},
            ${input.account.displayName},
            ${input.account.accountType},
            ${input.account.assetDomain},
            ${input.account.defaultCurrency},
            ${input.account.openingBalanceOriginal ?? null},
            ${input.account.openingBalanceCurrency ?? null},
            ${input.account.openingBalanceDate ?? null},
            ${input.account.includeInConsolidation},
            ${input.account.isActive},
            ${input.account.importTemplateDefaultId ?? null},
            ${input.account.matchingAliases},
            ${input.account.accountSuffix ?? null},
            ${input.account.balanceMode},
            ${input.account.staleAfterDays ?? null}
          )
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "accounts.create",
          "account",
          accountId,
          null,
          afterJson,
        );
        await insertAuditEventRecord(sql, auditEvent);
      }

      return { applied: input.apply, accountId };
    });
  }

  async updateAccount(input: UpdateAccountInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.accounts
        where id = ${input.accountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Account ${input.accountId} not found.`);
      }

      const hasTemplatePatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "importTemplateDefaultId",
      );
      const nextImportTemplateDefaultId = hasTemplatePatch
        ? (input.patch.importTemplateDefaultId ?? null)
        : beforeRow.import_template_default_id;

      if (nextImportTemplateDefaultId) {
        const templateRows = await sql`
          select compatible_account_type
          from public.import_templates
          where id = ${nextImportTemplateDefaultId}
            and user_id = ${this.userId}
          limit 1
        `;
        const template = templateRows[0];
        if (!template) {
          throw new Error(`Template ${nextImportTemplateDefaultId} not found.`);
        }
        if (template.compatible_account_type !== beforeRow.account_type) {
          throw new Error(
            `Template ${nextImportTemplateDefaultId} is not compatible with ${beforeRow.account_type}.`,
          );
        }
      }

      const hasOpeningBalanceOriginalPatch =
        Object.prototype.hasOwnProperty.call(
          input.patch,
          "openingBalanceOriginal",
        );
      const hasOpeningBalanceDatePatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "openingBalanceDate",
      );
      const hasAliasesPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "matchingAliases",
      );
      const hasIncludeInConsolidationPatch =
        Object.prototype.hasOwnProperty.call(
          input.patch,
          "includeInConsolidation",
        );
      const hasAccountSuffixPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "accountSuffix",
      );
      const hasStaleAfterDaysPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "staleAfterDays",
      );

      const nextInstitutionName =
        input.patch.institutionName ?? beforeRow.institution_name;
      const nextDisplayName = input.patch.displayName ?? beforeRow.display_name;
      const nextDefaultCurrency =
        input.patch.defaultCurrency ?? beforeRow.default_currency;
      const nextOpeningBalanceOriginal = hasOpeningBalanceOriginalPatch
        ? (input.patch.openingBalanceOriginal ?? null)
        : beforeRow.opening_balance_original;
      const nextOpeningBalanceDate = nextOpeningBalanceOriginal
        ? hasOpeningBalanceDatePatch
          ? (input.patch.openingBalanceDate ?? null)
          : beforeRow.opening_balance_date
        : null;
      const nextIncludeInConsolidation = hasIncludeInConsolidationPatch
        ? input.patch.includeInConsolidation
        : beforeRow.include_in_consolidation;
      const nextMatchingAliases = hasAliasesPatch
        ? (input.patch.matchingAliases ?? [])
        : beforeRow.matching_aliases;
      const nextAccountSuffix = hasAccountSuffixPatch
        ? (input.patch.accountSuffix ?? null)
        : beforeRow.account_suffix;
      const nextBalanceMode = input.patch.balanceMode ?? beforeRow.balance_mode;
      const nextStaleAfterDays = hasStaleAfterDaysPatch
        ? (input.patch.staleAfterDays ?? null)
        : beforeRow.stale_after_days;

      const beforeAccount =
        mapFromSql<DomainDataset["accounts"][number]>(beforeRow);
      const afterJson = {
        ...beforeAccount,
        institutionName: nextInstitutionName,
        displayName: nextDisplayName,
        defaultCurrency: nextDefaultCurrency,
        openingBalanceOriginal: nextOpeningBalanceOriginal,
        openingBalanceCurrency: nextOpeningBalanceOriginal
          ? nextDefaultCurrency
          : null,
        openingBalanceDate: nextOpeningBalanceDate,
        includeInConsolidation: nextIncludeInConsolidation,
        importTemplateDefaultId: nextImportTemplateDefaultId,
        matchingAliases: nextMatchingAliases,
        accountSuffix: nextAccountSuffix,
        balanceMode: nextBalanceMode,
        staleAfterDays: nextStaleAfterDays,
      };

      if (input.apply) {
        await sql`
          update public.accounts
          set
            institution_name = ${nextInstitutionName},
            display_name = ${nextDisplayName},
            default_currency = ${nextDefaultCurrency},
            opening_balance_original = ${nextOpeningBalanceOriginal},
            opening_balance_currency = ${
              nextOpeningBalanceOriginal ? nextDefaultCurrency : null
            },
            opening_balance_date = ${nextOpeningBalanceDate},
            include_in_consolidation = ${nextIncludeInConsolidation},
            import_template_default_id = ${nextImportTemplateDefaultId},
            matching_aliases = ${nextMatchingAliases},
            account_suffix = ${nextAccountSuffix},
            balance_mode = ${nextBalanceMode},
            stale_after_days = ${nextStaleAfterDays}
          where id = ${input.accountId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "accounts.update",
          "account",
          input.accountId,
          beforeAccount as unknown as Record<string, unknown>,
          afterJson as unknown as Record<string, unknown>,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          `Updated account ${afterJson.displayName}.`,
        );
        await markTransactionSearchRowsStale(sql, {
          userId: this.userId,
          accountIds: [input.accountId],
        });
        await queueTransactionSearchIndexJob(sql, {
          userId: this.userId,
          accountIds: [input.accountId],
          trigger: "account_update",
        });
      }

      return { applied: input.apply, accountId: input.accountId };
    });
  }

  async deleteAccount(input: DeleteAccountInput) {
    return withSeededUserContext(async (sql) => {
      const before = await sql`
        select * from public.accounts
        where id = ${input.accountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = before[0];
      if (!beforeRow) {
        throw new Error(`Account ${input.accountId} not found.`);
      }

      const blockers = await sql`
        with target as (
          select ${input.accountId}::uuid as account_id
        )
        select
          (select count(*)::int from public.import_batches where account_id = target.account_id) as import_batches,
          (
            select count(*)::int
            from public.transactions
            where account_id = target.account_id
               or related_account_id = target.account_id
          ) as transactions,
          (select count(*)::int from public.account_balance_snapshots where account_id = target.account_id) as balance_snapshots,
          (select count(*)::int from public.holding_adjustments where account_id = target.account_id) as holding_adjustments,
          (select count(*)::int from public.manual_investments where funding_account_id = target.account_id) as manual_investments,
          (select count(*)::int from public.investment_positions where account_id = target.account_id) as investment_positions,
          (select count(*)::int from public.daily_portfolio_snapshots where account_id = target.account_id) as portfolio_snapshots
        from target
      `;
      assertNoDeleteBlockers(
        blockers[0] as Record<string, unknown>,
        "Account cannot be removed because it already has dependent data:",
      );

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "accounts.delete",
        "account",
        input.accountId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.accounts
          where id = ${input.accountId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(sql, auditEvent);
      }

      return { applied: input.apply, accountId: input.accountId };
    });
  }

  async resetWorkspace(
    input: ResetWorkspaceInput,
  ): Promise<ResetWorkspaceResult> {
    return withSeededUserContext(async (sql) => {
      const [
        portfolioSnapshots,
        investmentPositions,
        holdingAdjustments,
        manualInvestmentValuations,
        manualInvestments,
        balanceSnapshots,
      ] = await Promise.all([
        sql`
            delete from public.daily_portfolio_snapshots
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.investment_positions
            where user_id = ${this.userId}
            returning account_id
          `,
        sql`
            delete from public.holding_adjustments
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.manual_investment_valuations
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.manual_investments
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.account_balance_snapshots
            where account_id in (
              select id from public.accounts where user_id = ${this.userId}
            )
            returning account_id
          `,
      ]);

      const [transactions, importBatches, rules, accounts, importTemplates] =
        await Promise.all([
          sql`
            delete from public.transactions
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.import_batches
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.classification_rules
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.accounts
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.import_templates
            where user_id = ${this.userId}
            returning id
          `,
        ]);

      const learnedReviewExamples = (await learnedReviewExamplesTableExists(
        sql,
      ))
        ? await sql`
            delete from public.learned_review_examples
            where user_id = ${this.userId}
            returning id
          `
        : [];

      const jobs = await sql`
          delete from public.jobs
          returning id
      `;

      await sql`
        delete from public.audit_events
        where actor_id = ${this.userId}
           or object_type in ('account', 'classification_rule', 'job', 'import_template', 'transaction')
      `;

      const deleted = {
        accounts: accounts.length,
        importTemplates: importTemplates.length,
        importBatches: importBatches.length,
        transactions: transactions.length,
        balanceSnapshots: balanceSnapshots.length,
        holdingAdjustments: holdingAdjustments.length,
        manualInvestments: manualInvestments.length,
        manualInvestmentValuations: manualInvestmentValuations.length,
        investmentPositions: investmentPositions.length,
        portfolioSnapshots: portfolioSnapshots.length,
        rules: rules.length,
        jobs: jobs.length,
        learnedReviewExamples: learnedReviewExamples.length,
      };

      if (input.apply) {
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "workspace.reset",
          "workspace",
          this.userId,
          null,
          deleted,
        );
        await insertAuditEventRecord(
          sql,
          auditEvent,
          "Cleared seeded demo finance data for a fresh local workspace.",
        );
      }

      return {
        applied: input.apply,
        deleted,
      };
    });
  }

  async updateTransaction(input: UpdateTransactionInput): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }> {
    return withSeededUserContext(async (sql) => {
      const beforeRow = await selectTransactionRowById(
        sql,
        this.userId,
        input.transactionId,
      );
      if (!beforeRow) {
        throw new Error(`Transaction ${input.transactionId} not found.`);
      }

      const patch = camelizeValue(input.patch);
      const requiresClassificationValidation =
        patch.transactionClass !== undefined ||
        patch.categoryCode !== undefined ||
        patch.economicEntityId !== undefined ||
        input.createRuleFromTransaction === true;
      const dataset = requiresClassificationValidation
        ? await loadDatasetForUser(sql, this.userId)
        : null;
      const account =
        dataset?.accounts.find(
          (candidate) => candidate.id === beforeRow.account_id,
        ) ?? null;
      if (requiresClassificationValidation && !account) {
        throw new Error(
          `Account ${beforeRow.account_id} was not found for transaction ${input.transactionId}.`,
        );
      }

      if (account && patch.transactionClass !== undefined) {
        assertTransactionClassAllowedForAccount(
          account,
          patch.transactionClass,
          "Manual transaction class",
        );
      }
      if (
        dataset &&
        account &&
        patch.categoryCode !== undefined &&
        patch.categoryCode
      ) {
        assertCategoryCodeAllowedForAccount(
          dataset,
          account,
          patch.categoryCode,
          "Manual category",
        );
      }
      if (
        dataset &&
        account &&
        patch.economicEntityId !== undefined &&
        patch.economicEntityId
      ) {
        assertEconomicEntityAllowedForAccount(
          dataset,
          account,
          patch.economicEntityId,
          "Manual economic entity",
        );
      }

      const updatePayload: Record<string, unknown> = {
        updated_at: new Date().toISOString(),
      };
      if (patch.transactionClass !== undefined)
        updatePayload.transaction_class = patch.transactionClass;
      if (patch.categoryCode !== undefined)
        updatePayload.category_code = patch.categoryCode;
      if (patch.economicEntityId !== undefined)
        updatePayload.economic_entity_id = patch.economicEntityId;
      if (patch.merchantNormalized !== undefined)
        updatePayload.merchant_normalized = patch.merchantNormalized;
      if (patch.counterpartyName !== undefined)
        updatePayload.counterparty_name = patch.counterpartyName;
      if (patch.needsReview !== undefined)
        updatePayload.needs_review = patch.needsReview;
      if (patch.reviewReason !== undefined)
        updatePayload.review_reason = patch.reviewReason;
      if (patch.excludeFromAnalytics !== undefined)
        updatePayload.exclude_from_analytics = patch.excludeFromAnalytics;
      if (patch.securityId !== undefined)
        updatePayload.security_id = patch.securityId;
      if (patch.quantity !== undefined) updatePayload.quantity = patch.quantity;
      if (patch.unitPriceOriginal !== undefined)
        updatePayload.unit_price_original = patch.unitPriceOriginal;
      if (patch.manualNotes !== undefined)
        updatePayload.manual_notes = patch.manualNotes;
      if (Object.keys(input.patch).length > 0) {
        updatePayload.classification_status = "manual_override";
        updatePayload.classification_source = "manual";
        updatePayload.classification_confidence = 1;
      }

      const after = input.apply
        ? await sql`
            update public.transactions
            set ${sql(updatePayload)}
            where id = ${input.transactionId}
              and user_id = ${this.userId}
            returning ${transactionColumnsSql(sql)}
          `
        : [{ ...beforeRow, ...updatePayload }];

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "transactions.update",
        "transaction",
        input.transactionId,
        beforeRow,
        after[0],
      );

      let generatedRuleId: string | undefined;

      if (input.apply) {
        await insertAuditEventRecord(sql, auditEvent);

        const beforeTransaction = mapFromSql<Transaction>(beforeRow);
        const afterTransaction = mapFromSql<Transaction>(after[0]);
        if (
          beforeTransaction.accountId === afterTransaction.accountId &&
          (beforeTransaction.securityId !== afterTransaction.securityId ||
            beforeTransaction.quantity !== afterTransaction.quantity ||
            beforeTransaction.unitPriceOriginal !==
              afterTransaction.unitPriceOriginal ||
            beforeTransaction.transactionClass !==
              afterTransaction.transactionClass ||
            beforeTransaction.needsReview !== afterTransaction.needsReview ||
            beforeTransaction.excludeFromAnalytics !==
              afterTransaction.excludeFromAnalytics ||
            beforeTransaction.economicEntityId !==
              afterTransaction.economicEntityId)
        ) {
          const accountRows = await sql`
            select asset_domain, account_type from public.accounts
            where id = ${afterTransaction.accountId}
            limit 1
          `;
          const persistedAccount = accountRows[0];
          if (
            persistedAccount?.asset_domain === "investment" ||
            isInvestmentAccountType(persistedAccount?.account_type)
          ) {
            await queueJob(sql, "position_rebuild", {
              accountId: afterTransaction.accountId,
              transactionId: afterTransaction.id,
              trigger: "transaction_update",
            });
          }
        }
        await queueJob(sql, "metric_refresh", {
          trigger: "transaction_update",
          transactionId: afterTransaction.id,
          accountId: afterTransaction.accountId,
        });
        await markTransactionSearchRowsStale(sql, {
          userId: this.userId,
          transactionIds: [afterTransaction.id],
        });
        await queueTransactionSearchIndexJob(sql, {
          userId: this.userId,
          transactionIds: [afterTransaction.id],
          trigger: "transaction_update",
        });

        if (input.createRuleFromTransaction) {
          const persistedTransaction = mapFromSql<Transaction>(after[0]);
          generatedRuleId = randomUUID();
          const persistedAccount =
            account ??
            (await loadDatasetForUser(sql, this.userId)).accounts.find(
              (candidate) => candidate.id === persistedTransaction.accountId,
            ) ??
            null;
          const outputsJson: Record<string, unknown> = {
            transaction_class: persistedTransaction.transactionClass,
            category_code: persistedTransaction.categoryCode,
          };
          if (
            persistedAccount &&
            persistedAccount.assetDomain !== "cash" &&
            persistedTransaction.economicEntityId !== persistedAccount.entityId
          ) {
            outputsJson.economic_entity_id_override =
              persistedTransaction.economicEntityId;
          }
          await sql`
            insert into public.classification_rules (
              id,
              user_id,
              priority,
              active,
              scope_json,
              conditions_json,
              outputs_json,
              created_from_transaction_id,
              auto_generated
            ) values (
              ${generatedRuleId},
              ${this.userId},
              50,
              true,
              ${serializeJson(sql, { account_id: persistedTransaction.accountId })}::jsonb,
              ${serializeJson(sql, {
                normalized_description_regex:
                  persistedTransaction.descriptionClean,
              })}::jsonb,
              ${serializeJson(sql, outputsJson)}::jsonb,
              ${persistedTransaction.id},
              true
            )
          `;
        }
      }

      return {
        applied: input.apply,
        transaction: mapFromSql<Transaction>(after[0]),
        auditEvent,
        generatedRuleId,
      };
    });
  }

  async createRule(input: CreateRuleInput) {
    return withSeededUserContext(async (sql) => {
      const ruleId = randomUUID();
      const dataset = await loadDatasetForUser(sql, this.userId);
      assertRuleOutputsAllowedForScope(
        dataset,
        input.scopeJson,
        input.outputsJson,
      );
      if (input.apply) {
        await sql`
          insert into public.classification_rules (
            id,
            user_id,
            priority,
            active,
            scope_json,
            conditions_json,
            outputs_json,
            auto_generated
          ) values (
            ${ruleId},
            ${this.userId},
            ${input.priority},
            true,
            ${serializeJson(sql, input.scopeJson)}::jsonb,
            ${serializeJson(sql, input.conditionsJson)}::jsonb,
            ${serializeJson(sql, input.outputsJson)}::jsonb,
            false
          )
        `;
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.create",
          "classification_rule",
          ruleId,
          null,
          {
            priority: input.priority,
            scopeJson: input.scopeJson,
            conditionsJson: input.conditionsJson,
            outputsJson: input.outputsJson,
          },
        );
        await insertAuditEventRecord(sql, auditEvent);
      }
      return { applied: input.apply, ruleId };
    });
  }

  async createTemplate(input: CreateTemplateInput) {
    return withSeededUserContext(async (sql) => {
      const templateId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.import_templates (
            id,
            user_id,
            name,
            institution_name,
            compatible_account_type,
            file_kind,
            sheet_name,
            header_row_index,
            rows_to_skip_before_header,
            rows_to_skip_after_header,
            delimiter,
            encoding,
            decimal_separator,
            thousands_separator,
            date_format,
            default_currency,
            column_map_json,
            sign_logic_json,
            normalization_rules_json,
            active,
            version
          ) values (
            ${templateId},
            ${this.userId},
            ${input.template.name},
            ${input.template.institutionName},
            ${input.template.compatibleAccountType},
            ${input.template.fileKind},
            ${input.template.sheetName ?? null},
            ${input.template.headerRowIndex},
            ${input.template.rowsToSkipBeforeHeader},
            ${input.template.rowsToSkipAfterHeader},
            ${input.template.delimiter ?? null},
            ${input.template.encoding ?? null},
            ${input.template.decimalSeparator ?? null},
            ${input.template.thousandsSeparator ?? null},
            ${input.template.dateFormat},
            ${input.template.defaultCurrency},
            ${serializeJson(sql, input.template.columnMapJson)}::jsonb,
            ${serializeJson(sql, input.template.signLogicJson)}::jsonb,
            ${serializeJson(sql, input.template.normalizationRulesJson)}::jsonb,
            ${input.template.active},
            1
          )
        `;
      }
      return { applied: input.apply, templateId };
    });
  }

  async deleteTemplate(input: DeleteTemplateInput) {
    return withSeededUserContext(async (sql) => {
      const before = await sql`
        select * from public.import_templates
        where id = ${input.templateId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = before[0];
      if (!beforeRow) {
        throw new Error(`Template ${input.templateId} not found.`);
      }

      const blockers = await sql`
        with target as (
          select ${input.templateId}::uuid as template_id
        )
        select
          (
            select count(*)::int
            from public.accounts
            where import_template_default_id = target.template_id
              and user_id = ${this.userId}
          ) as default_accounts,
          (
            select count(*)::int
            from public.import_batches
            where template_id = target.template_id
              and user_id = ${this.userId}
          ) as import_batches
        from target
      `;
      assertNoDeleteBlockers(
        blockers[0] as Record<string, unknown>,
        "Template cannot be removed because it already has dependent data:",
      );

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "templates.delete",
        "import_template",
        input.templateId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.import_templates
          where id = ${input.templateId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(sql, auditEvent);
      }

      return { applied: input.apply, templateId: input.templateId };
    });
  }

  async createCategory(input: CreateCategoryInput) {
    return withSeededUserContext(async (sql) => {
      const normalizedCode = input.category.code.trim().toLowerCase();
      const existing = await sql`
        select code
        from public.categories
        where code = ${normalizedCode}
        limit 1
      `;
      if (existing[0]) {
        throw new Error(`Category ${normalizedCode} already exists.`);
      }

      const [sortOrderRow] = await sql`
        select coalesce(max(sort_order), 0)::int as max_sort_order
        from public.categories
      `;
      const nextSortOrder = (sortOrderRow?.max_sort_order ?? 0) + 10;

      if (input.apply) {
        await sql`
          insert into public.categories (
            code,
            display_name,
            parent_code,
            scope_kind,
            direction_kind,
            sort_order,
            active,
            metadata_json
          ) values (
            ${normalizedCode},
            ${input.category.displayName},
            ${input.category.parentCode ?? null},
            ${input.category.scopeKind},
            ${input.category.directionKind},
            ${nextSortOrder},
            true,
            '{}'::jsonb
          )
        `;
      }

      return {
        applied: input.apply,
        categoryCode: normalizedCode,
      };
    });
  }

  async deleteCategory(input: DeleteCategoryInput) {
    return withSeededUserContext(async (sql) => {
      const normalizedCode = input.categoryCode.trim().toLowerCase();
      const existing = await sql`
        select code
        from public.categories
        where code = ${normalizedCode}
        limit 1
      `;
      if (!existing[0]) {
        throw new Error(`Category ${normalizedCode} not found.`);
      }

      const [transactionUsageRow, childCategoryRow] = await Promise.all([
        sql`
          select count(*)::int as usage_count
          from public.transactions
          where category_code = ${normalizedCode}
        `,
        sql`
          select count(*)::int as child_count
          from public.categories
          where parent_code = ${normalizedCode}
        `,
      ]);

      if ((transactionUsageRow[0]?.usage_count ?? 0) > 0) {
        throw new Error(
          `Category ${normalizedCode} is still assigned to transactions and cannot be deleted.`,
        );
      }

      if ((childCategoryRow[0]?.child_count ?? 0) > 0) {
        throw new Error(
          `Category ${normalizedCode} still has child categories and cannot be deleted.`,
        );
      }

      if (input.apply) {
        await sql`
          delete from public.categories
          where code = ${normalizedCode}
        `;
      }

      return {
        applied: input.apply,
        categoryCode: normalizedCode,
      };
    });
  }

  async addOpeningPosition(input: AddOpeningPositionInput) {
    return withSeededUserContext(async (sql) => {
      const adjustmentId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.holding_adjustments ${sql({
            id: adjustmentId,
            user_id: this.userId,
            entity_id: input.entityId,
            account_id: input.accountId,
            security_id: input.securityId,
            effective_date: input.effectiveDate,
            share_delta: input.shareDelta,
            cost_basis_delta_eur: input.costBasisDeltaEur,
            reason: "opening_position",
            note: "Created from app/CLI.",
          })}
        `;
        await queueJob(sql, "position_rebuild", {
          accountId: input.accountId,
          trigger: "opening_position",
        });
      }
      return { applied: input.apply, adjustmentId };
    });
  }

  async deleteHoldingAdjustment(input: DeleteHoldingAdjustmentInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRow = await selectHoldingAdjustmentRowById(
        sql,
        this.userId,
        input.adjustmentId,
      );
      if (!beforeRow) {
        throw new Error(`Holding adjustment ${input.adjustmentId} not found.`);
      }

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "positions.delete-opening",
        "holding_adjustment",
        input.adjustmentId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.holding_adjustments
          where id = ${input.adjustmentId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(sql, auditEvent);
        await queueJob(sql, "position_rebuild", {
          accountId: beforeRow.account_id,
          adjustmentId: input.adjustmentId,
          trigger: "opening_position_delete",
        });
      }

      return { applied: input.apply, adjustmentId: input.adjustmentId };
    });
  }

  async createManualInvestment(input: CreateManualInvestmentInput) {
    return withSeededUserContext(async (sql) => {
      const accountRows = await sql`
        select *
        from public.accounts
        where id = ${input.fundingAccountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const fundingAccount = accountRows[0];
      if (!fundingAccount) {
        throw new Error(
          `Funding account ${input.fundingAccountId} was not found.`,
        );
      }
      if (fundingAccount.entity_id !== input.entityId) {
        throw new Error(
          "The funding account must belong to the same entity as the tracked investment.",
        );
      }
      if (
        fundingAccount.asset_domain !== "cash" &&
        !isInvestmentAccountType(fundingAccount.account_type)
      ) {
        throw new Error(
          "Manual company investments must be linked to a cash-capable account so cost basis can be derived from transfers.",
        );
      }

      const manualInvestmentId = randomUUID();
      const valuationId = randomUUID();
      const nowIso = new Date().toISOString();
      const definitionRow = {
        id: manualInvestmentId,
        user_id: this.userId,
        entity_id: input.entityId,
        funding_account_id: input.fundingAccountId,
        label: input.label,
        matcher_text: input.matcherText,
        note: input.note ?? null,
        created_at: nowIso,
        updated_at: nowIso,
      };
      const valuationRow = {
        id: valuationId,
        user_id: this.userId,
        manual_investment_id: manualInvestmentId,
        snapshot_date: input.snapshotDate,
        current_value_original: input.currentValueOriginal,
        current_value_currency: input.currentValueCurrency,
        note: input.valuationNote ?? null,
        created_at: nowIso,
        updated_at: nowIso,
      };

      if (input.apply) {
        await sql`
          insert into public.manual_investments ${sql(definitionRow as Record<string, unknown>)}
        `;
        await sql`
          insert into public.manual_investment_valuations ${sql(valuationRow as Record<string, unknown>)}
        `;
        await insertAuditEventRecord(
          sql,
          createAuditEvent(
            input.sourceChannel,
            input.actorName,
            "manual_investments.create",
            "manual_investment",
            manualInvestmentId,
            null,
            {
              definition: definitionRow,
              valuation: valuationRow,
            },
          ),
        );
      }

      return { applied: input.apply, manualInvestmentId, valuationId };
    });
  }

  async updateManualInvestment(input: UpdateManualInvestmentInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRow = await selectManualInvestmentRowById(
        sql,
        this.userId,
        input.manualInvestmentId,
      );
      if (!beforeRow) {
        throw new Error(
          `Tracked investment ${input.manualInvestmentId} was not found.`,
        );
      }

      const accountRows = await sql`
        select *
        from public.accounts
        where id = ${input.fundingAccountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const fundingAccount = accountRows[0];
      if (!fundingAccount) {
        throw new Error(
          `Funding account ${input.fundingAccountId} was not found.`,
        );
      }
      if (fundingAccount.entity_id !== beforeRow.entity_id) {
        throw new Error(
          "The funding account must belong to the same entity as the tracked investment.",
        );
      }
      if (
        fundingAccount.asset_domain !== "cash" &&
        !isInvestmentAccountType(fundingAccount.account_type)
      ) {
        throw new Error(
          "Manual company investments must be linked to a cash-capable account so cost basis can be derived from transfers.",
        );
      }

      const nowIso = new Date().toISOString();
      const nextRow = {
        ...beforeRow,
        funding_account_id: input.fundingAccountId,
        label: input.label,
        matcher_text: input.matcherText,
        note: input.note ?? null,
        updated_at: nowIso,
      };

      if (input.apply) {
        await sql`
          update public.manual_investments
          set
            funding_account_id = ${input.fundingAccountId},
            label = ${input.label},
            matcher_text = ${input.matcherText},
            note = ${input.note ?? null},
            updated_at = ${nowIso}
          where id = ${input.manualInvestmentId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(
          sql,
          createAuditEvent(
            input.sourceChannel,
            input.actorName,
            "manual_investments.update",
            "manual_investment",
            input.manualInvestmentId,
            beforeRow,
            nextRow,
          ),
        );
      }

      return {
        applied: input.apply,
        manualInvestmentId: input.manualInvestmentId,
      };
    });
  }

  async recordManualInvestmentValuation(
    input: RecordManualInvestmentValuationInput,
  ) {
    return withSeededUserContext(async (sql) => {
      const manualInvestment = await selectManualInvestmentRowById(
        sql,
        this.userId,
        input.manualInvestmentId,
      );
      if (!manualInvestment) {
        throw new Error(
          `Tracked investment ${input.manualInvestmentId} was not found.`,
        );
      }

      const beforeRows = await sql`
        select *
        from public.manual_investment_valuations
        where manual_investment_id = ${input.manualInvestmentId}
          and snapshot_date = ${input.snapshotDate}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0] ?? null;
      const valuationId = beforeRow?.id ?? randomUUID();
      const nowIso = new Date().toISOString();
      const nextRow = {
        id: valuationId,
        user_id: this.userId,
        manual_investment_id: input.manualInvestmentId,
        snapshot_date: input.snapshotDate,
        current_value_original: input.currentValueOriginal,
        current_value_currency: input.currentValueCurrency,
        note: input.note ?? null,
        created_at: beforeRow?.created_at ?? nowIso,
        updated_at: nowIso,
      };

      if (input.apply) {
        await sql`
          insert into public.manual_investment_valuations ${sql(nextRow as Record<string, unknown>)}
          on conflict (manual_investment_id, snapshot_date)
          do update set
            current_value_original = excluded.current_value_original,
            current_value_currency = excluded.current_value_currency,
            note = excluded.note,
            updated_at = excluded.updated_at
        `;
        await insertAuditEventRecord(
          sql,
          createAuditEvent(
            input.sourceChannel,
            input.actorName,
            beforeRow
              ? "manual_investments.valuation.update"
              : "manual_investments.valuation.create",
            "manual_investment_valuation",
            valuationId,
            beforeRow,
            nextRow,
          ),
        );
      }

      return {
        applied: input.apply,
        manualInvestmentId: input.manualInvestmentId,
        valuationId,
      };
    });
  }

  async deleteManualInvestment(input: DeleteManualInvestmentInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRow = await selectManualInvestmentRowById(
        sql,
        this.userId,
        input.manualInvestmentId,
      );
      if (!beforeRow) {
        throw new Error(
          `Tracked investment ${input.manualInvestmentId} was not found.`,
        );
      }

      const valuationRows = await sql`
        select count(*)::int as valuation_count
        from public.manual_investment_valuations
        where manual_investment_id = ${input.manualInvestmentId}
          and user_id = ${this.userId}
      `;
      const beforeJson = {
        ...beforeRow,
        valuation_count: valuationRows[0]?.valuation_count ?? 0,
      };

      if (input.apply) {
        await sql`
          delete from public.manual_investments
          where id = ${input.manualInvestmentId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(
          sql,
          createAuditEvent(
            input.sourceChannel,
            input.actorName,
            "manual_investments.delete",
            "manual_investment",
            input.manualInvestmentId,
            beforeJson,
            null,
          ),
        );
      }

      return {
        applied: input.apply,
        manualInvestmentId: input.manualInvestmentId,
      };
    });
  }

  async queueRuleDraft(input: QueueRuleDraftInput) {
    return withSeededUserContext(async (sql) => {
      const jobId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.jobs (
            id,
            job_type,
            payload_json,
            status,
            attempts,
            available_at
          ) values (
            ${jobId},
            ${"rule_parse"},
            ${serializeJson(sql, { requestText: input.requestText })}::jsonb,
            ${"queued"},
            0,
            ${new Date().toISOString()}
          )
        `;
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.queue-draft",
          "job",
          jobId,
          null,
          { requestText: input.requestText },
        );
        await insertAuditEventRecord(sql, auditEvent);
      }
      return { applied: input.apply, jobId };
    });
  }

  async applyRuleDraft(input: ApplyRuleDraftInput) {
    return withSeededUserContext(async (sql) => {
      const rows = await sql`
        select * from public.jobs
        where id = ${input.jobId}
          and job_type = 'rule_parse'
        limit 1
      `;
      const job = rows[0];
      if (!job) {
        throw new Error(`Rule draft job ${input.jobId} not found.`);
      }

      const payloadJson = parseJsonColumn<Record<string, unknown>>(
        job.payload_json ?? {},
      );
      const parsedRule =
        payloadJson &&
        typeof payloadJson === "object" &&
        "parsedRule" in payloadJson &&
        typeof payloadJson.parsedRule === "object"
          ? (payloadJson.parsedRule as Record<string, unknown>)
          : null;

      if (!parsedRule) {
        throw new Error("Rule draft has not been parsed yet.");
      }

      const createResult = await this.createRule({
        priority: Number(parsedRule.priority ?? 60),
        scopeJson: (parsedRule.scopeJson ?? {}) as Record<string, unknown>,
        conditionsJson: (parsedRule.conditionsJson ?? {}) as Record<
          string,
          unknown
        >,
        outputsJson: (parsedRule.outputsJson ?? {}) as Record<string, unknown>,
        actorName: input.actorName,
        sourceChannel: input.sourceChannel,
        apply: input.apply,
      });

      if (input.apply) {
        await sql`
          update public.jobs
          set payload_json = ${serializeJson(sql, {
            ...payloadJson,
            appliedRuleId: createResult.ruleId,
          })}::jsonb
          where id = ${input.jobId}
        `;
      }

      return { applied: input.apply, ruleId: createResult.ruleId };
    });
  }

  async previewImport(
    input: ImportExecutionInput,
  ): Promise<ImportPreviewResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    if (!normalizedInput.filePath) {
      throw new Error("A file path is required to preview an import.");
    }

    const dataset = await this.getDataset();
    const rawResult = await runDeterministicImport(
      "preview",
      normalizedInput,
      dataset,
    );
    const prepared = buildImportedTransactions(
      dataset,
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

  async commitImport(input: ImportExecutionInput): Promise<ImportCommitResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    return withSeededUserContext(async (sql) => {
      const dataset = await loadDatasetForUser(sql, this.userId);
      const committed = await commitPreparedImportBatch(sql, {
        userId: this.userId,
        dataset,
        normalizedInput,
        previewFallback: () => this.previewImport(normalizedInput),
      });
      return committed.preview;
    });
  }

  async commitCreditCardStatementImport(
    input: CreditCardStatementImportInput,
  ): Promise<CreditCardStatementImportResult> {
    if (!input.filePath) {
      throw new Error(
        "A file path is required to upload a credit-card statement.",
      );
    }

    return withSeededUserContext(async (sql) => {
      const dataset = await loadDatasetForUser(sql, this.userId);
      const settlementTransaction = dataset.transactions.find(
        (candidate) => candidate.id === input.settlementTransactionId,
      );
      if (!settlementTransaction) {
        throw new Error(
          `Settlement transaction ${input.settlementTransactionId} was not found.`,
        );
      }
      if (!isCreditCardSettlementTransaction(settlementTransaction)) {
        throw new Error(
          "This row is not recognized as a credit-card settlement payment.",
        );
      }
      if (settlementTransaction.creditCardStatementStatus === "uploaded") {
        throw new Error(
          "This settlement row is already linked to a credit-card statement import.",
        );
      }

      const settlementAccount = dataset.accounts.find(
        (candidate) => candidate.id === settlementTransaction.accountId,
      );
      if (!settlementAccount) {
        throw new Error(
          `Settlement account ${settlementTransaction.accountId} was not found.`,
        );
      }

      const linkedCreditCardAccount =
        await resolveOrCreateLinkedCreditCardAccount(sql, {
          userId: this.userId,
          dataset,
          settlementLinkedCreditCardAccountId:
            settlementTransaction.linkedCreditCardAccountId,
          settlementDescriptionRaw: settlementTransaction.descriptionRaw,
          settlementAccount,
          templateId: input.templateId,
          actorName: "web-credit-card-statement",
          sourceChannel: "web",
        });
      const datasetWithLinkedAccount = dataset.accounts.some(
        (candidate) => candidate.id === linkedCreditCardAccount.id,
      )
        ? dataset
        : {
            ...dataset,
            accounts: [...dataset.accounts, linkedCreditCardAccount],
          };

      const normalizedInput = normalizeImportExecutionInput({
        accountId: linkedCreditCardAccount.id,
        templateId: input.templateId,
        originalFilename: input.originalFilename,
        filePath: input.filePath,
      });
      const previewResult = await runDeterministicImport(
        "preview",
        normalizedInput,
        datasetWithLinkedAccount,
      );
      const validationDataset = {
        ...datasetWithLinkedAccount,
        transactions: [],
      } satisfies DomainDataset;
      const validationPrepared = buildImportedTransactions(
        validationDataset,
        normalizedInput,
        "credit-card-statement-validation",
        previewResult.normalizedRows ?? [],
      );
      if (validationPrepared.inserted.length === 0) {
        throw new Error(
          "The uploaded credit-card statement did not produce any transaction rows.",
        );
      }

      const statementNetAmountBaseEur = sumPreparedTransactionAmountBaseEur(
        validationPrepared.inserted,
      );
      if (
        !new Decimal(statementNetAmountBaseEur).eq(
          new Decimal(settlementTransaction.amountBaseEur),
        )
      ) {
        throw new Error(
          `The statement total (${new Decimal(statementNetAmountBaseEur).toFixed(2)} EUR) must exactly match the settlement row (${new Decimal(settlementTransaction.amountBaseEur).toFixed(2)} EUR).`,
        );
      }

      const duplicatePrepared = buildImportedTransactions(
        datasetWithLinkedAccount,
        normalizedInput,
        "credit-card-statement-duplicate-check",
        previewResult.normalizedRows ?? [],
      );
      if (duplicatePrepared.duplicateCount > 0) {
        throw new Error(
          "This statement contains transactions that are already present in the linked credit-card ledger.",
        );
      }

      const committed = await commitPreparedImportBatch(sql, {
        userId: this.userId,
        dataset: datasetWithLinkedAccount,
        normalizedInput,
        options: {
          importedByActor: "web-credit-card-statement",
          importBatchExtraValues: {
            credit_card_settlement_transaction_id: settlementTransaction.id,
            statement_net_amount_base_eur: statementNetAmountBaseEur,
          },
        },
      });

      const settlementMirrorTransactionId = randomUUID();
      const mirrorCreatedAt = new Date().toISOString();
      const mirrorTransaction = {
        id: settlementMirrorTransactionId,
        userId: this.userId,
        accountId: linkedCreditCardAccount.id,
        accountEntityId: linkedCreditCardAccount.entityId,
        economicEntityId: linkedCreditCardAccount.entityId,
        importBatchId: null,
        providerName: null,
        providerRecordId: null,
        sourceFingerprint: `credit-card-settlement-mirror:${settlementTransaction.id}`,
        duplicateKey: `credit-card-settlement-mirror:${settlementTransaction.id}`,
        transactionDate: settlementTransaction.transactionDate,
        postedDate:
          settlementTransaction.postedDate ??
          settlementTransaction.transactionDate,
        amountOriginal: new Decimal(settlementTransaction.amountOriginal)
          .abs()
          .toFixed(8),
        currencyOriginal: settlementTransaction.currencyOriginal,
        amountBaseEur: new Decimal(settlementTransaction.amountBaseEur)
          .abs()
          .toFixed(8),
        fxRateToEur: settlementTransaction.fxRateToEur ?? null,
        descriptionRaw: `Credit card statement payment from ${settlementAccount.displayName}`,
        descriptionClean: normalizeCreditCardSettlementText(
          `Credit card statement payment from ${settlementAccount.displayName}`,
        ),
        merchantNormalized: null,
        counterpartyName: settlementAccount.displayName,
        transactionClass: "transfer_internal",
        categoryCode: null,
        subcategoryCode: null,
        transferGroupId: null,
        relatedAccountId: settlementAccount.id,
        relatedTransactionId: settlementTransaction.id,
        transferMatchStatus: "matched",
        crossEntityFlag: false,
        reimbursementStatus: "none",
        classificationStatus: "transfer_match",
        classificationSource: "transfer_matcher",
        classificationConfidence: "1.00",
        needsReview: false,
        reviewReason: null,
        excludeFromAnalytics: false,
        correctionOfTransactionId: null,
        voidedAt: null,
        manualNotes: null,
        llmPayload: {
          analysisStatus: "skipped",
          explanation:
            "Synthetic settlement mirror for a linked credit-card statement import.",
          model: null,
          error: null,
        },
        rawPayload: {
          creditCardStatementSettlementMirror: true,
          settlementTransactionId: settlementTransaction.id,
          linkedImportBatchId: committed.importBatchId,
        },
        securityId: null,
        quantity: null,
        unitPriceOriginal: null,
        creditCardStatementStatus: "not_applicable",
        linkedCreditCardAccountId: null,
        createdAt: mirrorCreatedAt,
        updatedAt: mirrorCreatedAt,
      } satisfies Transaction;

      await sql`
        insert into public.transactions ${sql({
          id: mirrorTransaction.id,
          user_id: mirrorTransaction.userId,
          account_id: mirrorTransaction.accountId,
          account_entity_id: mirrorTransaction.accountEntityId,
          economic_entity_id: mirrorTransaction.economicEntityId,
          import_batch_id: mirrorTransaction.importBatchId,
          source_fingerprint: mirrorTransaction.sourceFingerprint,
          duplicate_key: mirrorTransaction.duplicateKey,
          transaction_date: mirrorTransaction.transactionDate,
          posted_date: mirrorTransaction.postedDate,
          amount_original: mirrorTransaction.amountOriginal,
          currency_original: mirrorTransaction.currencyOriginal,
          amount_base_eur: mirrorTransaction.amountBaseEur,
          fx_rate_to_eur: mirrorTransaction.fxRateToEur,
          description_raw: mirrorTransaction.descriptionRaw,
          description_clean: mirrorTransaction.descriptionClean,
          merchant_normalized: mirrorTransaction.merchantNormalized,
          counterparty_name: mirrorTransaction.counterpartyName,
          transaction_class: mirrorTransaction.transactionClass,
          category_code: mirrorTransaction.categoryCode,
          subcategory_code: mirrorTransaction.subcategoryCode,
          transfer_group_id: mirrorTransaction.transferGroupId,
          related_account_id: mirrorTransaction.relatedAccountId,
          related_transaction_id: mirrorTransaction.relatedTransactionId,
          transfer_match_status: mirrorTransaction.transferMatchStatus,
          cross_entity_flag: mirrorTransaction.crossEntityFlag,
          reimbursement_status: mirrorTransaction.reimbursementStatus,
          classification_status: mirrorTransaction.classificationStatus,
          classification_source: mirrorTransaction.classificationSource,
          classification_confidence: mirrorTransaction.classificationConfidence,
          needs_review: mirrorTransaction.needsReview,
          review_reason: mirrorTransaction.reviewReason,
          exclude_from_analytics: mirrorTransaction.excludeFromAnalytics,
          correction_of_transaction_id:
            mirrorTransaction.correctionOfTransactionId,
          voided_at: mirrorTransaction.voidedAt,
          manual_notes: mirrorTransaction.manualNotes,
          llm_payload: mirrorTransaction.llmPayload,
          raw_payload: mirrorTransaction.rawPayload,
          security_id: mirrorTransaction.securityId,
          quantity: mirrorTransaction.quantity,
          unit_price_original: mirrorTransaction.unitPriceOriginal,
          credit_card_statement_status:
            mirrorTransaction.creditCardStatementStatus,
          linked_credit_card_account_id:
            mirrorTransaction.linkedCreditCardAccountId,
          created_at: mirrorTransaction.createdAt,
          updated_at: mirrorTransaction.updatedAt,
        } as Record<string, unknown>)}
      `;

      const afterSettlementRow = await updateTransactionRecord(sql, {
        userId: this.userId,
        transactionId: settlementTransaction.id,
        updatePayload: {
          related_account_id: linkedCreditCardAccount.id,
          related_transaction_id: settlementMirrorTransactionId,
          transfer_match_status: "matched",
          needs_review: false,
          review_reason: null,
          credit_card_statement_status: "uploaded",
          linked_credit_card_account_id: linkedCreditCardAccount.id,
          updated_at: new Date().toISOString(),
        },
      });
      if (!afterSettlementRow) {
        throw new Error(
          `Settlement transaction ${settlementTransaction.id} could not be linked.`,
        );
      }
      const afterSettlementTransaction =
        mapFromSql<Transaction>(afterSettlementRow);

      await insertAuditEventRecord(
        sql,
        createAuditEvent(
          "web",
          "web-credit-card-statement",
          "transactions.credit-card-settlement-mirror",
          "transaction",
          mirrorTransaction.id,
          null,
          mirrorTransaction as unknown as Record<string, unknown>,
        ),
      );
      await insertAuditEventRecord(
        sql,
        createAuditEvent(
          "web",
          "web-credit-card-statement",
          "transactions.link-credit-card-statement",
          "transaction",
          settlementTransaction.id,
          settlementTransaction as unknown as Record<string, unknown>,
          afterSettlementTransaction as unknown as Record<string, unknown>,
        ),
      );
      await sql`
        update public.import_batches
        set commit_summary_json =
          coalesce(commit_summary_json, '{}'::jsonb) ||
          ${serializeJson(sql, {
            settlementMirrorTransactionId,
            linkedCreditCardAccountId: linkedCreditCardAccount.id,
          })}::jsonb
        where id = ${committed.importBatchId}
          and user_id = ${this.userId}
      `;
      await queueJob(sql, "metric_refresh", {
        trigger: "credit_card_statement_import",
        settlementTransactionId: settlementTransaction.id,
        accountId: settlementAccount.id,
        linkedCreditCardAccountId: linkedCreditCardAccount.id,
        importBatchId: committed.importBatchId,
      });

      return {
        ...committed.preview,
        settlementTransactionId: settlementTransaction.id,
        linkedCreditCardAccountId: linkedCreditCardAccount.id,
        linkedCreditCardAccountName: linkedCreditCardAccount.displayName,
        settlementMirrorTransactionId,
        statementNetAmountBaseEur,
      };
    });
  }

  async runPendingJobs(apply: boolean): Promise<JobRunResult> {
    return runFinanceJobQueue({
      apply,
      userId: this.userId,
      reanalyzeTransactionReview,
    });
  }
}

export function createFinanceRepository(): FinanceRepository {
  return new SqlFinanceRepository();
}

export { getRevolutRuntimeStatus };
export { createSqlClient, getDbRuntimeConfig } from "./sql-runtime";

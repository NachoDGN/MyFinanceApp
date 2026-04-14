import type { LearnedReviewExample, Transaction } from "@myfinance/domain";
import type { PromptProfileId } from "@myfinance/llm";

import { mapFromSql, readOptionalRecord, readOptionalString } from "./sql-json";
import {
  getDbRuntimeConfig,
  withSeededUserContext,
  type SqlClient,
} from "./sql-runtime";

type AnalyzerPromptProfileId = Extract<
  PromptProfileId,
  "cash_transaction_analyzer" | "investment_transaction_analyzer"
>;

export async function learnedReviewExamplesTableExists(sql: SqlClient) {
  const rows = await sql`
    select to_regclass('public.learned_review_examples') is not null as exists
  `;

  return rows[0]?.exists === true;
}

function buildSourceTransactionSnapshot(transaction: Transaction) {
  return {
    transactionDate: transaction.transactionDate,
    postedDate: transaction.postedDate ?? null,
    amountOriginal: transaction.amountOriginal,
    currencyOriginal: transaction.currencyOriginal,
    descriptionRaw: transaction.descriptionRaw,
    merchantNormalized: transaction.merchantNormalized ?? null,
    counterpartyName: transaction.counterpartyName ?? null,
    securityId: transaction.securityId ?? null,
    quantity: transaction.quantity ?? null,
    unitPriceOriginal: transaction.unitPriceOriginal ?? null,
  } satisfies Record<string, unknown>;
}

function buildInitialInferenceSnapshot(transaction: Transaction) {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);

  return {
    transactionClass: transaction.transactionClass,
    categoryCode: transaction.categoryCode ?? null,
    classificationSource: transaction.classificationSource,
    classificationStatus: transaction.classificationStatus,
    classificationConfidence: transaction.classificationConfidence,
    needsReview: transaction.needsReview,
    reviewReason: transaction.reviewReason ?? null,
    model:
      readOptionalString(llmNode?.model) ??
      readOptionalString(llmPayload?.model) ??
      null,
    explanation:
      readOptionalString(llmNode?.explanation) ??
      readOptionalString(llmPayload?.explanation) ??
      null,
    reason:
      readOptionalString(llmNode?.reason) ??
      readOptionalString(llmPayload?.reason) ??
      null,
  } satisfies Record<string, unknown>;
}

function buildCorrectedOutcomeSnapshot(transaction: Transaction) {
  return {
    transactionClass: transaction.transactionClass,
    categoryCode: transaction.categoryCode ?? null,
    merchantNormalized: transaction.merchantNormalized ?? null,
    counterpartyName: transaction.counterpartyName ?? null,
    securityId: transaction.securityId ?? null,
    quantity: transaction.quantity ?? null,
    unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    needsReview: transaction.needsReview,
    reviewReason: transaction.reviewReason ?? null,
  } satisfies Record<string, unknown>;
}

export function resolveAnalyzerPromptProfileId(assetDomain: "cash" | "investment") {
  return (
    assetDomain === "investment"
      ? "investment_transaction_analyzer"
      : "cash_transaction_analyzer"
  ) satisfies AnalyzerPromptProfileId;
}

export async function upsertLearnedReviewExample(
  sql: SqlClient,
  input: {
    userId: string;
    accountId: string;
    sourceTransaction: Transaction;
    correctedTransaction: Transaction;
    sourceAuditEventId: string | null;
    promptProfileId: AnalyzerPromptProfileId;
    userContext: string;
  },
) {
  if (!(await learnedReviewExamplesTableExists(sql))) {
    return null;
  }

  const rows = await sql`
    insert into public.learned_review_examples (
      user_id,
      account_id,
      source_transaction_id,
      source_audit_event_id,
      prompt_profile_id,
      user_context,
      source_transaction_snapshot_json,
      initial_inference_snapshot_json,
      corrected_outcome_snapshot_json,
      metadata_json,
      active,
      updated_at
    ) values (
      ${input.userId},
      ${input.accountId},
      ${input.sourceTransaction.id},
      ${input.sourceAuditEventId},
      ${input.promptProfileId},
      ${input.userContext},
      ${sql.json(buildSourceTransactionSnapshot(input.sourceTransaction))}::jsonb,
      ${sql.json(buildInitialInferenceSnapshot(input.sourceTransaction))}::jsonb,
      ${sql.json(buildCorrectedOutcomeSnapshot(input.correctedTransaction))}::jsonb,
      ${sql.json({
        sourceImportBatchId: input.sourceTransaction.importBatchId ?? null,
      })}::jsonb,
      ${true},
      ${new Date().toISOString()}
    )
    on conflict (user_id, source_transaction_id)
    do update
    set
      account_id = excluded.account_id,
      source_audit_event_id = excluded.source_audit_event_id,
      prompt_profile_id = excluded.prompt_profile_id,
      user_context = excluded.user_context,
      source_transaction_snapshot_json = excluded.source_transaction_snapshot_json,
      initial_inference_snapshot_json = excluded.initial_inference_snapshot_json,
      corrected_outcome_snapshot_json = excluded.corrected_outcome_snapshot_json,
      metadata_json = excluded.metadata_json,
      active = true,
      updated_at = excluded.updated_at
    returning *
  `;

  return mapFromSql<LearnedReviewExample>(rows[0]);
}

export async function listLearnedReviewExamples(input?: {
  includeInactive?: boolean;
}) {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext(async (sql) => {
    if (!(await learnedReviewExamplesTableExists(sql))) {
      return [];
    }

    const rows = await sql`
      select *
      from public.learned_review_examples
      where user_id = ${userId}
        and (${input?.includeInactive === true} or active = true)
      order by active desc, updated_at desc, created_at desc
    `;

    return mapFromSql<LearnedReviewExample[]>(rows);
  });
}

export async function deactivateLearnedReviewExample(input: {
  learnedReviewExampleId: string;
}) {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext(async (sql) => {
    if (!(await learnedReviewExamplesTableExists(sql))) {
      throw new Error(
        "Learned review examples are unavailable until the database migration is applied.",
      );
    }

    const rows = await sql`
      update public.learned_review_examples
      set active = false,
          updated_at = ${new Date().toISOString()}
      where id = ${input.learnedReviewExampleId}
        and user_id = ${userId}
      returning *
    `;

    if (!rows[0]) {
      throw new Error(
        `Learned review example ${input.learnedReviewExampleId} was not found.`,
      );
    }

    return mapFromSql<LearnedReviewExample>(rows[0]);
  });
}

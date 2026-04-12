import { normalizeSecurityText } from "./text";
import type { Transaction } from "./types";

export type TransactionReviewState =
  | "pending_enrichment"
  | "needs_review"
  | "resolved";

const TRANSACTION_ANALYSIS_STATUSES = [
  "pending",
  "done",
  "failed",
  "skipped",
] as const;

const CREDIT_CARD_STATEMENT_REVIEW_REASON =
  "Upload the matching credit-card statement to resolve category KPIs.";
export const UNCATEGORIZED_TRANSACTION_REVIEW_REASON =
  "Assign a category before this transaction can be treated as resolved.";

type SettlementLikeTransaction = Pick<
  Transaction,
  | "descriptionRaw"
  | "descriptionClean"
  | "creditCardStatementStatus"
  | "transactionClass"
  | "relatedAccountId"
  | "relatedTransactionId"
  | "transferMatchStatus"
>;

export function isCreditCardSettlementText(value: string) {
  const normalizedText = normalizeSecurityText(value);
  return (
    normalizedText.includes("LIQUIDACION") &&
    normalizedText.includes("TARJETAS DE CREDITO")
  );
}

export function getTransactionAnalysisStatus(transaction: {
  llmPayload?: unknown;
}): "pending" | "done" | "failed" | "skipped" | null {
  if (
    !transaction.llmPayload ||
    typeof transaction.llmPayload !== "object" ||
    Array.isArray(transaction.llmPayload)
  ) {
    return null;
  }

  const analysisStatus = (
    transaction.llmPayload as { analysisStatus?: unknown }
  ).analysisStatus;
  return typeof analysisStatus === "string" &&
    TRANSACTION_ANALYSIS_STATUSES.includes(
      analysisStatus as (typeof TRANSACTION_ANALYSIS_STATUSES)[number],
    )
    ? (analysisStatus as "pending" | "done" | "failed" | "skipped")
    : null;
}

export function isTransactionPendingEnrichment(transaction: {
  llmPayload?: unknown;
}) {
  return getTransactionAnalysisStatus(transaction) === "pending";
}

export function isCreditCardSettlementTransaction(
  transaction: Pick<
    SettlementLikeTransaction,
    "creditCardStatementStatus" | "descriptionRaw" | "descriptionClean"
  >,
) {
  if (
    transaction.creditCardStatementStatus === "upload_required" ||
    transaction.creditCardStatementStatus === "uploaded"
  ) {
    return true;
  }

  return isCreditCardSettlementText(
    `${transaction.descriptionRaw} ${transaction.descriptionClean}`,
  );
}

export function isUnmatchedCreditCardSettlementTransaction(
  transaction: SettlementLikeTransaction,
) {
  if (transaction.transactionClass !== "transfer_internal") {
    return false;
  }

  if (
    transaction.relatedAccountId ||
    transaction.relatedTransactionId ||
    transaction.transferMatchStatus === "matched"
  ) {
    return false;
  }

  return isCreditCardSettlementTransaction(transaction);
}

export function needsCreditCardStatementUpload(
  transaction: Pick<
    Transaction,
    "creditCardStatementStatus" | "descriptionRaw" | "descriptionClean"
  >,
) {
  return (
    transaction.creditCardStatementStatus === "upload_required" ||
    (transaction.creditCardStatementStatus === "not_applicable" &&
      isCreditCardSettlementTransaction(transaction))
  );
}

export function isUncategorizedCategoryCode(
  categoryCode: string | null | undefined,
) {
  if (typeof categoryCode !== "string") {
    return false;
  }

  const normalized = categoryCode.trim().toLowerCase();
  return (
    normalized === "uncategorized_expense" ||
    normalized === "uncategorized_income"
  );
}

export function getTransactionReviewReason(
  transaction: Pick<
    Transaction,
    | "reviewReason"
    | "creditCardStatementStatus"
    | "descriptionRaw"
    | "descriptionClean"
  > & { categoryCode?: Transaction["categoryCode"] },
) {
  if (needsCreditCardStatementUpload(transaction)) {
    return CREDIT_CARD_STATEMENT_REVIEW_REASON;
  }
  if (transaction.reviewReason) {
    return transaction.reviewReason;
  }
  if (isUncategorizedCategoryCode(transaction.categoryCode)) {
    return UNCATEGORIZED_TRANSACTION_REVIEW_REASON;
  }
  return null;
}

export function needsTransactionManualReview(
  transaction: Pick<
    Transaction,
    | "needsReview"
    | "creditCardStatementStatus"
    | "descriptionRaw"
    | "descriptionClean"
  > & {
    categoryCode?: Transaction["categoryCode"];
    llmPayload?: unknown;
  },
) {
  return (
    !isTransactionPendingEnrichment(transaction) &&
    (transaction.needsReview === true ||
      isUncategorizedCategoryCode(transaction.categoryCode) ||
      needsCreditCardStatementUpload(transaction))
  );
}

export function getTransactionReviewState(
  transaction: Pick<
    Transaction,
    | "needsReview"
    | "creditCardStatementStatus"
    | "descriptionRaw"
    | "descriptionClean"
  > & {
    categoryCode?: Transaction["categoryCode"];
    llmPayload?: unknown;
  },
): TransactionReviewState {
  if (isTransactionPendingEnrichment(transaction)) {
    return "pending_enrichment";
  }

  return needsTransactionManualReview(transaction)
    ? "needs_review"
    : "resolved";
}

export function isTransactionResolvedForAnalytics(
  transaction: Pick<
    Transaction,
    | "needsReview"
    | "creditCardStatementStatus"
    | "descriptionRaw"
    | "descriptionClean"
  > & {
    categoryCode?: Transaction["categoryCode"];
    excludeFromAnalytics?: boolean;
    voidedAt?: string | null;
    llmPayload?: unknown;
  },
) {
  return (
    !isTransactionPendingEnrichment(transaction) &&
    !needsTransactionManualReview(transaction) &&
    transaction.excludeFromAnalytics !== true &&
    !transaction.voidedAt
  );
}

import {
  type SimilarAccountTransactionPromptContext,
  type TransactionEnrichmentDecision,
} from "@myfinance/classification";
import type { DomainDataset, Transaction } from "@myfinance/domain";

import {
  readTransactionRawOutput,
  readTransactionReviewContext,
  type SimilarUnresolvedTransactionMatch,
} from "./transaction-embedding-search";
import {
  readOptionalRecord,
  readOptionalString,
  readRawOutputField,
  readRawOutputNumberAsString,
  readRawOutputString,
} from "./sql-json";
import type { SqlClient } from "./sql-runtime";

const MAX_PROPAGATED_CONTEXT_ENTRIES = 10;

const REVIEW_PROPAGATION_ANALYTICS_FIELDS = [
  "transactionClass",
  "categoryCode",
  "economicEntityId",
] as const;

const REVIEW_PROPAGATION_INVESTMENT_FIELDS = [
  "transactionClass",
  "securityId",
  "quantity",
  "unitPriceOriginal",
] as const;

export type ResolvedSourcePrecedent = {
  sourceTransactionId: string;
  sourceAuditEventId: string | null;
  sourceDescriptionRaw: string;
  userProvidedContext: string | null;
  finalTransaction: {
    transactionClass: string;
    securityId: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    needsReview: boolean;
    reviewReason: string | null;
  };
  llm: {
    model: string | null;
    explanation: string | null;
    reason: string | null;
    resolutionProcess: string | null;
    rawOutput: Record<string, unknown> | null;
  };
  rebuildEvidence: Record<string, unknown> | null;
};

export type PropagatedContextEntry = {
  kind: "unresolved_source_context" | "resolved_source_precedent";
  sourceTransactionId: string;
  sourceAuditEventId: string | null;
  propagatedAt: string;
  similarity: number;
  sourceDescriptionRaw: string;
  sourceTransactionClass: string | null;
  sourceNeedsReview: boolean;
  sourceReviewReason: string | null;
  userProvidedContext: string | null;
  summaryText: string;
  resolvedPrecedent: Record<string, unknown> | null;
};

function readUnknownArray(value: unknown) {
  return Array.isArray(value) ? value : null;
}

export async function selectReviewPropagationCandidateMatches(input: {
  dataset: DomainDataset;
  account: DomainDataset["accounts"][number];
  sourceTransaction: Transaction;
  embeddingMatches: SimilarUnresolvedTransactionMatch[];
}) {
  if (input.embeddingMatches.length === 0) {
    return [];
  }

  const candidateById = new Map(
    input.dataset.transactions
      .filter((candidate) => candidate.id !== input.sourceTransaction.id)
      .filter((candidate) => candidate.accountId === input.account.id)
      .filter((candidate) => candidate.needsReview)
      .filter((candidate) => !candidate.voidedAt)
      .map((candidate) => [candidate.id, candidate] as const),
  );

  return [...input.embeddingMatches]
    .filter((match) => candidateById.has(match.transactionId))
    .sort((left, right) => {
      if (right.similarity !== left.similarity) {
        return right.similarity - left.similarity;
      }
      const leftCreatedAt =
        candidateById.get(left.transactionId)?.createdAt ?? "";
      const rightCreatedAt =
        candidateById.get(right.transactionId)?.createdAt ?? "";
      return rightCreatedAt.localeCompare(leftCreatedAt);
    });
}

function getTransactionUserProvidedContext(transaction: Transaction) {
  const reviewContext = readTransactionReviewContext(transaction);
  return (
    readOptionalString(reviewContext?.userProvidedContext) ??
    transaction.manualNotes ??
    null
  );
}

export function buildResolvedSourcePrecedent(
  sourceTransaction: Transaction,
  sourceAuditEventId: string | null,
): ResolvedSourcePrecedent {
  const llmPayload = readOptionalRecord(sourceTransaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);

  return {
    sourceTransactionId: sourceTransaction.id,
    sourceAuditEventId,
    sourceDescriptionRaw: sourceTransaction.descriptionRaw,
    userProvidedContext: getTransactionUserProvidedContext(sourceTransaction),
    finalTransaction: {
      transactionClass: sourceTransaction.transactionClass,
      securityId: sourceTransaction.securityId ?? null,
      quantity: sourceTransaction.quantity ?? null,
      unitPriceOriginal: sourceTransaction.unitPriceOriginal ?? null,
      needsReview: sourceTransaction.needsReview,
      reviewReason: sourceTransaction.reviewReason ?? null,
    },
    llm: {
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
      resolutionProcess: readRawOutputString(rawOutput, "resolution_process"),
      rawOutput,
    },
    rebuildEvidence: readOptionalRecord(llmPayload?.rebuildEvidence) ?? null,
  };
}

export function buildResolvedReviewSeedTransaction(
  transaction: Transaction,
  assetDomain: "cash" | "investment",
): Transaction {
  return {
    ...transaction,
    merchantNormalized: null,
    counterpartyName: null,
    transactionClass: "unknown",
    categoryCode:
      assetDomain === "investment" ? "uncategorized_investment" : null,
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Resolved transaction requested manual reanalysis.",
    manualNotes: null,
    llmPayload: null,
    securityId: null,
    quantity: null,
    unitPriceOriginal: null,
  };
}

export function buildResolvedReviewSimilarTransactionContext(
  transaction: Transaction,
  similarity: number,
): SimilarAccountTransactionPromptContext {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);

  return {
    transactionDate: transaction.transactionDate,
    postedDate: transaction.postedDate ?? null,
    amountOriginal: transaction.amountOriginal,
    currencyOriginal: transaction.currencyOriginal,
    descriptionRaw: transaction.descriptionRaw,
    transactionClass: transaction.transactionClass,
    categoryCode: transaction.categoryCode ?? null,
    merchantNormalized: transaction.merchantNormalized ?? null,
    counterpartyName: transaction.counterpartyName ?? null,
    securityId: transaction.securityId ?? null,
    quantity: transaction.quantity ?? null,
    unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    reviewReason: transaction.reviewReason ?? null,
    similarityScore: similarity.toFixed(2),
    userProvidedContext: getTransactionUserProvidedContext(transaction),
    resolvedInstrumentName:
      readRawOutputString(rawOutput, "resolved_instrument_name") ?? null,
    resolvedInstrumentIsin:
      readRawOutputString(rawOutput, "resolved_instrument_isin") ?? null,
    resolvedInstrumentTicker:
      readRawOutputString(rawOutput, "resolved_instrument_ticker") ?? null,
    resolvedInstrumentExchange:
      readRawOutputString(rawOutput, "resolved_instrument_exchange") ?? null,
    currentPrice:
      typeof readRawOutputField(rawOutput, "current_price") === "number"
        ? (readRawOutputField(rawOutput, "current_price") as number)
        : null,
    currentPriceCurrency:
      readRawOutputString(rawOutput, "current_price_currency") ?? null,
    currentPriceTimestamp:
      readRawOutputString(rawOutput, "current_price_timestamp") ?? null,
    currentPriceSource:
      readRawOutputString(rawOutput, "current_price_source") ?? null,
    currentPriceType:
      readRawOutputString(rawOutput, "current_price_type") ?? null,
    resolutionProcess:
      readRawOutputString(rawOutput, "resolution_process") ?? null,
    model:
      readOptionalString(llmNode?.model) ??
      readOptionalString(llmPayload?.model) ??
      null,
  };
}

function buildResolvedSourcePrecedentSummary(
  precedent: ResolvedSourcePrecedent,
) {
  return [
    `A similar transaction in this same account was resolved from "${precedent.sourceDescriptionRaw}".`,
    precedent.userProvidedContext
      ? `User review context: ${precedent.userProvidedContext}.`
      : null,
    `Final class: ${precedent.finalTransaction.transactionClass}.`,
    precedent.finalTransaction.securityId
      ? `Resolved security id: ${precedent.finalTransaction.securityId}.`
      : null,
    precedent.llm.resolutionProcess
      ? `Resolution process: ${precedent.llm.resolutionProcess}.`
      : precedent.llm.reason
        ? `Resolution reason: ${precedent.llm.reason}.`
        : null,
    readOptionalRecord(precedent.rebuildEvidence)
      ?.quantityDerivedFromHistoricalPrice === true
      ? "Quantity was later derived during the rebuild step from a historical price or NAV."
      : null,
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function buildUnresolvedSourceContextSummary(sourceTransaction: Transaction) {
  return [
    `A similar transaction in this same account is still unresolved: "${sourceTransaction.descriptionRaw}".`,
    getTransactionUserProvidedContext(sourceTransaction)
      ? `User review context: ${getTransactionUserProvidedContext(sourceTransaction)}.`
      : null,
    sourceTransaction.reviewReason
      ? `Remaining unresolved reason: ${sourceTransaction.reviewReason}.`
      : "It still remains unresolved after manual review.",
    "Use this as supporting context only when the descriptions appear to refer to the same instrument or event.",
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function normalizePropagatedContextEntry(
  value: unknown,
): PropagatedContextEntry | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const kind =
    record.kind === "unresolved_source_context" ||
    record.kind === "resolved_source_precedent"
      ? record.kind
      : null;
  const sourceTransactionId = readOptionalString(record.sourceTransactionId);
  const propagatedAt = readOptionalString(record.propagatedAt);
  const summaryText = readOptionalString(record.summaryText);
  const sourceDescriptionRaw = readOptionalString(record.sourceDescriptionRaw);
  if (
    !kind ||
    !sourceTransactionId ||
    !propagatedAt ||
    !summaryText ||
    !sourceDescriptionRaw
  ) {
    return null;
  }

  return {
    kind,
    sourceTransactionId,
    sourceAuditEventId: readOptionalString(record.sourceAuditEventId),
    propagatedAt,
    similarity: Number(record.similarity ?? 0),
    sourceDescriptionRaw,
    sourceTransactionClass: readOptionalString(record.sourceTransactionClass),
    sourceNeedsReview: record.sourceNeedsReview === true,
    sourceReviewReason: readOptionalString(record.sourceReviewReason),
    userProvidedContext: readOptionalString(record.userProvidedContext),
    summaryText,
    resolvedPrecedent: readOptionalRecord(record.resolvedPrecedent) ?? null,
  };
}

export function mergePropagatedContextHistory(
  existingEntries: unknown,
  nextEntry: PropagatedContextEntry,
  limit = MAX_PROPAGATED_CONTEXT_ENTRIES,
) {
  const normalizedExisting = (readUnknownArray(existingEntries) ?? [])
    .map((entry) => normalizePropagatedContextEntry(entry))
    .filter((entry): entry is PropagatedContextEntry => Boolean(entry));

  const deduplicatedExisting = normalizedExisting.filter(
    (entry) =>
      !(
        entry.sourceTransactionId === nextEntry.sourceTransactionId &&
        (entry.sourceAuditEventId ?? null) ===
          (nextEntry.sourceAuditEventId ?? null)
      ),
  );

  return [nextEntry, ...deduplicatedExisting].slice(0, limit);
}

export function buildUnresolvedSourcePropagatedContextEntry(input: {
  sourceTransaction: Transaction;
  sourceAuditEventId: string | null;
  similarity: number;
  propagatedAt: string;
}): PropagatedContextEntry {
  return {
    kind: "unresolved_source_context",
    sourceTransactionId: input.sourceTransaction.id,
    sourceAuditEventId: input.sourceAuditEventId,
    propagatedAt: input.propagatedAt,
    similarity: input.similarity,
    sourceDescriptionRaw: input.sourceTransaction.descriptionRaw,
    sourceTransactionClass: input.sourceTransaction.transactionClass ?? null,
    sourceNeedsReview: input.sourceTransaction.needsReview,
    sourceReviewReason: input.sourceTransaction.reviewReason ?? null,
    userProvidedContext: getTransactionUserProvidedContext(
      input.sourceTransaction,
    ),
    summaryText: buildUnresolvedSourceContextSummary(input.sourceTransaction),
    resolvedPrecedent: null,
  };
}

export function buildResolvedSourcePropagatedContextEntry(input: {
  sourceTransaction: Transaction;
  sourceAuditEventId: string | null;
  similarity: number;
  propagatedAt: string;
  precedent: ResolvedSourcePrecedent;
}): PropagatedContextEntry {
  return {
    kind: "resolved_source_precedent",
    sourceTransactionId: input.sourceTransaction.id,
    sourceAuditEventId: input.sourceAuditEventId,
    propagatedAt: input.propagatedAt,
    similarity: input.similarity,
    sourceDescriptionRaw: input.sourceTransaction.descriptionRaw,
    sourceTransactionClass: input.sourceTransaction.transactionClass ?? null,
    sourceNeedsReview: input.sourceTransaction.needsReview,
    sourceReviewReason: input.sourceTransaction.reviewReason ?? null,
    userProvidedContext: getTransactionUserProvidedContext(
      input.sourceTransaction,
    ),
    summaryText: buildResolvedSourcePrecedentSummary(input.precedent),
    resolvedPrecedent: input.precedent as unknown as Record<string, unknown>,
  };
}

export function canSeedReviewPropagationFromTransaction(
  account: { assetDomain: "cash" | "investment" },
  transaction: Pick<
    Transaction,
    "transactionClass" | "needsReview" | "securityId" | "voidedAt"
  >,
) {
  if (transaction.voidedAt || transaction.transactionClass === "unknown") {
    return false;
  }

  if (!transaction.needsReview) {
    return true;
  }

  return (
    account.assetDomain === "investment" && Boolean(transaction.securityId)
  );
}

export function shouldQueueReviewPropagationAfterManualReview(
  _account: { assetDomain: "cash" | "investment" },
  transaction: Pick<Transaction, "needsReview">,
) {
  return transaction.needsReview === true;
}

export function buildReviewPropagationUserContext(
  sourceTransaction: Transaction,
) {
  const rawOutput = readTransactionRawOutput(sourceTransaction);
  const llmPayload = readOptionalRecord(sourceTransaction.llmPayload);
  const rebuildEvidence = readOptionalRecord(llmPayload?.rebuildEvidence);
  const instrumentName = readRawOutputString(
    rawOutput,
    "resolved_instrument_name",
  );
  const instrumentIsin = readRawOutputString(
    rawOutput,
    "resolved_instrument_isin",
  );
  const instrumentTicker = readRawOutputString(
    rawOutput,
    "resolved_instrument_ticker",
  );
  const instrumentExchange = readRawOutputString(
    rawOutput,
    "resolved_instrument_exchange",
  );
  const currentPrice = readRawOutputNumberAsString(rawOutput, "current_price");
  const currentPriceCurrency = readRawOutputString(
    rawOutput,
    "current_price_currency",
  );
  const currentPriceTimestamp = readRawOutputString(
    rawOutput,
    "current_price_timestamp",
  );
  const currentPriceSource = readRawOutputString(
    rawOutput,
    "current_price_source",
  );
  const currentPriceType = readRawOutputString(rawOutput, "current_price_type");
  const resolutionProcess = readRawOutputString(
    rawOutput,
    "resolution_process",
  );

  return [
    "A similar unresolved transaction from this same account was manually re-reviewed and should be used as supporting precedent when the evidence matches.",
    `Source transaction description: ${sourceTransaction.descriptionRaw}.`,
    `Source applied class: ${sourceTransaction.transactionClass}.`,
    sourceTransaction.securityId
      ? `Source mapped security id: ${sourceTransaction.securityId}.`
      : null,
    instrumentName ? `Resolved instrument name: ${instrumentName}.` : null,
    instrumentIsin ? `Resolved instrument ISIN: ${instrumentIsin}.` : null,
    instrumentTicker
      ? `Resolved instrument ticker: ${instrumentTicker}${
          instrumentExchange ? ` on ${instrumentExchange}` : ""
        }.`
      : null,
    currentPrice
      ? `Resolved current ${currentPriceType ?? "price"}: ${currentPrice}${
          currentPriceCurrency ? ` ${currentPriceCurrency}` : ""
        }${currentPriceTimestamp ? ` as of ${currentPriceTimestamp}` : ""}${
          currentPriceSource ? ` from ${currentPriceSource}` : ""
        }.`
      : null,
    resolutionProcess ? `Resolution process: ${resolutionProcess}.` : null,
    rebuildEvidence?.quantityDerivedFromHistoricalPrice === true
      ? "Quantity was later derived from a historical price or NAV during rebuild."
      : null,
    sourceTransaction.reviewReason
      ? `The source transaction may still need review for this remaining reason: ${sourceTransaction.reviewReason}.`
      : null,
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function isInvestmentTradeTransactionClass(transactionClass: string) {
  return (
    transactionClass === "investment_trade_buy" ||
    transactionClass === "investment_trade_sell"
  );
}

export function mergeEnrichmentDecisionWithExistingTransaction(
  existingTransaction: Transaction,
  decision: TransactionEnrichmentDecision,
) {
  if (
    !isInvestmentTradeTransactionClass(decision.transactionClass) ||
    !isInvestmentTradeTransactionClass(existingTransaction.transactionClass) ||
    existingTransaction.transactionClass !== decision.transactionClass ||
    !existingTransaction.securityId ||
    !existingTransaction.quantity ||
    existingTransaction.needsReview
  ) {
    return decision;
  }

  if (
    decision.quantity ||
    decision.unitPriceOriginal ||
    !decision.needsReview
  ) {
    return decision;
  }

  return {
    ...decision,
    quantity: existingTransaction.quantity,
    unitPriceOriginal:
      decision.unitPriceOriginal ??
      existingTransaction.unitPriceOriginal ??
      null,
    needsReview: false,
    reviewReason: null,
  } satisfies TransactionEnrichmentDecision;
}

export async function refreshFinanceAnalyticsArtifacts(sql: SqlClient) {
  await sql`select public.refresh_finance_analytics()`;
}

export function replaceTransactionInDataset(
  dataset: DomainDataset,
  transaction: Transaction,
) {
  const index = dataset.transactions.findIndex(
    (candidate) => candidate.id === transaction.id,
  );
  if (index === -1) {
    return dataset;
  }

  const nextTransactions = [...dataset.transactions];
  nextTransactions[index] = transaction;
  return {
    ...dataset,
    transactions: nextTransactions,
  };
}

function hasTransactionFieldChange(
  before: Transaction,
  after: Transaction,
  fields: readonly (keyof Transaction)[],
) {
  return fields.some((field) => before[field] !== after[field]);
}

function buildInvestmentResolutionSignal(transaction: Transaction) {
  const rawOutput = readTransactionRawOutput(transaction);
  return {
    resolvedInstrumentName:
      readRawOutputString(rawOutput, "resolved_instrument_name") ?? null,
    resolvedInstrumentIsin:
      readRawOutputString(rawOutput, "resolved_instrument_isin") ?? null,
    resolvedInstrumentTicker:
      readRawOutputString(rawOutput, "resolved_instrument_ticker") ?? null,
    resolvedInstrumentExchange:
      readRawOutputString(rawOutput, "resolved_instrument_exchange") ?? null,
    currentPriceType:
      readRawOutputString(rawOutput, "current_price_type") ?? null,
  };
}

export function shouldRunInvestmentRebuildAfterReviewPropagation(
  before: Transaction,
  after: Transaction,
) {
  return (
    hasTransactionFieldChange(
      before,
      after,
      REVIEW_PROPAGATION_INVESTMENT_FIELDS,
    ) ||
    JSON.stringify(buildInvestmentResolutionSignal(before)) !==
      JSON.stringify(buildInvestmentResolutionSignal(after))
  );
}

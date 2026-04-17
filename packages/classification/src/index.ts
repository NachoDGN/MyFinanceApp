import { z } from "zod";

import {
  analyzeBankTransaction,
  createLLMClient,
  isModelConfigured,
  resolveModelProvider,
  type AnalyzeBankTransactionResult,
  type PromptProfileOverrides,
} from "@myfinance/llm";
import type {
  Account,
  ClassificationRule,
  DomainDataset,
  Transaction,
} from "@myfinance/domain";
import {
  buildAllowedCategoriesForAccount,
  buildAllowedTransactionClassesForAccount,
  getAllowedCategoryCodesForAccount,
  isUncategorizedCategoryCode,
  normalizeInvestmentMatchingText,
  resolveConstrainedEconomicEntityId,
  UNCATEGORIZED_TRANSACTION_REVIEW_REASON,
} from "@myfinance/domain";

import {
  applyRuleMatch,
  buildDeterministicClassification,
  extractProviderContext,
  resolveValidEconomicEntityOverride,
} from "./deterministic-classification";
import {
  buildHistoricalReviewExamples,
  buildReviewPromptExamples,
  buildInvestmentPortfolioState,
  buildPersistedInvestmentSecurityMappings,
  inferTradeQuantityFromUnitPrice,
  isTradeTransactionClass,
  parseInvestmentEvent,
  rankSimilarAccountTransactions,
  normalizeTradeQuantity,
  type DeterministicClassification,
} from "./investment-support";
import {
  normalizeOptionalText,
  readOptionalBoolean,
  readOptionalRecord,
  readOptionalString,
  readUnknownArray,
  readRawOutputString,
} from "./utils";

export { normalizeInvestmentMatchingText };
export { rankSimilarAccountTransactions, parseInvestmentEvent };
export {
  rankReviewPropagationTransactions,
  getReviewPropagationEmbeddingModel,
} from "./investment-support";
export {
  applyRuleMatch,
  detectInternalTransfer,
} from "./deterministic-classification";
export type {
  ReviewPropagationTransactionMatch,
  SimilarAccountTransactionMatch,
} from "./investment-support";

const quotaExhaustedTransactionModels = new Set<string>();

export const NON_AI_RULE_SUMMARIES = [
  {
    id: "description_normalization",
    title: "Description normalization",
    summary:
      "Whitespace, repeated spaces, SEPA markers, and card-ending boilerplate are normalized before any rule logic runs.",
    evidence: [
      "trim",
      "collapse spaces",
      "remove SEPA",
      "remove CARD ENDING ####",
    ],
  },
  {
    id: "saved_rule_engine",
    title: "Saved rule engine",
    summary:
      "Active user rules are evaluated by priority and can currently match normalized-description regexes and merchant equality.",
    evidence: [
      "normalized_description_regex",
      "merchant_equals",
      "priority ascending",
    ],
  },
  {
    id: "transfer_matcher",
    title: "Internal transfer matcher",
    summary:
      "Owned-account transfers are detected by opposite sign, near date, similar amount, currency match, and account alias hints.",
    evidence: [
      "3-day window",
      "same currency",
      "opposite sign",
      "matching aliases",
    ],
  },
  {
    id: "investment_parser",
    title: "Investment parser",
    summary:
      "Brokerage rows are deterministically parsed for buy, sell, dividend, interest, fee, and FX conversion patterns before any LLM is used.",
    evidence: [
      "DIVIDEND",
      "INTEREST",
      "FEE",
      "FX",
      "@ quantity",
      "BUY/SELL quantity name",
    ],
  },
  {
    id: "fallback_buckets",
    title: "Fallback buckets",
    summary:
      "When deterministic logic cannot safely decide, the system falls back to unknown or uncategorized codes instead of inventing categories.",
    evidence: [
      "unknown",
      "uncategorized_income",
      "uncategorized_expense",
      "uncategorized_investment",
    ],
  },
] as const;

export const constrainedLlmClassificationSchema = z.object({
  transaction_class: z.string(),
  category_code: z.string(),
  merchant_normalized: z.string().nullable().optional(),
  economic_entity_override: z.string().nullable().optional(),
  security_hint: z.string().nullable().optional(),
  confidence: z.number().min(0).max(1),
  reason: z.string(),
});

export const CLASSIFICATION_PRECEDENCE = [
  "manual_override",
  "user_rule",
  "transfer_matcher",
  "investment_parser",
  "llm",
  "system_fallback",
] as const;

type LlmClassification = {
  analysisStatus: "done" | "failed" | "skipped";
  model: string | null;
  transactionClass: string | null;
  categoryCode: string | null;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  economicEntityId: string | null;
  securityHint: string | null;
  quantity: string | null;
  unitPriceOriginal: string | null;
  confidence: string | null;
  explanation: string | null;
  reason: string | null;
  error: string | null;
  rawOutput: Record<string, unknown> | null;
  requestedAt: string;
  completedAt: string;
  durationMs: number;
  reviewExamplesUsed: Array<{
    auditEventId: string;
    objectId: string;
    createdAt: string;
  }>;
};

export interface TransactionEnrichmentDecision {
  transactionClass: string;
  categoryCode: string | null;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  economicEntityId: string;
  classificationStatus: Transaction["classificationStatus"];
  classificationSource: Transaction["classificationSource"];
  classificationConfidence: string;
  needsReview: boolean;
  reviewReason: string | null;
  securityHint: string | null;
  quantity: string | null;
  unitPriceOriginal: string | null;
  llmPayload: Record<string, unknown>;
}

export interface TransactionReviewContextInput {
  userProvidedContext?: string | null;
  previousReviewReason?: string | null;
  previousUserContext?: string | null;
  previousLlmPayload?: Record<string, unknown> | null;
  propagatedContexts?: unknown[];
  resolvedSourcePrecedent?: unknown | null;
  persistedSecurityMappings?: unknown[];
}

export interface SimilarAccountTransactionPromptContext {
  transactionDate: string;
  postedDate: string | null;
  amountOriginal: string;
  currencyOriginal: string;
  descriptionRaw: string;
  transactionClass: string;
  categoryCode: string | null;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  securityId: string | null;
  quantity: string | null;
  unitPriceOriginal: string | null;
  reviewReason: string | null;
  similarityScore: string;
  userProvidedContext?: string | null;
  resolvedInstrumentName?: string | null;
  resolvedInstrumentIsin?: string | null;
  resolvedInstrumentTicker?: string | null;
  resolvedInstrumentExchange?: string | null;
  currentPrice?: number | null;
  currentPriceCurrency?: string | null;
  currentPriceTimestamp?: string | null;
  currentPriceSource?: string | null;
  currentPriceType?: string | null;
  resolutionProcess?: string | null;
  model?: string | null;
}

export interface TransactionBatchContext {
  phase: "parallel_first_pass" | "sequential_escalation";
  sourceBatchKey?: string | null;
  batchSummary?: string | null;
  retrievalContext?: string | null;
  totalTransactions?: number | null;
  trustedResolvedCount?: number | null;
}

export interface TransactionEnrichmentOptions {
  trigger?:
    | "import_classification"
    | "manual_review_update"
    | "manual_resolved_review"
    | "review_propagation";
  reviewContext?: TransactionReviewContextInput;
  promptOverrides?: PromptProfileOverrides;
  similarAccountTransactions?: SimilarAccountTransactionPromptContext[];
  reviewExamples?: ReturnType<typeof buildHistoricalReviewExamples>;
  batchContext?: TransactionBatchContext | null;
  modelNameOverride?: string | null;
  skipHistoricalReviewExamples?: boolean;
  skipSimilarAccountTransactions?: boolean;
  allowDeterministicLlmSkip?: boolean;
}

type TransactionEnrichmentTrigger = Exclude<
  NonNullable<TransactionEnrichmentOptions["trigger"]>,
  "import_classification"
>;

function buildReviewExamplesUsed(
  reviewExamples: ReturnType<typeof buildHistoricalReviewExamples>,
) {
  return reviewExamples.map((example) => ({
    auditEventId: example.auditEventId,
    objectId: example.objectId,
    createdAt: example.createdAt,
  }));
}

function buildTransactionReviewContext(
  transaction: Transaction,
  persistedSecurityMappings: unknown[],
  options?: TransactionEnrichmentOptions,
) {
  const existingReviewContext = readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.reviewContext,
  );

  return {
    trigger: options?.trigger ?? "import_classification",
    previousReviewReason:
      options?.reviewContext?.previousReviewReason ??
      transaction.reviewReason ??
      null,
    previousUserContext:
      options?.reviewContext?.previousUserContext ??
      transaction.manualNotes ??
      null,
    userProvidedContext: options?.reviewContext?.userProvidedContext ?? null,
    previousLlmPayload:
      options?.reviewContext?.previousLlmPayload ??
      (transaction.llmPayload as Record<string, unknown> | null | undefined) ??
      null,
    propagatedContexts:
      options?.reviewContext?.propagatedContexts ??
      readUnknownArray(existingReviewContext?.propagatedContexts) ??
      [],
    persistedSecurityMappings:
      options?.reviewContext?.persistedSecurityMappings ??
      persistedSecurityMappings,
    resolvedSourcePrecedent:
      options?.reviewContext?.resolvedSourcePrecedent ??
      existingReviewContext?.resolvedSourcePrecedent ??
      null,
  };
}

export function getTransactionClassifierConfig() {
  const defaultModel =
    process.env.LLM_TRANSACTION_MODEL ??
    process.env.GEMINI_TRANSACTION_MODEL ??
    process.env.OPENAI_TRANSACTION_MODEL ??
    "gemini-3-flash-preview";
  return {
    model: defaultModel,
    lowConfidenceCutoff: Number(
      process.env.LLM_TRANSACTION_LOW_CONFIDENCE ??
        process.env.GEMINI_TRANSACTION_LOW_CONFIDENCE ??
        process.env.OPENAI_TRANSACTION_LOW_CONFIDENCE ??
        "0.70",
    ),
  };
}

function getResolvedTransactionReviewModel() {
  return process.env.RESOLVED_TRANSACTION_REVIEW_LLM?.trim() || "gpt-5.4-mini";
}

export function getBatchEscalationReviewModel() {
  return (
    process.env.BATCH_TRANSACTION_ESCALATION_LLM?.trim() ||
    "gemini-3-flash-preview"
  );
}

function normalizeCashCategoryCode(
  transactionClass: string,
  categoryCode: string | null,
  allowedCategoryCodes: Set<string>,
) {
  if (
    ["loan_inflow", "loan_principal_payment", "loan_interest_payment"].includes(
      transactionClass,
    ) &&
    allowedCategoryCodes.has("debt") &&
    (categoryCode === null || isUncategorizedCategoryCode(categoryCode))
  ) {
    return "debt";
  }

  return categoryCode;
}

export function getInvestmentTransactionClassifierConfig(
  trigger?: TransactionEnrichmentOptions["trigger"],
) {
  const base = getTransactionClassifierConfig();
  const model =
    trigger === "manual_resolved_review"
      ? getResolvedTransactionReviewModel()
      : isFollowupInvestmentReviewTrigger(trigger)
        ? process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM?.trim() ||
          process.env.INVESTMENT_TRANSACTION_MANUAL_REVIEW_LLM?.trim() ||
          "gemini-3-flash-preview"
        : (process.env.INVESTMENT_TRANSACTION_REVIEW_LLM ?? "gpt-5.4-mini");
  return {
    ...base,
    model,
  };
}

function isFollowupInvestmentReviewTrigger(
  trigger?: TransactionEnrichmentOptions["trigger"],
): trigger is Exclude<TransactionEnrichmentTrigger, "import_classification"> {
  return trigger === "manual_review_update" || trigger === "review_propagation";
}

function getInvestmentReviewModel(
  trigger?: TransactionEnrichmentOptions["trigger"],
) {
  return getInvestmentTransactionClassifierConfig(trigger).model;
}

function getTransactionReviewModel(
  account: Account,
  trigger?: TransactionEnrichmentOptions["trigger"],
) {
  if (trigger === "manual_resolved_review") {
    return account.assetDomain === "investment"
      ? getResolvedTransactionReviewModel()
      : getTransactionClassifierConfig().model;
  }

  return account.assetDomain === "investment"
    ? getInvestmentReviewModel(trigger)
    : getTransactionClassifierConfig().model;
}

function buildSkippedLlmClassification(
  requestedAt: string,
  reason: string,
  reviewExamplesUsed: Array<{
    auditEventId: string;
    objectId: string;
    createdAt: string;
  }>,
): LlmClassification {
  const completedAt = new Date().toISOString();
  return {
    analysisStatus: "skipped",
    model: null,
    transactionClass: null,
    categoryCode: null,
    merchantNormalized: null,
    counterpartyName: null,
    economicEntityId: null,
    securityHint: null,
    quantity: null,
    unitPriceOriginal: null,
    confidence: null,
    explanation: null,
    reason,
    error: reason,
    rawOutput: null,
    requestedAt,
    completedAt,
    durationMs:
      new Date(completedAt).getTime() - new Date(requestedAt).getTime(),
    reviewExamplesUsed,
  };
}

function getOpenAiTransactionFallbackModel(
  account: Account,
  trigger: TransactionEnrichmentOptions["trigger"] | undefined,
  primaryModel: string,
) {
  const explicitOpenAiModel = process.env.OPENAI_TRANSACTION_MODEL?.trim();
  const fallbackModel =
    account.assetDomain === "investment"
      ? getInvestmentReviewModel(trigger)
      : explicitOpenAiModel || "gpt-5.4-mini";

  if (!fallbackModel || !isModelConfigured(fallbackModel)) {
    return null;
  }

  if (resolveModelProvider(fallbackModel) !== "openai") {
    return null;
  }

  if (fallbackModel.trim().toLowerCase() === primaryModel.trim().toLowerCase()) {
    return null;
  }

  return fallbackModel;
}

function shouldRetryTransactionClassificationWithOpenAiFallback(
  result: AnalyzeBankTransactionResult,
  primaryModel: string,
  fallbackModel: string | null,
) {
  if (result.analysisStatus !== "failed") {
    return false;
  }

  if (!fallbackModel) {
    return false;
  }

  if (resolveModelProvider(primaryModel) !== "gemini") {
    return false;
  }

  if (result.provider !== "gemini") {
    return false;
  }

  if (result.statusCode === 429) {
    return true;
  }

  return /RESOURCE_EXHAUSTED|spending cap|quota/i.test(result.error ?? "");
}

function normalizeModelKey(modelName: string) {
  return modelName.trim().toLowerCase();
}

function markTransactionModelQuotaExhausted(modelName: string) {
  quotaExhaustedTransactionModels.add(normalizeModelKey(modelName));
}

function isTransactionModelQuotaExhausted(modelName: string) {
  return quotaExhaustedTransactionModels.has(normalizeModelKey(modelName));
}

export function isTransactionClassifierConfigured() {
  return isModelConfigured(getTransactionClassifierConfig().model);
}

export function isInvestmentTransactionClassifierConfigured() {
  return isModelConfigured(getInvestmentTransactionClassifierConfig().model);
}

async function requestLlmClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
  options?: TransactionEnrichmentOptions,
): Promise<LlmClassification> {
  const persistedSecurityMappings = buildPersistedInvestmentSecurityMappings(
    dataset,
    account,
    transaction,
    deterministic,
  );
  const model =
    options?.modelNameOverride?.trim() ||
    getTransactionReviewModel(account, options?.trigger);
  const providerContext = extractProviderContext(transaction);
  const providerMerchantName = readOptionalString(
    readOptionalRecord(providerContext?.merchant)?.name,
  );
  const allowedTransactionClasses =
    buildAllowedTransactionClassesForAccount(account);
  const allowedCategories = buildAllowedCategoriesForAccount(dataset, account);
  const reviewExamples =
    options?.reviewExamples ??
    (options?.skipHistoricalReviewExamples
      ? []
      : buildReviewPromptExamples(dataset, account, transaction));
  const reviewExamplesUsed = buildReviewExamplesUsed(reviewExamples);
  const reviewContext = buildTransactionReviewContext(
    transaction,
    persistedSecurityMappings,
    options,
  );
  const requestedAt = new Date().toISOString();
  if (
    options?.allowDeterministicLlmSkip !== false &&
    !deterministic.needsReview &&
    (deterministic.classificationSource === "user_rule" ||
      deterministic.classificationSource === "transfer_matcher")
  ) {
    return buildSkippedLlmClassification(
      requestedAt,
      "Skipped LLM because deterministic classification is already trusted.",
      reviewExamplesUsed,
    );
  }
  const similarAccountTransactions =
    options?.similarAccountTransactions ??
    (options?.skipSimilarAccountTransactions
      ? []
      : rankSimilarAccountTransactions(dataset, account, transaction, {
          limit: 5,
          minScore: 6,
          includeNeedsReview: false,
          requireEarlierDate: true,
        }).map((match) => ({
          transactionDate: match.transaction.transactionDate,
          postedDate: match.transaction.postedDate ?? null,
          amountOriginal: match.transaction.amountOriginal,
          currencyOriginal: match.transaction.currencyOriginal,
          descriptionRaw: match.transaction.descriptionRaw,
          transactionClass: match.transaction.transactionClass,
          categoryCode: match.transaction.categoryCode ?? null,
          merchantNormalized: match.transaction.merchantNormalized ?? null,
          counterpartyName: match.transaction.counterpartyName ?? null,
          securityId: match.transaction.securityId ?? null,
          quantity: match.transaction.quantity ?? null,
          unitPriceOriginal: match.transaction.unitPriceOriginal ?? null,
          reviewReason: match.transaction.reviewReason ?? null,
          similarityScore: match.score.toFixed(2),
        })));
  if (!isModelConfigured(model)) {
    const completedAt = new Date().toISOString();
    return {
      analysisStatus: "skipped",
      model: null,
      transactionClass: null,
      categoryCode: null,
      merchantNormalized: null,
      counterpartyName: null,
      economicEntityId: null,
      securityHint: null,
      quantity: null,
      unitPriceOriginal: null,
      confidence: null,
      explanation: null,
      reason: null,
      error: `LLM credentials are not configured for model ${model}.`,
      rawOutput: null,
      requestedAt,
      completedAt,
      durationMs:
        new Date(completedAt).getTime() - new Date(requestedAt).getTime(),
      reviewExamplesUsed,
    };
  }

  const llmClient = createLLMClient();
  const analyzerInput = {
    account: {
      id: account.id,
      assetDomain: account.assetDomain,
      institutionName: account.institutionName,
      displayName: account.displayName,
      accountType: account.accountType,
    },
    allowedTransactionClasses,
    allowedCategories: allowedCategories.map((category) => ({
      code: category.code,
      displayName: category.displayName,
    })),
    transaction: {
      transactionDate: transaction.transactionDate,
      postedDate: transaction.postedDate ?? null,
      amountOriginal: transaction.amountOriginal,
      currencyOriginal: transaction.currencyOriginal,
      descriptionRaw: transaction.descriptionRaw,
      merchantNormalized:
        transaction.merchantNormalized ?? providerMerchantName ?? null,
      counterpartyName: transaction.counterpartyName ?? null,
      securityId: transaction.securityId ?? null,
      quantity: transaction.quantity ?? null,
      unitPriceOriginal: transaction.unitPriceOriginal ?? null,
      providerContext,
      rawPayload: transaction.rawPayload,
    },
    deterministicHint: {
      transactionClass: deterministic.transactionClass,
      categoryCode: deterministic.categoryCode,
      explanation: deterministic.explanation,
      source: deterministic.classificationSource,
    },
    portfolioState: buildInvestmentPortfolioState(
      dataset,
      account,
      transaction,
      deterministic,
    ),
    similarAccountTransactions,
    batchContext: options?.batchContext ?? null,
    reviewExamples: reviewExamples.map((example) => ({
      transaction: example.transaction,
      initialInference: example.initialInference,
      userFeedback: example.userFeedback,
      correctedOutcome: example.correctedOutcome,
    })),
    promptOverrides:
      options?.promptOverrides?.[
        account.assetDomain === "investment"
          ? "investment_transaction_analyzer"
          : "cash_transaction_analyzer"
      ] ?? null,
    reviewContext,
  } as const;

  const fallbackModel = getOpenAiTransactionFallbackModel(
    account,
    options?.trigger,
    model,
  );
  const initialModel =
    fallbackModel &&
    resolveModelProvider(model) === "gemini" &&
    isTransactionModelQuotaExhausted(model)
      ? fallbackModel
      : model;

  let result = await analyzeBankTransaction(
    llmClient,
    analyzerInput,
    initialModel,
  );
  if (
    shouldRetryTransactionClassificationWithOpenAiFallback(
      result,
      model,
      fallbackModel,
    )
  ) {
    markTransactionModelQuotaExhausted(model);
    result = await analyzeBankTransaction(
      llmClient,
      analyzerInput,
      fallbackModel!,
    );
  }
  const completedAt = new Date().toISOString();
  const durationMs =
    new Date(completedAt).getTime() - new Date(requestedAt).getTime();

  if (result.analysisStatus !== "done" || !result.output) {
    return {
      analysisStatus: "failed",
      model: result.model,
      transactionClass: null,
      categoryCode: null,
      merchantNormalized: null,
      counterpartyName: null,
      economicEntityId: null,
      securityHint: null,
      quantity: null,
      unitPriceOriginal: null,
      confidence: null,
      explanation: null,
      reason: null,
      error: result.error ?? "Unknown LLM classification failure.",
      rawOutput: result.rawOutput,
      requestedAt,
      completedAt,
      durationMs,
      reviewExamplesUsed,
    };
  }

  return {
    analysisStatus: "done",
    model: result.model,
    transactionClass: result.output.transaction_class,
    categoryCode: normalizeOptionalText(result.output.category_code ?? null),
    merchantNormalized: normalizeOptionalText(
      result.output.merchant_normalized ?? null,
    ),
    counterpartyName: normalizeOptionalText(
      result.output.counterparty_name ?? null,
    ),
    economicEntityId: resolveValidEconomicEntityOverride(
      dataset,
      result.output.economic_entity_override ?? null,
    ),
    securityHint: normalizeOptionalText(result.output.security_hint ?? null),
    quantity: normalizeOptionalText(result.output.quantity ?? null),
    unitPriceOriginal: normalizeOptionalText(
      result.output.unit_price_original ?? null,
    ),
    confidence: result.output.confidence.toFixed(2),
    explanation: result.output.explanation,
    reason: result.output.reason,
    error: null,
    rawOutput: result.rawOutput,
    requestedAt,
    completedAt,
    durationMs,
    reviewExamplesUsed,
  };
}

export async function enrichImportedTransaction(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options?: TransactionEnrichmentOptions,
): Promise<TransactionEnrichmentDecision> {
  const providerContext = extractProviderContext(transaction);
  const deterministic = buildDeterministicClassification(
    dataset,
    account,
    transaction,
  );
  const persistedSecurityMappings = buildPersistedInvestmentSecurityMappings(
    dataset,
    account,
    transaction,
    deterministic,
  );
  const reviewContext = buildTransactionReviewContext(
    transaction,
    persistedSecurityMappings,
    options,
  );
  const llm = await requestLlmClassification(
    dataset,
    account,
    transaction,
    deterministic,
    options,
  );
  const { lowConfidenceCutoff } = getTransactionClassifierConfig();
  const allowedCategoryCodes = getAllowedCategoryCodesForAccount(
    dataset,
    account,
  );
  const allowedClassSet = new Set<string>(
    buildAllowedTransactionClassesForAccount(account),
  );

  let transactionClass = deterministic.transactionClass;
  let categoryCode = deterministic.categoryCode;
  let merchantNormalized = deterministic.merchantNormalized;
  let counterpartyName = deterministic.counterpartyName;
  let economicEntityId = deterministic.economicEntityId;
  let classificationStatus = deterministic.classificationStatus;
  let classificationSource = deterministic.classificationSource;
  let classificationConfidence = deterministic.classificationConfidence;
  let needsReview = deterministic.needsReview;
  let reviewReason = deterministic.reviewReason;
  let securityHint = deterministic.securityHint;
  let quantity = deterministic.quantity;
  let unitPriceOriginal = deterministic.unitPriceOriginal;
  const reviewCanBeResolvedByLlm =
    !deterministic.needsReview ||
    deterministic.reviewReason === "Needs LLM enrichment.";
  const llmConfidence = Number(llm.confidence ?? "0") || 0;

  if (llm.analysisStatus === "done") {
    const llmTransactionClass =
      llm.transactionClass && allowedClassSet.has(llm.transactionClass)
        ? llm.transactionClass
        : deterministic.transactionClass;
    const llmCategoryCode =
      llm.categoryCode && allowedCategoryCodes.has(llm.categoryCode)
        ? llm.categoryCode
        : deterministic.categoryCode;

    const deterministicWins =
      !deterministic.needsReview &&
      (new Set(["user_rule", "transfer_matcher"]).has(
        deterministic.classificationSource,
      ) ||
        deterministic.classificationStatus === "rule");
    const shouldApplyLlm =
      !deterministicWins &&
      (deterministic.classificationSource !== "investment_parser" ||
        llmConfidence >= lowConfidenceCutoff);
    const llmUnitPriceOriginal = normalizeOptionalText(llm.unitPriceOriginal);
    const llmQuantity =
      normalizeTradeQuantity(llmTransactionClass, llm.quantity) ??
      inferTradeQuantityFromUnitPrice(
        llmTransactionClass,
        transaction.amountOriginal,
        llmUnitPriceOriginal,
      );

    if (shouldApplyLlm) {
      transactionClass = llmTransactionClass;
      categoryCode = llmCategoryCode;
      economicEntityId = resolveConstrainedEconomicEntityId(
        dataset,
        account,
        llm.economicEntityId,
        deterministic.economicEntityId,
      );
      if (isTradeTransactionClass(llmTransactionClass)) {
        quantity = llmQuantity ?? deterministic.quantity;
        unitPriceOriginal =
          llmUnitPriceOriginal ?? deterministic.unitPriceOriginal;
      }
      classificationStatus = "llm";
      classificationSource = "llm";
      classificationConfidence =
        llm.confidence ?? deterministic.classificationConfidence;
    } else if (
      deterministic.classificationSource === "investment_parser" &&
      llmTransactionClass !== deterministic.transactionClass
    ) {
      needsReview = true;
      reviewReason = `LLM suggested ${llmTransactionClass} at ${llmConfidence.toFixed(2)} confidence, but the deterministic parser kept ${deterministic.transactionClass}.`;
    }

    merchantNormalized =
      llm.merchantNormalized ?? deterministic.merchantNormalized;
    counterpartyName = llm.counterpartyName ?? deterministic.counterpartyName;
    securityHint = llm.securityHint ?? deterministic.securityHint;

    if (llmConfidence < lowConfidenceCutoff) {
      needsReview = true;
      reviewReason = `Low-confidence ${transactionClass} classification.`;
    } else if (transactionClass !== "unknown" && reviewCanBeResolvedByLlm) {
      needsReview = false;
      reviewReason = null;
    }

    if (transactionClass === "unknown") {
      needsReview = true;
      reviewReason = llm.reason ?? "The transaction still needs review.";
    }
  } else if (transactionClass === "unknown") {
    needsReview = true;
    reviewReason = llm.error ?? deterministic.reviewReason;
  }

  if (
    account.assetDomain === "investment" &&
    !transaction.securityId &&
    securityHint &&
    !reviewReason
  ) {
    needsReview = true;
    reviewReason = `Parsed investment trade for "${securityHint}", but the system has not matched it to a tracked security yet.`;
  }

  if (isTradeTransactionClass(transactionClass)) {
    quantity = normalizeTradeQuantity(transactionClass, quantity);
  } else {
    quantity = null;
    unitPriceOriginal = null;
  }

  if (
    account.assetDomain === "investment" &&
    isTradeTransactionClass(transactionClass) &&
    !quantity
  ) {
    needsReview = true;
    reviewReason = reviewReason ?? "Quantity still needs to be derived.";
  }

  categoryCode = normalizeCashCategoryCode(
    transactionClass,
    categoryCode,
    allowedCategoryCodes,
  );

  if (isUncategorizedCategoryCode(categoryCode)) {
    needsReview = true;
    reviewReason = reviewReason ?? UNCATEGORIZED_TRANSACTION_REVIEW_REASON;
  }

  return {
    transactionClass,
    categoryCode,
    merchantNormalized,
    counterpartyName,
    economicEntityId,
    classificationStatus,
    classificationSource,
    classificationConfidence,
    needsReview,
    reviewReason,
    securityHint,
    quantity,
    unitPriceOriginal,
    llmPayload: {
      analysisStatus: llm.analysisStatus,
      model:
        llm.model ??
        options?.modelNameOverride?.trim() ??
        (account.assetDomain === "investment"
          ? getInvestmentReviewModel(options?.trigger)
          : getTransactionClassifierConfig().model),
      explanation: llm.explanation ?? deterministic.explanation,
      reason: llm.reason ?? deterministic.explanation,
      confidence: llm.confidence ?? deterministic.classificationConfidence,
      deterministic,
      llm,
      providerContext,
      reviewContext,
      reviewExamplesUsed: llm.reviewExamplesUsed,
      timing: {
        requestedAt: llm.requestedAt,
        completedAt: llm.completedAt,
        durationMs: llm.durationMs,
      },
      batchPipeline: options?.batchContext
        ? {
            phase: options.batchContext.phase,
            sourceBatchKey: options.batchContext.sourceBatchKey ?? null,
            totalTransactions: options.batchContext.totalTransactions ?? null,
            trustedResolvedCount:
              options.batchContext.trustedResolvedCount ?? null,
            processedAt: llm.completedAt,
          }
        : null,
      applied: {
        transactionClass,
        categoryCode,
        merchantNormalized,
        counterpartyName,
        economicEntityId,
        classificationStatus,
        classificationSource,
        classificationConfidence,
        needsReview,
        reviewReason,
        securityHint,
        quantity,
        unitPriceOriginal,
      },
      analyzedAt: llm.completedAt,
    },
  };
}

import { z } from "zod";

import {
  analyzeBankTransaction,
  createLLMClient,
  isModelConfigured,
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
  normalizeDescription,
  normalizeInvestmentMatchingText,
  resolveConstrainedEconomicEntityId,
} from "@myfinance/domain";

import {
  buildHistoricalReviewExamples,
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
export { rankReviewPropagationTransactions, getReviewPropagationEmbeddingModel } from "./investment-support";
export type {
  ReviewPropagationTransactionMatch,
  SimilarAccountTransactionMatch,
} from "./investment-support";

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

export function applyRuleMatch(
  transaction: Transaction,
  rules: ClassificationRule[],
): ClassificationRule | null {
  const ordered = [...rules]
    .filter((rule) => rule.active)
    .sort((a, b) => a.priority - b.priority);

  const comparison = normalizeDescription(
    transaction.descriptionRaw,
  ).comparison;

  for (const rule of ordered) {
    const scopeAccountId =
      typeof rule.scopeJson.account_id === "string"
        ? rule.scopeJson.account_id
        : null;
    const scopeEntityId =
      typeof rule.scopeJson.entity_id === "string"
        ? rule.scopeJson.entity_id
        : null;
    if (scopeAccountId && scopeAccountId !== transaction.accountId) {
      continue;
    }
    if (
      scopeEntityId &&
      scopeEntityId !== transaction.economicEntityId &&
      scopeEntityId !== transaction.accountEntityId
    ) {
      continue;
    }

    const regex = rule.conditionsJson.normalized_description_regex;
    const merchant = rule.conditionsJson.merchant_equals;

    if (typeof regex === "string" && new RegExp(regex).test(comparison)) {
      return rule;
    }

    if (
      typeof merchant === "string" &&
      transaction.merchantNormalized?.toUpperCase() === merchant.toUpperCase()
    ) {
      return rule;
    }
  }

  return null;
}

export function detectInternalTransfer(
  transaction: Transaction,
  candidateRows: Transaction[],
  ownedAccounts: Account[],
  dayWindow = 3,
): Transaction | null {
  if (!ownedAccounts.some((account) => account.id === transaction.accountId)) {
    return null;
  }

  return (
    candidateRows.find((candidate) => {
      if (candidate.id === transaction.id) return false;
      const dateDelta =
        Math.abs(
          (Date.parse(`${candidate.transactionDate}T00:00:00Z`) -
            Date.parse(`${transaction.transactionDate}T00:00:00Z`)) /
            86400000,
        ) <= dayWindow;
      const oppositeSign =
        Number(transaction.amountBaseEur) * Number(candidate.amountBaseEur) < 0;
      const sameMagnitude =
        Math.abs(
          Math.abs(Number(transaction.amountBaseEur)) -
            Math.abs(Number(candidate.amountBaseEur)),
        ) < 0.01;
      const sameCurrency =
        candidate.currencyOriginal === transaction.currencyOriginal;
      const aliasHint = ownedAccounts.some(
        (account) =>
          account.id === candidate.accountId &&
          account.matchingAliases.some((alias) =>
            normalizeDescription(
              transaction.descriptionRaw,
            ).comparison.includes(alias.toUpperCase()),
          ),
      );
      return (
        dateDelta && oppositeSign && sameMagnitude && sameCurrency && aliasHint
      );
    }) ?? null
  );
}

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

export interface TransactionEnrichmentOptions {
  trigger?:
    | "import_classification"
    | "manual_review_update"
    | "manual_resolved_review"
    | "review_propagation";
  reviewContext?: TransactionReviewContextInput;
  promptOverrides?: PromptProfileOverrides;
  similarAccountTransactions?: SimilarAccountTransactionPromptContext[];
}

type TransactionEnrichmentTrigger = Exclude<
  NonNullable<TransactionEnrichmentOptions["trigger"]>,
  "import_classification"
>;

function resolveValidEconomicEntityOverride(
  dataset: DomainDataset,
  value: string | null | undefined,
) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) {
    return null;
  }

  return dataset.entities.some((entity) => entity.id === normalized)
    ? normalized
    : null;
}

function getFallbackCategory(transaction: Transaction, account: Account) {
  if (account.assetDomain === "investment") {
    return "uncategorized_investment";
  }
  return transaction.categoryCode ?? null;
}

function extractProviderContext(transaction: Transaction) {
  const rawPayload = readOptionalRecord(transaction.rawPayload);
  return (
    readOptionalRecord(rawPayload?.providerContext) ??
    readOptionalRecord(rawPayload?.provider_context) ??
    readOptionalRecord(rawPayload?.ProviderContext) ??
    null
  );
}

function buildRevolutDeterministicClassification(
  transaction: Transaction,
): DeterministicClassification | null {
  if (transaction.providerName !== "revolut_business") {
    return null;
  }

  const providerContext = extractProviderContext(transaction);
  if (!providerContext) {
    return null;
  }

  const revolutTransaction = readOptionalRecord(providerContext.transaction);
  const merchant = readOptionalRecord(providerContext.merchant);
  const revolutType = readOptionalString(revolutTransaction?.type);
  if (!revolutType) {
    return null;
  }

  const merchantName = readOptionalString(merchant?.name);
  const refundTypes = new Set([
    "refund",
    "card_refund",
    "charge_refund",
    "tax_refund",
  ]);

  if (revolutType === "exchange") {
    return {
      transactionClass: "fx_conversion",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: merchantName,
      counterpartyName: transaction.counterpartyName ?? null,
      economicEntityId: transaction.economicEntityId,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.98",
      explanation:
        "Revolut marks this transaction as an exchange, so it is treated as an FX conversion.",
      needsReview: false,
      reviewReason: null,
      securityHint: null,
      quantity: null,
      unitPriceOriginal: null,
    };
  }

  if (revolutType === "fee") {
    return {
      transactionClass: "fee",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: merchantName,
      counterpartyName: transaction.counterpartyName ?? null,
      economicEntityId: transaction.economicEntityId,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.96",
      explanation:
        "Revolut marks this transaction as a fee, so it is treated as bank fees.",
      needsReview: false,
      reviewReason: null,
      securityHint: null,
      quantity: null,
      unitPriceOriginal: null,
    };
  }

  if (refundTypes.has(revolutType)) {
    return {
      transactionClass: "refund",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: merchantName,
      counterpartyName: transaction.counterpartyName ?? null,
      economicEntityId: transaction.economicEntityId,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.96",
      explanation:
        "Revolut marks this transaction as a refund, so it is treated as money returning from a prior charge.",
      needsReview: false,
      reviewReason: null,
      securityHint: null,
      quantity: null,
      unitPriceOriginal: null,
    };
  }

  return null;
}

function buildDeterministicClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
): DeterministicClassification {
  const matchedRule = applyRuleMatch(transaction, dataset.rules);
  if (matchedRule) {
    const allowedTransactionClasses = new Set(
      buildAllowedTransactionClassesForAccount(account),
    );
    const allowedCategoryCodes = getAllowedCategoryCodesForAccount(
      dataset,
      account,
    );
    const requestedTransactionClass =
      typeof matchedRule.outputsJson.transaction_class === "string"
        ? matchedRule.outputsJson.transaction_class
        : null;
    const requestedCategoryCode =
      typeof matchedRule.outputsJson.category_code === "string"
        ? matchedRule.outputsJson.category_code
        : null;
    const requestedEconomicEntityId =
      typeof matchedRule.outputsJson.economic_entity_id_override === "string"
        ? matchedRule.outputsJson.economic_entity_id_override
        : null;
    const rejectedRuleOutputs: string[] = [];
    if (
      requestedTransactionClass &&
      !allowedTransactionClasses.has(
        requestedTransactionClass as Transaction["transactionClass"],
      )
    ) {
      rejectedRuleOutputs.push("transaction class");
    }
    if (requestedCategoryCode && !allowedCategoryCodes.has(requestedCategoryCode)) {
      rejectedRuleOutputs.push("category");
    }
    if (
      requestedEconomicEntityId &&
      resolveConstrainedEconomicEntityId(
        dataset,
        account,
        requestedEconomicEntityId,
        transaction.economicEntityId,
      ) !== requestedEconomicEntityId
    ) {
      rejectedRuleOutputs.push("economic entity");
    }

    const transactionClass =
      requestedTransactionClass &&
      allowedTransactionClasses.has(
        requestedTransactionClass as Transaction["transactionClass"],
      )
        ? requestedTransactionClass
        : transaction.transactionClass;
    const categoryCode =
      requestedCategoryCode && allowedCategoryCodes.has(requestedCategoryCode)
        ? requestedCategoryCode
        : (transaction.categoryCode ?? getFallbackCategory(transaction, account));
    const economicEntityId = resolveConstrainedEconomicEntityId(
      dataset,
      account,
      requestedEconomicEntityId,
      transaction.economicEntityId,
    );

    return {
      transactionClass,
      categoryCode,
      merchantNormalized:
        typeof matchedRule.outputsJson.merchant_normalized === "string"
          ? matchedRule.outputsJson.merchant_normalized
          : (transaction.merchantNormalized ?? null),
      counterpartyName:
        typeof matchedRule.outputsJson.counterparty_name === "string"
          ? matchedRule.outputsJson.counterparty_name
          : (transaction.counterpartyName ?? null),
      economicEntityId,
      classificationStatus: "rule",
      classificationSource: "user_rule",
      classificationConfidence:
        rejectedRuleOutputs.length === 0 ? "1.00" : "0.00",
      explanation:
        rejectedRuleOutputs.length === 0
          ? "Matched an existing saved classification rule."
          : "A saved rule matched, but some requested outputs were incompatible with this account.",
      needsReview: rejectedRuleOutputs.length > 0,
      reviewReason:
        rejectedRuleOutputs.length > 0
          ? `Saved rule requested an incompatible ${rejectedRuleOutputs.join(", ")} for this account.`
          : null,
      securityHint: null,
      quantity: transaction.quantity ?? null,
      unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    };
  }

  const transferMatch = detectInternalTransfer(
    transaction,
    dataset.transactions,
    dataset.accounts,
  );
  if (transferMatch) {
    return {
      transactionClass: "transfer_internal",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: transaction.merchantNormalized ?? null,
      counterpartyName:
        transferMatch.counterpartyName ??
        transferMatch.merchantNormalized ??
        null,
      economicEntityId: transaction.economicEntityId,
      classificationStatus: "transfer_match",
      classificationSource: "transfer_matcher",
      classificationConfidence: "1.00",
      explanation:
        "Matched an opposite-signed owned-account transfer candidate.",
      needsReview: false,
      reviewReason: null,
      securityHint: null,
      quantity: transaction.quantity ?? null,
      unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    };
  }

  const revolutDeterministic = buildRevolutDeterministicClassification(
    transaction,
  );
  if (revolutDeterministic) {
    return revolutDeterministic;
  }

  if (account.assetDomain === "investment") {
    const parsed = parseInvestmentEvent(transaction);
    if (parsed.transactionClass !== "unknown") {
      const categoryCode =
        parsed.transactionClass === "dividend"
          ? "dividend"
          : parsed.transactionClass === "interest"
            ? "interest"
            : parsed.transactionClass === "fee"
              ? "broker_fee"
              : parsed.transactionClass === "transfer_internal"
                ? "uncategorized_investment"
              : parsed.transactionClass === "investment_trade_buy"
                ? "stock_buy"
                : "uncategorized_investment";

      return {
        transactionClass: parsed.transactionClass,
        categoryCode,
        merchantNormalized: transaction.merchantNormalized ?? null,
        counterpartyName: transaction.counterpartyName ?? null,
        economicEntityId: transaction.economicEntityId,
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        explanation: "Matched the deterministic investment statement parser.",
        needsReview: !transaction.securityId && Boolean(parsed.securityHint),
        reviewReason:
          !transaction.securityId && parsed.securityHint
            ? `Parsed investment trade for "${parsed.securityHint}", but the system has not matched it to a tracked security yet.`
            : null,
        securityHint: parsed.securityHint ?? null,
        quantity: normalizeTradeQuantity(
          parsed.transactionClass,
          transaction.quantity ?? parsed.quantity ?? null,
        ),
        unitPriceOriginal:
          transaction.unitPriceOriginal ?? parsed.unitPriceOriginal ?? null,
      };
    }
  }

  return {
    transactionClass: "unknown",
    categoryCode: getFallbackCategory(transaction, account),
    merchantNormalized: transaction.merchantNormalized ?? null,
    counterpartyName: transaction.counterpartyName ?? null,
    economicEntityId: transaction.economicEntityId,
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    explanation: "No deterministic classifier matched the imported row.",
    needsReview: true,
    reviewReason: "Needs LLM enrichment.",
    securityHint: null,
    quantity: transaction.quantity ?? null,
    unitPriceOriginal: transaction.unitPriceOriginal ?? null,
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
          "gpt-5.4"
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
  const existingReviewContext = readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.reviewContext,
  );
  const persistedSecurityMappings = buildPersistedInvestmentSecurityMappings(
    dataset,
    account,
    transaction,
    deterministic,
  );
  const model = getTransactionReviewModel(account, options?.trigger);
  const providerContext = extractProviderContext(transaction);
  const providerMerchantName = readOptionalString(
    readOptionalRecord(providerContext?.merchant)?.name,
  );
  const allowedTransactionClasses = buildAllowedTransactionClassesForAccount(
    account,
  );
  const allowedCategories = buildAllowedCategoriesForAccount(dataset, account);
  const reviewExamples = buildHistoricalReviewExamples(
    dataset,
    account,
    transaction,
  );
  const similarAccountTransactions =
    options?.similarAccountTransactions ??
    rankSimilarAccountTransactions(dataset, account, transaction, {
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
    }));
  const requestedAt = new Date().toISOString();
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
      reviewExamplesUsed: reviewExamples.map((example) => ({
        auditEventId: example.auditEventId,
        objectId: example.objectId,
        createdAt: example.createdAt,
      })),
    };
  }

  const result = await analyzeBankTransaction(
    createLLMClient(),
    {
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
      reviewContext: {
        trigger: options?.trigger ?? "import_classification",
        previousReviewReason:
          options?.reviewContext?.previousReviewReason ??
          transaction.reviewReason ??
          null,
        previousUserContext:
          options?.reviewContext?.previousUserContext ??
          transaction.manualNotes ??
          null,
        userProvidedContext:
          options?.reviewContext?.userProvidedContext ?? null,
        previousLlmPayload:
          options?.reviewContext?.previousLlmPayload ??
          (transaction.llmPayload as
            | Record<string, unknown>
            | null
            | undefined) ??
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
      },
    },
    model,
  );
  const completedAt = new Date().toISOString();
  const durationMs =
    new Date(completedAt).getTime() - new Date(requestedAt).getTime();

  if (result.analysisStatus !== "done" || !result.output) {
    return {
      analysisStatus: "failed",
      model,
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
      reviewExamplesUsed: reviewExamples.map((example) => ({
        auditEventId: example.auditEventId,
        objectId: example.objectId,
        createdAt: example.createdAt,
      })),
    };
  }

  return {
    analysisStatus: "done",
    model,
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
    reviewExamplesUsed: reviewExamples.map((example) => ({
      auditEventId: example.auditEventId,
      objectId: example.objectId,
      createdAt: example.createdAt,
    })),
  };
}

export async function enrichImportedTransaction(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options?: TransactionEnrichmentOptions,
): Promise<TransactionEnrichmentDecision> {
  const existingReviewContext = readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.reviewContext,
  );
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
      new Set(["user_rule", "transfer_matcher"]).has(
        deterministic.classificationSource,
      ) && !deterministic.needsReview;
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
        (account.assetDomain === "investment"
          ? getInvestmentReviewModel(options?.trigger)
          : getTransactionClassifierConfig().model),
      explanation: llm.explanation ?? deterministic.explanation,
      reason: llm.reason ?? deterministic.explanation,
      confidence: llm.confidence ?? deterministic.classificationConfidence,
      deterministic,
      llm,
      providerContext,
      reviewContext: {
        trigger: options?.trigger ?? "import_classification",
        previousReviewReason:
          options?.reviewContext?.previousReviewReason ??
          transaction.reviewReason ??
          null,
        previousUserContext:
          options?.reviewContext?.previousUserContext ??
          transaction.manualNotes ??
          null,
        userProvidedContext:
          options?.reviewContext?.userProvidedContext ?? null,
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
      },
      reviewExamplesUsed: llm.reviewExamplesUsed,
      timing: {
        requestedAt: llm.requestedAt,
        completedAt: llm.completedAt,
        durationMs: llm.durationMs,
      },
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

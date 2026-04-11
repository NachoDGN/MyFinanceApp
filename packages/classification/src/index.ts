import { z } from "zod";

import {
  analyzeBankTransaction,
  createTextEmbeddingClient,
  createLLMClient,
  isModelConfigured,
  isTextEmbeddingConfigured,
  type PromptProfileOverrides,
  type TextEmbeddingClient,
} from "@myfinance/llm";
import type {
  Account,
  AuditEvent,
  ClassificationRule,
  DomainDataset,
  Transaction,
} from "@myfinance/domain";
import {
  buildAllowedCategoriesForAccount,
  buildAllowedTransactionClassesForAccount,
  buildLiveHoldingRows,
  extractIsinFromText,
  getAllowedCategoryCodesForAccount,
  getDatasetLatestDate,
  normalizeDescription,
  normalizeInvestmentMatchingText,
  resolveConstrainedEconomicEntityId,
} from "@myfinance/domain";

export { normalizeInvestmentMatchingText };

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

export function parseInvestmentEvent(transaction: Transaction): {
  transactionClass:
    | "investment_trade_buy"
    | "investment_trade_sell"
    | "transfer_internal"
    | "dividend"
    | "interest"
    | "fee"
    | "fx_conversion"
    | "balance_adjustment"
    | "unknown";
  quantity?: string;
  securityHint?: string;
  unitPriceOriginal?: string;
} {
  const comparison = normalizeDescription(
    transaction.descriptionRaw,
  ).comparison;

  if (comparison.includes("DIVIDEND")) {
    return { transactionClass: "dividend" };
  }
  if (/\bTRANSFERENCIAS?\s+ENTRE\s+CUENTAS\b/.test(comparison)) {
    return { transactionClass: "transfer_internal" };
  }
  if (comparison.includes("INTEREST")) {
    return { transactionClass: "interest" };
  }
  if (
    Number(transaction.amountOriginal) === 0 &&
    comparison.includes("IRPF") &&
    comparison.includes("INTERESES")
  ) {
    return { transactionClass: "balance_adjustment" };
  }
  if (
    comparison.startsWith("PERIODO ") &&
    Number(transaction.amountOriginal) > 0
  ) {
    return { transactionClass: "interest" };
  }
  if (comparison.includes("COMMISSION") || comparison.includes("FEE")) {
    return { transactionClass: "fee" };
  }
  if (comparison.includes("FX") || comparison.includes("CONVERSION")) {
    return { transactionClass: "fx_conversion" };
  }

  const quantityMatch = comparison.match(/@\s*([0-9]+(?:\.[0-9]+)?)/);
  const buyMatch = comparison.match(
    /(BUY|SELL)\s+([0-9]+(?:\.[0-9]+)?)\s+(.+)/,
  );

  if (quantityMatch) {
    const quantity = quantityMatch[1];
    const securityHint = comparison.split("@")[0]?.trim() ?? comparison;
    const gross = Math.abs(Number(transaction.amountOriginal));
    const unitPriceOriginal =
      quantity === "0" ? undefined : (gross / Number(quantity)).toFixed(2);
    const transactionClass =
      Number(transaction.amountOriginal) < 0
        ? "investment_trade_buy"
        : "investment_trade_sell";
    return {
      transactionClass,
      quantity:
        quantity === "0"
          ? undefined
          : (normalizeTradeQuantity(transactionClass, quantity) ?? undefined),
      securityHint,
      unitPriceOriginal,
    };
  }

  if (buyMatch) {
    const transactionClass =
      buyMatch[1] === "BUY" ? "investment_trade_buy" : "investment_trade_sell";
    return {
      transactionClass,
      quantity:
        normalizeTradeQuantity(transactionClass, buyMatch[2]) ?? undefined,
      securityHint: buyMatch[3],
    };
  }

  const genericSecurityHint = comparison
    .replace(/\b(?:BUY|SELL)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const looksLikeNamedSecurity =
    /\b(?:ETF|FUND|INDEX|STOCK|SHARES?|UCITS|ADR|GDR|INC|CORP|CORPORATION|HOLDINGS|CLASS|CAP)\b/.test(
      comparison,
    );
  if (looksLikeNamedSecurity && genericSecurityHint) {
    return {
      transactionClass:
        Number(transaction.amountOriginal) < 0
          ? "investment_trade_buy"
          : "investment_trade_sell",
      securityHint: genericSecurityHint,
    };
  }

  return { transactionClass: "unknown" };
}

type DeterministicClassification = {
  transactionClass: string;
  categoryCode: string | null;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  economicEntityId: string;
  classificationStatus: Transaction["classificationStatus"];
  classificationSource: Transaction["classificationSource"];
  classificationConfidence: string;
  explanation: string;
  needsReview: boolean;
  reviewReason: string | null;
  securityHint: string | null;
  quantity: string | null;
  unitPriceOriginal: string | null;
};

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

type TransactionEnrichmentTrigger = NonNullable<
  TransactionEnrichmentOptions["trigger"]
>;

type HistoricalReviewExample = {
  auditEventId: string;
  objectId: string;
  createdAt: string;
  accountId: string | null;
  institutionName: string | null;
  transaction: {
    transactionDate: string | null;
    postedDate: string | null;
    amountOriginal: string | null;
    currencyOriginal: string | null;
    descriptionRaw: string | null;
    merchantNormalized: string | null;
    counterpartyName: string | null;
    securityId: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
  };
  initialInference: {
    transactionClass: string | null;
    categoryCode: string | null;
    classificationSource: string | null;
    classificationStatus: string | null;
    classificationConfidence: string | null;
    needsReview: boolean | null;
    reviewReason: string | null;
    model: string | null;
    explanation: string | null;
    reason: string | null;
  };
  userFeedback: string;
  correctedOutcome: {
    transactionClass: string | null;
    categoryCode: string | null;
    merchantNormalized: string | null;
    counterpartyName: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    reviewReason: string | null;
  };
};

type PersistedSecurityMapping = {
  securityId: string;
  matchedAlias: string;
  aliasSource: string;
  confidence: string;
  providerSymbol: string;
  displaySymbol: string;
  securityName: string;
  isin: string | null;
};

export interface SimilarAccountTransactionMatch {
  transaction: Transaction;
  score: number;
}

export interface ReviewPropagationTransactionMatch extends SimilarAccountTransactionMatch {
  semanticSimilarity: number | null;
  lexicalScore: number;
  exactMatch: boolean;
}

const INVESTMENT_DESCRIPTOR_STOPWORDS = new Set([
  "BUY",
  "SELL",
  "FUND",
  "FUNDS",
  "ETF",
  "ETFS",
  "INDEX",
  "STOCK",
  "STOCKS",
  "SHARE",
  "SHARES",
  "UCITS",
  "OEIC",
  "ACC",
  "ACCU",
  "ACCUMULATION",
  "ACCUMULATING",
  "DIST",
  "DISTRIBUTION",
  "DISTRIBUTING",
  "CLASS",
  "CL",
  "EUR",
  "USD",
  "GBP",
  "NAV",
]);

const FUND_BRAND_TOKENS = new Set([
  "VANGUARD",
  "ISHARES",
  "AMUNDI",
  "BLACKROCK",
  "INVESCO",
  "FIDELITY",
  "JPMORGAN",
  "SPDR",
  "XTRACKERS",
  "HSBC",
  "LYXOR",
  "UBS",
  "DWS",
  "FRANKLIN",
]);

function normalizeOptionalText(value: string | null | undefined) {
  const text = value?.trim() ?? "";
  return text || null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readOptionalBoolean(value: unknown) {
  return typeof value === "boolean" ? value : null;
}

function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

function readOptionalRecord(value: unknown) {
  return isRecord(value) ? value : null;
}

function readUnknownArray(value: unknown) {
  return Array.isArray(value) ? value : null;
}

function camelizeJsonKey(value: string) {
  return value.replace(/_([a-z])/g, (_, character: string) =>
    character.toUpperCase(),
  );
}

function readRawOutputField(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  if (!rawOutput) {
    return null;
  }

  if (key in rawOutput) {
    return rawOutput[key];
  }

  const camelizedKey = camelizeJsonKey(key);
  if (camelizedKey in rawOutput) {
    return rawOutput[camelizedKey];
  }

  return null;
}

function readRawOutputString(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  return readOptionalString(readRawOutputField(rawOutput, key));
}

function tokenizePromptText(value: string | null | undefined) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) {
    return new Set<string>();
  }

  return new Set(
    normalizeDescription(normalized)
      .comparison.split(/[^A-Z0-9]+/)
      .filter((token) => token.length >= 3),
  );
}

export function getReviewPropagationEmbeddingModel() {
  return (
    process.env.REVIEW_PROPAGATION_EMBEDDING_MODEL?.trim() ||
    "gemini-embedding-001"
  );
}

function looksLikeFundDescriptor(normalizedText: string) {
  return /\b(FUND|ETF|INDEX|UCITS|OEIC|NAV)\b/.test(normalizedText);
}

function tokenizeInvestmentMatchingText(
  value: string | null | undefined,
  options: { distinctiveOnly?: boolean } = {},
) {
  const normalized = normalizeInvestmentMatchingText(value);
  if (!normalized) {
    return new Set<string>();
  }

  const stopwords = new Set(INVESTMENT_DESCRIPTOR_STOPWORDS);
  if (options.distinctiveOnly && looksLikeFundDescriptor(normalized)) {
    for (const token of FUND_BRAND_TOKENS) {
      stopwords.add(token);
    }
  }

  return new Set(
    normalized
      .split(/[^A-Z0-9]+/)
      .filter((token) => token.length >= 2)
      .filter((token) =>
        options.distinctiveOnly ? !stopwords.has(token) : true,
      ),
  );
}

function buildReviewPropagationContextText(transaction: Transaction) {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);

  return [
    transaction.descriptionRaw,
    transaction.manualNotes,
    readOptionalString(reviewContext?.previousUserContext),
    readOptionalString(reviewContext?.userProvidedContext),
    readRawOutputString(rawOutput, "resolved_instrument_name"),
    readRawOutputString(rawOutput, "resolved_instrument_isin"),
    readRawOutputString(rawOutput, "current_price_type"),
    readRawOutputString(rawOutput, "reason"),
    readRawOutputString(rawOutput, "explanation"),
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function extractTransactionIsinEvidence(transaction: Transaction) {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);

  return extractIsinFromText(
    readRawOutputString(rawOutput, "resolved_instrument_isin"),
    transaction.manualNotes,
    readOptionalString(reviewContext?.previousUserContext),
    readOptionalString(reviewContext?.userProvidedContext),
    readRawOutputString(rawOutput, "reason"),
    readRawOutputString(rawOutput, "explanation"),
  );
}

function countOverlappingTokens(left: Set<string>, right: Set<string>) {
  return [...left].filter((token) => right.has(token)).length;
}

function calculateJaccardScore(left: Set<string>, right: Set<string>) {
  const union = new Set([...left, ...right]).size;
  if (union === 0) {
    return 0;
  }
  return countOverlappingTokens(left, right) / union;
}

function calculateCosineSimilarity(left: number[], right: number[]) {
  const length = Math.min(left.length, right.length);
  if (length === 0) {
    return 0;
  }

  let score = 0;
  for (let index = 0; index < length; index += 1) {
    score += left[index]! * right[index]!;
  }
  return score;
}

function buildReviewPropagationEvidence(transaction: Transaction) {
  const normalizedDescription = normalizeInvestmentMatchingText(
    transaction.descriptionRaw,
  );
  const contextText = buildReviewPropagationContextText(transaction);
  const combinedText = normalizeInvestmentMatchingText(contextText);
  const allTokens = tokenizeInvestmentMatchingText(transaction.descriptionRaw);
  const distinctiveTokens = tokenizeInvestmentMatchingText(
    transaction.descriptionRaw,
    { distinctiveOnly: true },
  );
  const exactIsin = extractTransactionIsinEvidence(transaction);
  const explicitEtf =
    /\bETF\b/.test(combinedText) &&
    !/\bNOT AN ETF\b|\bNOT ETF\b/.test(combinedText);
  const explicitMutualFund = /\b(MUTUAL FUND|INDEX FUND|OEIC|NAV)\b/.test(
    combinedText,
  );

  return {
    normalizedDescription,
    allTokens,
    distinctiveTokens,
    embeddingText:
      normalizedDescription || normalizeInvestmentMatchingText(contextText),
    exactIsin,
    explicitEtf,
    explicitMutualFund,
  };
}

function extractHistoricalReviewExample(
  auditEvent: AuditEvent,
): HistoricalReviewExample | null {
  if (auditEvent.commandName !== "transactions.review_reanalyze") {
    return null;
  }

  const before = readOptionalRecord(auditEvent.beforeJson);
  const after = readOptionalRecord(auditEvent.afterJson);
  if (!before || !after) {
    return null;
  }

  const afterLlmPayload = readOptionalRecord(after.llmPayload);
  const afterReviewContext = readOptionalRecord(afterLlmPayload?.reviewContext);
  const userFeedback =
    normalizeOptionalText(
      typeof afterReviewContext?.userProvidedContext === "string"
        ? afterReviewContext.userProvidedContext
        : null,
    ) ??
    normalizeOptionalText(
      typeof after.manualNotes === "string" ? after.manualNotes : null,
    );
  if (!userFeedback) {
    return null;
  }

  const beforeLlmPayload = readOptionalRecord(before.llmPayload);
  const beforeLlm = readOptionalRecord(beforeLlmPayload?.llm);
  const afterAccountId =
    normalizeOptionalText(
      typeof after.accountId === "string" ? after.accountId : null,
    ) ??
    normalizeOptionalText(
      typeof before.accountId === "string" ? before.accountId : null,
    );

  return {
    auditEventId: auditEvent.id,
    objectId: auditEvent.objectId,
    createdAt: auditEvent.createdAt,
    accountId: afterAccountId,
    institutionName: normalizeOptionalText(
      typeof after.counterpartyName === "string"
        ? after.counterpartyName
        : null,
    ),
    transaction: {
      transactionDate: normalizeOptionalText(
        typeof before.transactionDate === "string"
          ? before.transactionDate
          : null,
      ),
      postedDate: normalizeOptionalText(
        typeof before.postedDate === "string" ? before.postedDate : null,
      ),
      amountOriginal: normalizeOptionalText(
        typeof before.amountOriginal === "string"
          ? before.amountOriginal
          : null,
      ),
      currencyOriginal: normalizeOptionalText(
        typeof before.currencyOriginal === "string"
          ? before.currencyOriginal
          : null,
      ),
      descriptionRaw: normalizeOptionalText(
        typeof before.descriptionRaw === "string"
          ? before.descriptionRaw
          : null,
      ),
      merchantNormalized: normalizeOptionalText(
        typeof before.merchantNormalized === "string"
          ? before.merchantNormalized
          : null,
      ),
      counterpartyName: normalizeOptionalText(
        typeof before.counterpartyName === "string"
          ? before.counterpartyName
          : null,
      ),
      securityId: normalizeOptionalText(
        typeof before.securityId === "string" ? before.securityId : null,
      ),
      quantity: normalizeOptionalText(
        typeof before.quantity === "string" ? before.quantity : null,
      ),
      unitPriceOriginal: normalizeOptionalText(
        typeof before.unitPriceOriginal === "string"
          ? before.unitPriceOriginal
          : null,
      ),
    },
    initialInference: {
      transactionClass: normalizeOptionalText(
        typeof before.transactionClass === "string"
          ? before.transactionClass
          : null,
      ),
      categoryCode: normalizeOptionalText(
        typeof before.categoryCode === "string" ? before.categoryCode : null,
      ),
      classificationSource: normalizeOptionalText(
        typeof before.classificationSource === "string"
          ? before.classificationSource
          : null,
      ),
      classificationStatus: normalizeOptionalText(
        typeof before.classificationStatus === "string"
          ? before.classificationStatus
          : null,
      ),
      classificationConfidence: normalizeOptionalText(
        typeof before.classificationConfidence === "string"
          ? before.classificationConfidence
          : null,
      ),
      needsReview: readOptionalBoolean(before.needsReview),
      reviewReason: normalizeOptionalText(
        typeof before.reviewReason === "string" ? before.reviewReason : null,
      ),
      model:
        normalizeOptionalText(
          typeof beforeLlm?.model === "string" ? beforeLlm.model : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.model === "string"
            ? beforeLlmPayload.model
            : null,
        ),
      explanation:
        normalizeOptionalText(
          typeof beforeLlm?.explanation === "string"
            ? beforeLlm.explanation
            : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.explanation === "string"
            ? beforeLlmPayload.explanation
            : null,
        ),
      reason:
        normalizeOptionalText(
          typeof beforeLlm?.reason === "string" ? beforeLlm.reason : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.reason === "string"
            ? beforeLlmPayload.reason
            : null,
        ),
    },
    userFeedback,
    correctedOutcome: {
      transactionClass: normalizeOptionalText(
        typeof after.transactionClass === "string"
          ? after.transactionClass
          : null,
      ),
      categoryCode: normalizeOptionalText(
        typeof after.categoryCode === "string" ? after.categoryCode : null,
      ),
      merchantNormalized: normalizeOptionalText(
        typeof after.merchantNormalized === "string"
          ? after.merchantNormalized
          : null,
      ),
      counterpartyName: normalizeOptionalText(
        typeof after.counterpartyName === "string"
          ? after.counterpartyName
          : null,
      ),
      quantity: normalizeOptionalText(
        typeof after.quantity === "string" ? after.quantity : null,
      ),
      unitPriceOriginal: normalizeOptionalText(
        typeof after.unitPriceOriginal === "string"
          ? after.unitPriceOriginal
          : null,
      ),
      reviewReason: normalizeOptionalText(
        typeof after.reviewReason === "string" ? after.reviewReason : null,
      ),
    },
  };
}

function buildHistoricalReviewExamples(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  limit = 5,
) {
  const accountById = new Map(
    dataset.accounts.map((candidate) => [candidate.id, candidate]),
  );
  const targetTokens = tokenizePromptText(transaction.descriptionRaw);

  return dataset.auditEvents
    .map((auditEvent) => extractHistoricalReviewExample(auditEvent))
    .filter((example): example is HistoricalReviewExample => Boolean(example))
    .filter((example) => example.objectId !== transaction.id)
    .filter((example) => {
      if (!example.accountId) {
        return false;
      }
      const exampleAccount = accountById.get(example.accountId);
      return exampleAccount?.assetDomain === account.assetDomain;
    })
    .sort((left, right) => {
      const leftAccount = left.accountId
        ? accountById.get(left.accountId)
        : null;
      const rightAccount = right.accountId
        ? accountById.get(right.accountId)
        : null;

      const scoreExample = (
        example: HistoricalReviewExample,
        exampleAccount: Account | undefined | null,
      ) => {
        let score = 0;
        if (exampleAccount?.institutionName === account.institutionName) {
          score += 20;
        }
        if (
          transaction.securityId &&
          example.transaction.securityId === transaction.securityId
        ) {
          score += 30;
        }
        const exampleTokens = tokenizePromptText(
          example.transaction.descriptionRaw,
        );
        for (const token of targetTokens) {
          if (exampleTokens.has(token)) {
            score += 2;
          }
        }
        return score;
      };

      const scoreDelta =
        scoreExample(right, rightAccount) - scoreExample(left, leftAccount);
      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      return (
        new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime()
      );
    })
    .slice(0, limit);
}

function scoreSimilarAccountTransaction(
  target: Transaction,
  candidate: Transaction,
) {
  const targetTokens = tokenizePromptText(target.descriptionRaw);
  const candidateTokens = tokenizePromptText(candidate.descriptionRaw);
  const overlappingTokenCount = [...targetTokens].filter((token) =>
    candidateTokens.has(token),
  ).length;
  const unionCount = new Set([...targetTokens, ...candidateTokens]).size;
  const targetMagnitude = Math.abs(Number(target.amountOriginal));
  const candidateMagnitude = Math.abs(Number(candidate.amountOriginal));
  const sameDirection =
    Math.sign(Number(target.amountOriginal)) ===
    Math.sign(Number(candidate.amountOriginal));

  let score = overlappingTokenCount * 2;
  if (unionCount > 0) {
    score += (overlappingTokenCount / unionCount) * 10;
  }
  if (target.securityId && candidate.securityId === target.securityId) {
    score += 20;
  }
  if (
    target.transactionClass !== "unknown" &&
    candidate.transactionClass === target.transactionClass
  ) {
    score += 4;
  }
  if (target.categoryCode && candidate.categoryCode === target.categoryCode) {
    score += 3;
  }
  if (sameDirection) {
    score += 2;
  }
  if (targetMagnitude > 0 && candidateMagnitude > 0) {
    const amountRatio =
      Math.min(targetMagnitude, candidateMagnitude) /
      Math.max(targetMagnitude, candidateMagnitude);
    if (amountRatio >= 0.8) {
      score += 3;
    } else if (amountRatio >= 0.5) {
      score += 1;
    }
  }

  return score;
}

export function rankSimilarAccountTransactions(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options: {
    limit?: number;
    minScore?: number;
    includeNeedsReview?: boolean;
    requireEarlierDate?: boolean;
  } = {},
): SimilarAccountTransactionMatch[] {
  const limit = options.limit ?? 5;
  const minScore = options.minScore ?? 6;

  return dataset.transactions
    .filter((candidate) => candidate.id !== transaction.id)
    .filter((candidate) => candidate.accountId === account.id)
    .filter((candidate) => !candidate.voidedAt)
    .filter((candidate) =>
      options.includeNeedsReview ? true : candidate.needsReview !== true,
    )
    .filter((candidate) =>
      options.requireEarlierDate
        ? candidate.transactionDate <= transaction.transactionDate
        : true,
    )
    .map((candidate) => ({
      transaction: candidate,
      score: scoreSimilarAccountTransaction(transaction, candidate),
    }))
    .filter((match) => match.score >= minScore)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return (
        new Date(right.transaction.createdAt).getTime() -
        new Date(left.transaction.createdAt).getTime()
      );
    })
    .slice(0, limit);
}

export async function rankReviewPropagationTransactions(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options: {
    limit?: number;
    embeddingClient?: TextEmbeddingClient | null;
  } = {},
): Promise<ReviewPropagationTransactionMatch[]> {
  const limit = options.limit ?? 25;

  if (account.assetDomain !== "investment") {
    return rankSimilarAccountTransactions(dataset, account, transaction, {
      includeNeedsReview: true,
      requireEarlierDate: false,
      limit,
      minScore: 6,
    })
      .filter((match) => match.transaction.needsReview)
      .map((match) => ({
        ...match,
        semanticSimilarity: null,
        lexicalScore: match.score,
        exactMatch: false,
      }));
  }

  const sourceEvidence = buildReviewPropagationEvidence(transaction);
  const sourceMagnitude = Math.abs(Number(transaction.amountOriginal));
  const sourceSign = Math.sign(Number(transaction.amountOriginal));
  const candidateMatches = dataset.transactions
    .filter((candidate) => candidate.id !== transaction.id)
    .filter((candidate) => candidate.accountId === account.id)
    .filter((candidate) => candidate.needsReview)
    .filter((candidate) => !candidate.voidedAt)
    .filter(
      (candidate) =>
        candidate.currencyOriginal === transaction.currencyOriginal,
    )
    .filter((candidate) => {
      const candidateSign = Math.sign(Number(candidate.amountOriginal));
      return (
        candidateSign === 0 || sourceSign === 0 || candidateSign === sourceSign
      );
    })
    .filter((candidate) => {
      if (
        transaction.transactionClass === "unknown" ||
        candidate.transactionClass === "unknown"
      ) {
        return true;
      }
      return candidate.transactionClass === transaction.transactionClass;
    })
    .map((candidate) => {
      const candidateEvidence = buildReviewPropagationEvidence(candidate);
      if (
        sourceEvidence.exactIsin &&
        candidateEvidence.exactIsin &&
        sourceEvidence.exactIsin !== candidateEvidence.exactIsin
      ) {
        return null;
      }
      if (sourceEvidence.explicitMutualFund && candidateEvidence.explicitEtf) {
        return null;
      }
      if (sourceEvidence.explicitEtf && candidateEvidence.explicitMutualFund) {
        return null;
      }

      const commonTokenCount = countOverlappingTokens(
        sourceEvidence.allTokens,
        candidateEvidence.allTokens,
      );
      const distinctiveOverlapCount = countOverlappingTokens(
        sourceEvidence.distinctiveTokens,
        candidateEvidence.distinctiveTokens,
      );
      const distinctiveJaccard = calculateJaccardScore(
        sourceEvidence.distinctiveTokens,
        candidateEvidence.distinctiveTokens,
      );
      const exactIsinMatch =
        Boolean(sourceEvidence.exactIsin) &&
        sourceEvidence.exactIsin === candidateEvidence.exactIsin;
      const exactSecurityIdMatch =
        Boolean(transaction.securityId) &&
        transaction.securityId === candidate.securityId;
      const exactDescriptionMatch =
        sourceEvidence.normalizedDescription.length > 0 &&
        sourceEvidence.normalizedDescription ===
          candidateEvidence.normalizedDescription;
      const candidateMagnitude = Math.abs(Number(candidate.amountOriginal));
      const amountRatio =
        sourceMagnitude > 0 && candidateMagnitude > 0
          ? Math.min(sourceMagnitude, candidateMagnitude) /
            Math.max(sourceMagnitude, candidateMagnitude)
          : 0;
      const lexicalScore =
        commonTokenCount * 2 +
        distinctiveOverlapCount * 10 +
        distinctiveJaccard * 20 +
        (amountRatio >= 0.95 ? 4 : amountRatio >= 0.7 ? 2 : 0) +
        (exactDescriptionMatch ? 40 : 0) +
        (exactSecurityIdMatch ? 50 : 0) +
        (exactIsinMatch ? 80 : 0);

      const exactMatch =
        exactIsinMatch || exactSecurityIdMatch || exactDescriptionMatch;

      return {
        transaction: candidate,
        candidateEvidence,
        lexicalScore,
        exactMatch,
        distinctiveOverlapCount,
        distinctiveJaccard,
      };
    })
    .filter(
      (
        match,
      ): match is {
        transaction: Transaction;
        candidateEvidence: ReturnType<typeof buildReviewPropagationEvidence>;
        lexicalScore: number;
        exactMatch: boolean;
        distinctiveOverlapCount: number;
        distinctiveJaccard: number;
      } => Boolean(match),
    );

  if (candidateMatches.length === 0) {
    return [];
  }

  let embeddingClient = options.embeddingClient;
  if (embeddingClient === undefined && isTextEmbeddingConfigured()) {
    try {
      embeddingClient = createTextEmbeddingClient(
        getReviewPropagationEmbeddingModel(),
      );
    } catch {
      embeddingClient = null;
    }
  }

  const semanticSimilarityByTransactionId = new Map<string, number>();
  if (embeddingClient) {
    try {
      const embeddingTexts = [
        sourceEvidence.embeddingText || sourceEvidence.normalizedDescription,
        ...candidateMatches.map(
          (match) =>
            match.candidateEvidence.embeddingText ??
            match.candidateEvidence.normalizedDescription,
        ),
      ];
      const embeddings = await embeddingClient.embedTexts({
        texts: embeddingTexts,
        taskType: "SEMANTIC_SIMILARITY",
        outputDimensionality: 768,
      });
      const sourceVector = embeddings[0] ?? [];
      candidateMatches.forEach((match, index) => {
        const candidateVector = embeddings[index + 1] ?? [];
        semanticSimilarityByTransactionId.set(
          match.transaction.id,
          calculateCosineSimilarity(sourceVector, candidateVector),
        );
      });
    } catch {
      semanticSimilarityByTransactionId.clear();
    }
  }

  return candidateMatches
    .map((match) => {
      const semanticSimilarity =
        semanticSimilarityByTransactionId.get(match.transaction.id) ?? null;
      const passesThreshold =
        match.exactMatch ||
        (semanticSimilarity !== null &&
          (semanticSimilarity >= 0.97 ||
            (semanticSimilarity >= 0.9 &&
              (match.distinctiveOverlapCount >= 1 ||
                match.distinctiveJaccard >= 0.5)))) ||
        (semanticSimilarity === null &&
          (match.distinctiveOverlapCount >= 2 ||
            match.distinctiveJaccard >= 0.67));

      return {
        transaction: match.transaction,
        score:
          match.lexicalScore +
          (semanticSimilarity !== null ? semanticSimilarity * 100 : 0),
        lexicalScore: match.lexicalScore,
        semanticSimilarity,
        exactMatch: match.exactMatch,
        passesThreshold,
      };
    })
    .filter((match) => match.passesThreshold)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return (
        new Date(right.transaction.createdAt).getTime() -
        new Date(left.transaction.createdAt).getTime()
      );
    })
    .slice(0, limit)
    .map(({ passesThreshold: _passesThreshold, ...match }) => match);
}

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

function isTradeTransactionClass(
  transactionClass: string,
): transactionClass is "investment_trade_buy" | "investment_trade_sell" {
  return (
    transactionClass === "investment_trade_buy" ||
    transactionClass === "investment_trade_sell"
  );
}

function normalizeTradeQuantity(
  transactionClass: string,
  quantity: string | null | undefined,
) {
  const normalized = normalizeOptionalText(quantity);
  if (!normalized || !isTradeTransactionClass(transactionClass)) {
    return null;
  }

  const numericQuantity = Number(normalized);
  if (!Number.isFinite(numericQuantity) || numericQuantity === 0) {
    return null;
  }

  const absoluteQuantity = Math.abs(numericQuantity);
  const signedQuantity =
    transactionClass === "investment_trade_sell"
      ? -absoluteQuantity
      : absoluteQuantity;
  return signedQuantity.toFixed(8);
}

function inferTradeQuantityFromUnitPrice(
  transactionClass: string,
  amountOriginal: string,
  unitPriceOriginal: string | null | undefined,
) {
  if (!isTradeTransactionClass(transactionClass)) {
    return null;
  }

  const normalizedUnitPrice = normalizeOptionalText(unitPriceOriginal);
  if (!normalizedUnitPrice) {
    return null;
  }

  const numericUnitPrice = Number(normalizedUnitPrice);
  const numericAmount = Math.abs(Number(amountOriginal));
  if (
    !Number.isFinite(numericUnitPrice) ||
    numericUnitPrice === 0 ||
    !Number.isFinite(numericAmount) ||
    numericAmount === 0
  ) {
    return null;
  }

  return normalizeTradeQuantity(
    transactionClass,
    (numericAmount / numericUnitPrice).toFixed(8),
  );
}

function buildInvestmentPortfolioState(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
) {
  if (account.assetDomain !== "investment") {
    return undefined;
  }

  const asOfDate = getDatasetLatestDate(dataset);
  const holdings = buildLiveHoldingRows(
    dataset,
    { kind: "account", accountId: account.id },
    asOfDate,
  );

  const targetHint = normalizeDescription(
    deterministic.securityHint ??
      transaction.descriptionRaw ??
      transaction.securityId ??
      "",
  ).comparison;
  const matchedHolding =
    holdings.find((holding) => holding.securityId === transaction.securityId) ??
    holdings.find((holding) => {
      if (!targetHint) return false;
      const symbol = normalizeDescription(holding.symbol).comparison;
      const securityName = normalizeDescription(
        holding.securityName,
      ).comparison;
      return (
        targetHint.includes(symbol) ||
        targetHint.includes(securityName) ||
        securityName.includes(targetHint)
      );
    }) ??
    null;

  const normalizedTradeQuantity = normalizeTradeQuantity(
    deterministic.transactionClass,
    transaction.quantity ?? deterministic.quantity ?? null,
  );
  const quantity = Number(normalizedTradeQuantity ?? 0);
  const impliedUnitPrice =
    Number.isFinite(quantity) && Math.abs(quantity) > 0
      ? (
          Math.abs(Number(transaction.amountOriginal)) / Math.abs(quantity)
        ).toFixed(2)
      : null;
  const latestHoldingPrice = matchedHolding?.currentPrice ?? null;
  const sameCurrency =
    matchedHolding?.currentPriceCurrency === transaction.currencyOriginal;
  const priceDeltaPercent =
    impliedUnitPrice &&
    latestHoldingPrice &&
    sameCurrency &&
    Number(latestHoldingPrice) > 0
      ? (
          (Math.abs(Number(impliedUnitPrice) - Number(latestHoldingPrice)) /
            Number(latestHoldingPrice)) *
          100
        ).toFixed(2)
      : null;

  const serializeHolding = (holding: (typeof holdings)[number]) => ({
    securityId: holding.securityId,
    symbol: holding.symbol,
    securityName: holding.securityName,
    quantity: holding.quantity,
    currentPrice: holding.currentPrice,
    currentPriceCurrency: holding.currentPriceCurrency,
    currentValueEur: holding.currentValueEur,
    quoteTimestamp: holding.quoteTimestamp,
    quoteFreshness: holding.quoteFreshness,
  });

  return {
    scope: "account" as const,
    asOfDate,
    holdings: holdings.map(serializeHolding),
    matchedHolding: matchedHolding ? serializeHolding(matchedHolding) : null,
    priceSanityCheck:
      impliedUnitPrice || matchedHolding
        ? {
            impliedUnitPrice,
            impliedUnitPriceCurrency: transaction.currencyOriginal,
            latestHoldingPrice,
            latestHoldingPriceCurrency:
              matchedHolding?.currentPriceCurrency ?? null,
            latestHoldingQuoteTimestamp: matchedHolding?.quoteTimestamp ?? null,
            priceDeltaPercent,
          }
        : null,
  };
}

function buildPersistedInvestmentSecurityMappings(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
) {
  if (
    account.assetDomain !== "investment" ||
    !isTradeTransactionClass(deterministic.transactionClass)
  ) {
    return [] as PersistedSecurityMapping[];
  }

  const candidateAliases = new Set(
    [
      deterministic.securityHint,
      transaction.descriptionRaw,
      transaction.descriptionClean,
    ]
      .map((value) => normalizeDescription(value ?? "").comparison)
      .filter(Boolean),
  );
  if (candidateAliases.size === 0) {
    return [] as PersistedSecurityMapping[];
  }

  const seenSecurityIds = new Set<string>();
  const persistedMappings: PersistedSecurityMapping[] = [];

  for (const alias of dataset.securityAliases) {
    const normalizedAlias = normalizeDescription(
      alias.aliasTextNormalized,
    ).comparison;
    if (!candidateAliases.has(normalizedAlias)) {
      continue;
    }

    const security =
      dataset.securities.find(
        (candidate) => candidate.id === alias.securityId,
      ) ?? null;
    if (!security || seenSecurityIds.has(security.id)) {
      continue;
    }

    seenSecurityIds.add(security.id);
    persistedMappings.push({
      securityId: security.id,
      matchedAlias: alias.aliasTextNormalized,
      aliasSource: alias.aliasSource,
      confidence: alias.confidence,
      providerSymbol: security.providerSymbol,
      displaySymbol: security.displaySymbol,
      securityName: security.name,
      isin: security.isin ?? null,
    });

    if (persistedMappings.length >= 3) {
      break;
    }
  }

  return persistedMappings;
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

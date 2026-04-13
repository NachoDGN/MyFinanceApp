import { z } from "zod";

import { resolveModelProvider } from "../config";
import { renderTransactionAnalyzerPrompt } from "../prompts";
import { LLMError, type LLMTaskClient } from "../types";

function normalizeOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeOptionalNumber(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function normalizeTransactionAnalysisPayload(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }

  const record = value as Record<string, unknown>;
  const explanation =
    normalizeOptionalString(record.explanation) ??
    normalizeOptionalString(record.reason) ??
    "Model explanation unavailable.";
  const reason =
    normalizeOptionalString(record.reason) ??
    normalizeOptionalString(record.explanation) ??
    "Model reason unavailable.";
  const inferredConfidence = (() => {
    const explicitConfidence = normalizeOptionalNumber(record.confidence);
    if (explicitConfidence !== null) {
      return explicitConfidence;
    }

    const hasClassification =
      normalizeOptionalString(record.transaction_class) !== null ||
      normalizeOptionalString(record.transactionClass) !== null;
    const hasCategory =
      normalizeOptionalString(record.category_code) !== null ||
      normalizeOptionalString(record.categoryCode) !== null;

    if (hasClassification && hasCategory) {
      return 0.85;
    }
    if (hasClassification) {
      return 0.75;
    }
    return 0.5;
  })();

  return {
    transaction_class:
      normalizeOptionalString(record.transaction_class) ??
      normalizeOptionalString(record.transactionClass),
    category_code:
      normalizeOptionalString(record.category_code) ??
      normalizeOptionalString(record.categoryCode),
    merchant_normalized:
      normalizeOptionalString(record.merchant_normalized) ??
      normalizeOptionalString(record.merchantNormalized),
    counterparty_name:
      normalizeOptionalString(record.counterparty_name) ??
      normalizeOptionalString(record.counterpartyName),
    economic_entity_override:
      normalizeOptionalString(record.economic_entity_override) ??
      normalizeOptionalString(record.economicEntityOverride),
    security_hint:
      normalizeOptionalString(record.security_hint) ??
      normalizeOptionalString(record.securityHint),
    quantity: normalizeOptionalString(record.quantity),
    unit_price_original:
      normalizeOptionalString(record.unit_price_original) ??
      normalizeOptionalString(record.unitPriceOriginal),
    resolved_instrument_name:
      normalizeOptionalString(record.resolved_instrument_name) ??
      normalizeOptionalString(record.resolvedInstrumentName),
    resolved_instrument_isin:
      normalizeOptionalString(record.resolved_instrument_isin) ??
      normalizeOptionalString(record.resolvedInstrumentIsin),
    resolved_instrument_ticker:
      normalizeOptionalString(record.resolved_instrument_ticker) ??
      normalizeOptionalString(record.resolvedInstrumentTicker),
    resolved_instrument_exchange:
      normalizeOptionalString(record.resolved_instrument_exchange) ??
      normalizeOptionalString(record.resolvedInstrumentExchange),
    current_price:
      normalizeOptionalNumber(record.current_price) ??
      normalizeOptionalNumber(record.currentPrice),
    current_price_currency:
      normalizeOptionalString(record.current_price_currency) ??
      normalizeOptionalString(record.currentPriceCurrency),
    current_price_timestamp:
      normalizeOptionalString(record.current_price_timestamp) ??
      normalizeOptionalString(record.currentPriceTimestamp),
    current_price_source:
      normalizeOptionalString(record.current_price_source) ??
      normalizeOptionalString(record.currentPriceSource),
    current_price_type:
      normalizeOptionalString(record.current_price_type) ??
      normalizeOptionalString(record.currentPriceType),
    resolution_process:
      normalizeOptionalString(record.resolution_process) ??
      normalizeOptionalString(record.resolutionProcess),
    confidence: inferredConfidence,
    explanation,
    reason,
  };
}

const transactionAnalysisResponseObjectSchema = z.object({
  transaction_class: z.string().min(1),
  category_code: z.string().nullable().default(null),
  merchant_normalized: z.string().nullable().default(null),
  counterparty_name: z.string().nullable().default(null),
  economic_entity_override: z.string().nullable().default(null),
  security_hint: z.string().nullable().default(null),
  quantity: z.string().nullable().default(null),
  unit_price_original: z.string().nullable().default(null),
  resolved_instrument_name: z.string().nullable().default(null),
  resolved_instrument_isin: z.string().nullable().default(null),
  resolved_instrument_ticker: z.string().nullable().default(null),
  resolved_instrument_exchange: z.string().nullable().default(null),
  current_price: z.number().nullable().default(null),
  current_price_currency: z.string().nullable().default(null),
  current_price_timestamp: z.string().nullable().default(null),
  current_price_source: z.string().nullable().default(null),
  current_price_type: z.string().nullable().default(null),
  resolution_process: z.string().nullable().default(null),
  confidence: z.number().min(0).max(1),
  explanation: z.string().min(1).max(240),
  reason: z.string().min(1).max(320),
});

export type TransactionAnalysisOutput = z.infer<
  typeof transactionAnalysisResponseObjectSchema
>;

export const transactionAnalysisResponseSchema: z.ZodType<TransactionAnalysisOutput> =
  z.preprocess(
  normalizeTransactionAnalysisPayload,
  transactionAnalysisResponseObjectSchema,
) as z.ZodType<TransactionAnalysisOutput>;

const transactionAnalysisJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: [
    "transaction_class",
    "category_code",
    "merchant_normalized",
    "counterparty_name",
    "economic_entity_override",
    "security_hint",
    "quantity",
    "unit_price_original",
    "resolved_instrument_name",
    "resolved_instrument_isin",
    "resolved_instrument_ticker",
    "resolved_instrument_exchange",
    "current_price",
    "current_price_currency",
    "current_price_timestamp",
    "current_price_source",
    "current_price_type",
    "resolution_process",
    "confidence",
    "explanation",
    "reason",
  ],
  properties: {
    transaction_class: { type: "string" },
    category_code: { type: ["string", "null"] },
    merchant_normalized: { type: ["string", "null"] },
    counterparty_name: { type: ["string", "null"] },
    economic_entity_override: { type: ["string", "null"] },
    security_hint: { type: ["string", "null"] },
    quantity: { type: ["string", "null"] },
    unit_price_original: { type: ["string", "null"] },
    resolved_instrument_name: { type: ["string", "null"] },
    resolved_instrument_isin: { type: ["string", "null"] },
    resolved_instrument_ticker: { type: ["string", "null"] },
    resolved_instrument_exchange: { type: ["string", "null"] },
    current_price: { type: ["number", "null"] },
    current_price_currency: { type: ["string", "null"] },
    current_price_timestamp: { type: ["string", "null"] },
    current_price_source: { type: ["string", "null"] },
    current_price_type: { type: ["string", "null"] },
    resolution_process: { type: ["string", "null"] },
    confidence: { type: "number", minimum: 0, maximum: 1 },
    explanation: { type: "string" },
    reason: { type: "string" },
  },
} satisfies Record<string, unknown>;

export interface AnalyzeBankTransactionInput {
  account: {
    id: string;
    assetDomain: "cash" | "investment";
    institutionName: string;
    displayName: string;
    accountType: string;
  };
  allowedTransactionClasses: readonly string[];
  allowedCategories: ReadonlyArray<{ code: string; displayName: string }>;
  transaction: {
    transactionDate: string;
    postedDate?: string | null;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    merchantNormalized?: string | null;
    counterpartyName?: string | null;
    securityId?: string | null;
    quantity?: string | null;
    unitPriceOriginal?: string | null;
    providerContext?: unknown;
    rawPayload: unknown;
  };
  deterministicHint: {
    transactionClass: string;
    categoryCode?: string | null;
    explanation: string;
    source: string;
  };
  portfolioState?: {
    scope: "account";
    asOfDate: string;
    holdings: Array<{
      securityId: string;
      symbol: string;
      securityName: string;
      quantity: string;
      currentPrice: string | null;
      currentPriceCurrency: string | null;
      currentValueEur: string | null;
      quoteTimestamp: string | null;
      quoteFreshness: "fresh" | "delayed" | "stale" | "missing";
    }>;
    matchedHolding: {
      securityId: string;
      symbol: string;
      securityName: string;
      quantity: string;
      currentPrice: string | null;
      currentPriceCurrency: string | null;
      currentValueEur: string | null;
      quoteTimestamp: string | null;
      quoteFreshness: "fresh" | "delayed" | "stale" | "missing";
    } | null;
    priceSanityCheck: {
      impliedUnitPrice: string | null;
      impliedUnitPriceCurrency: string;
      latestHoldingPrice: string | null;
      latestHoldingPriceCurrency: string | null;
      latestHoldingQuoteTimestamp: string | null;
      priceDeltaPercent: string | null;
    } | null;
  };
  similarAccountTransactions?: Array<{
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
  }>;
  batchContext?: {
    phase: string;
    sourceBatchKey?: string | null;
    batchSummary?: string | null;
    retrievalContext?: string | null;
    totalTransactions?: number | null;
    trustedResolvedCount?: number | null;
  } | null;
  reviewContext?: {
    trigger: string;
    previousReviewReason?: string | null;
    previousUserContext?: string | null;
    userProvidedContext?: string | null;
    previousLlmPayload?: unknown;
    propagatedContexts?: unknown[];
    persistedSecurityMappings?: unknown[];
    resolvedSourcePrecedent?: unknown | null;
  };
  reviewExamples?: Array<{
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
  }>;
  promptOverrides?: Record<string, unknown> | null;
}

export interface AnalyzeBankTransactionResult {
  analysisStatus: "done" | "failed";
  model: string;
  output: TransactionAnalysisOutput | null;
  error: string | null;
  rawOutput: Record<string, unknown> | null;
  provider: "openai" | "gemini";
  statusCode: number | null;
  failureKind: string | null;
}

export async function analyzeBankTransaction(
  client: LLMTaskClient,
  input: AnalyzeBankTransactionInput,
  modelName: string,
) {
  try {
    const enableWebSearch =
      input.account.assetDomain === "investment" &&
      resolveModelProvider(modelName) === "openai";
    const prompt = renderTransactionAnalyzerPrompt(input.account.assetDomain, {
      institutionName: input.account.institutionName,
      accountDisplayName: input.account.displayName,
      accountType: input.account.accountType,
      accountId: input.account.id,
      allowedTransactionClasses: input.allowedTransactionClasses.join(", "),
      allowedCategoryCodes: input.allowedCategories
        .map((category) => `${category.code} (${category.displayName})`)
        .join(", "),
      transactionDate: input.transaction.transactionDate,
      postedDate: input.transaction.postedDate ?? "null",
      amountOriginal: input.transaction.amountOriginal,
      currencyOriginal: input.transaction.currencyOriginal,
      descriptionRaw: input.transaction.descriptionRaw,
      merchantNormalized: input.transaction.merchantNormalized ?? "null",
      counterpartyName: input.transaction.counterpartyName ?? "null",
      securityId: input.transaction.securityId ?? "null",
      quantity: input.transaction.quantity ?? "null",
      unitPriceOriginal: input.transaction.unitPriceOriginal ?? "null",
      providerContext: JSON.stringify(input.transaction.providerContext ?? null),
      rawPayload: JSON.stringify(input.transaction.rawPayload),
      deterministicHint: JSON.stringify(input.deterministicHint),
      portfolioState: JSON.stringify(input.portfolioState ?? null),
      similarAccountHistory: JSON.stringify(
        input.similarAccountTransactions ?? [],
      ),
      batchContext: input.batchContext
        ? {
            phase: input.batchContext.phase,
            sourceBatchKey: input.batchContext.sourceBatchKey ?? "unknown",
            batchSummary: input.batchContext.batchSummary ?? "none",
            retrievalContext: input.batchContext.retrievalContext ?? "none",
            totalTransactions: String(input.batchContext.totalTransactions ?? 0),
            trustedResolvedCount: String(
              input.batchContext.trustedResolvedCount ?? 0,
            ),
          }
        : null,
      reviewExamples:
        input.reviewExamples?.map((example) => ({
          transaction: JSON.stringify(example.transaction),
          initialInference: JSON.stringify(example.initialInference),
          userFeedback: example.userFeedback,
          correctedOutcome: JSON.stringify(example.correctedOutcome),
        })) ?? [],
      reviewContext: input.reviewContext
        ? {
            trigger: input.reviewContext.trigger,
            previousReviewReason:
              input.reviewContext.previousReviewReason ?? "null",
            previousUserContext:
              input.reviewContext.previousUserContext ?? "null",
            userProvidedContext:
              input.reviewContext.userProvidedContext ?? "null",
            previousLlmPayload: JSON.stringify(
              input.reviewContext.previousLlmPayload ?? null,
            ),
            propagatedContexts: JSON.stringify(
              input.reviewContext.propagatedContexts ?? [],
            ),
            persistedSecurityMappings: JSON.stringify(
              input.reviewContext.persistedSecurityMappings ?? [],
            ),
            resolvedSourcePrecedent: JSON.stringify(
              input.reviewContext.resolvedSourcePrecedent ?? null,
            ),
          }
        : null,
      promptOverrides: input.promptOverrides ?? null,
    });

    const output = await client.generateJson<TransactionAnalysisOutput>({
      systemPrompt: prompt.systemPrompt,
      userPrompt: prompt.userPrompt,
      modelName,
      responseSchema: transactionAnalysisResponseSchema,
      responseJsonSchema: transactionAnalysisJsonSchema,
      schemaName: "transaction_enrichment",
      temperature: 0,
      tools: enableWebSearch ? [{ type: "web_search" }] : undefined,
      toolChoice: enableWebSearch ? "auto" : undefined,
    });

    return {
      analysisStatus: "done",
      model: modelName,
      output,
      error: null,
      rawOutput: output as unknown as Record<string, unknown>,
      provider: resolveModelProvider(modelName),
      statusCode: null,
      failureKind: null,
    } satisfies AnalyzeBankTransactionResult;
  } catch (error) {
    return {
      analysisStatus: "failed",
      model: modelName,
      output: null,
      error:
        error instanceof Error
          ? error.message
          : "Unknown LLM classification failure.",
      rawOutput:
        error instanceof LLMError
          ? error.providerError ??
            (error.rawOutput ? { invalidOutput: error.rawOutput } : null)
          : null,
      provider:
        error instanceof LLMError
          ? error.provider
          : resolveModelProvider(modelName),
      statusCode: error instanceof LLMError ? error.statusCode ?? null : null,
      failureKind: error instanceof LLMError ? error.kind : null,
    } satisfies AnalyzeBankTransactionResult;
  }
}

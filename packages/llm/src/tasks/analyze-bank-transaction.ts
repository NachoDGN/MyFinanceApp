import { z } from "zod";

import { resolveModelProvider } from "../config";
import { renderTransactionAnalyzerPrompt } from "../prompts";
import { LLMError, type LLMTaskClient } from "../types";

export const transactionAnalysisResponseSchema = z.object({
  transaction_class: z.string().min(1),
  category_code: z.string().nullable(),
  merchant_normalized: z.string().nullable(),
  counterparty_name: z.string().nullable(),
  economic_entity_override: z.string().nullable(),
  security_hint: z.string().nullable(),
  resolved_instrument_name: z.string().nullable().optional(),
  resolved_instrument_isin: z.string().nullable().optional(),
  resolved_instrument_ticker: z.string().nullable().optional(),
  resolved_instrument_exchange: z.string().nullable().optional(),
  current_price: z.number().nullable().optional(),
  current_price_currency: z.string().nullable().optional(),
  current_price_timestamp: z.string().nullable().optional(),
  current_price_source: z.string().nullable().optional(),
  current_price_type: z.string().nullable().optional(),
  confidence: z.number().min(0).max(1),
  explanation: z.string().min(1).max(240),
  reason: z.string().min(1).max(320),
});

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
    "resolved_instrument_name",
    "resolved_instrument_isin",
    "resolved_instrument_ticker",
    "resolved_instrument_exchange",
    "current_price",
    "current_price_currency",
    "current_price_timestamp",
    "current_price_source",
    "current_price_type",
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
    resolved_instrument_name: { type: ["string", "null"] },
    resolved_instrument_isin: { type: ["string", "null"] },
    resolved_instrument_ticker: { type: ["string", "null"] },
    resolved_instrument_exchange: { type: ["string", "null"] },
    current_price: { type: ["number", "null"] },
    current_price_currency: { type: ["string", "null"] },
    current_price_timestamp: { type: ["string", "null"] },
    current_price_source: { type: ["string", "null"] },
    current_price_type: { type: ["string", "null"] },
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
  }>;
  reviewContext?: {
    trigger: string;
    previousReviewReason?: string | null;
    previousUserContext?: string | null;
    userProvidedContext?: string | null;
    previousLlmPayload?: unknown;
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

export type TransactionAnalysisOutput = z.infer<
  typeof transactionAnalysisResponseSchema
>;

export interface AnalyzeBankTransactionResult {
  analysisStatus: "done" | "failed";
  model: string;
  output: TransactionAnalysisOutput | null;
  error: string | null;
  rawOutput: Record<string, unknown> | null;
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
      rawPayload: JSON.stringify(input.transaction.rawPayload),
      deterministicHint: JSON.stringify(input.deterministicHint),
      portfolioState: JSON.stringify(input.portfolioState ?? null),
      similarAccountHistory: JSON.stringify(
        input.similarAccountTransactions ?? [],
      ),
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
          }
        : null,
      promptOverrides: input.promptOverrides ?? null,
    });

    const output = await client.generateJson({
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
      rawOutput: output,
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
        error instanceof LLMError && error.rawOutput
          ? { invalidOutput: error.rawOutput }
          : null,
    } satisfies AnalyzeBankTransactionResult;
  }
}

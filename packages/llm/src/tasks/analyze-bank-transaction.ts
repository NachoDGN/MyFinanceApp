import { z } from "zod";

import { LLMError, type LLMTaskClient } from "../types";

export const transactionAnalysisResponseSchema = z.object({
  transaction_class: z.string().min(1),
  category_code: z.string().nullable(),
  merchant_normalized: z.string().nullable(),
  counterparty_name: z.string().nullable(),
  economic_entity_override: z.string().nullable(),
  security_hint: z.string().nullable(),
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

function buildSystemPrompt(assetDomain: "cash" | "investment") {
  return [
    assetDomain === "investment"
      ? "You classify brokerage and investment account transactions with security-aware structured output."
      : "You classify cash and company account transactions into existing taxonomy codes only.",
    "Return one strict JSON object only.",
    "Use only the allowed transaction classes and category codes provided.",
    "Use null instead of guessing unsupported values.",
    "Keep the explanation to one short sentence.",
    assetDomain === "investment"
      ? [
          "Treat clearly named stocks, ETFs, index funds, and mutual funds as investment transactions even when ticker or quantity is missing.",
          "Use security_hint for the best normalized issuer or fund name visible in the description.",
          "If the instrument is recognizable but the exact catalog mapping is uncertain, still classify the transaction and explain the remaining ambiguity in reason.",
          "Never invent security ids or ticker symbols.",
          "Use the latest portfolio snapshot when provided to sanity-check whether the row can realistically be a buy, sell, or fee.",
          "Broker commissions can mention a security name and quantity without being a real disposal.",
          "If a positive row implies a per-share price that is far below the latest quote for a still-held security, classify it as fee instead of investment_trade_sell unless the row clearly states a real sale.",
          "You are a financial instrument identification expert. When you receive a partial asset name or description, do not provide a single best-guess ISIN or ticker unless the identification is totally clear.",
          "First decompose the instrument into issuer, benchmark index, and geographic region. Then identify the plausible vehicles, explicitly distinguishing ETFs from mutual funds.",
          "For each plausible vehicle, call out the variables that change the ISIN, including dividend treatment, legal domicile, and share class. If the description is still ambiguous, explain exactly what information is missing instead of making assumptions.",
        ].join(" ")
      : "Never invent merchants, counterparties, or categories.",
  ].join(" ");
}

function buildUserPrompt(input: AnalyzeBankTransactionInput) {
  const reviewExamples =
    input.reviewExamples && input.reviewExamples.length > 0
      ? [
          "Examples from prior user corrections:",
          ...input.reviewExamples.flatMap((example, index) => [
            `Example ${index + 1} transaction metadata: ${JSON.stringify(example.transaction)}.`,
            `Example ${index + 1} initial inference: ${JSON.stringify(example.initialInference)}.`,
            `Example ${index + 1} user feedback: ${example.userFeedback}.`,
            `Example ${index + 1} corrected outcome: ${JSON.stringify(example.correctedOutcome)}.`,
          ]),
        ]
      : [];
  const reviewContext = input.reviewContext
    ? [
        `Review trigger: ${input.reviewContext.trigger}.`,
        `Previous review reason: ${input.reviewContext.previousReviewReason ?? "null"}.`,
        `Previous user review context: ${input.reviewContext.previousUserContext ?? "null"}.`,
        `New user review context: ${input.reviewContext.userProvidedContext ?? "null"}.`,
        `Previous LLM analysis: ${JSON.stringify(input.reviewContext.previousLlmPayload ?? null)}.`,
      ]
    : [];

  return [
    `Institution: ${input.account.institutionName}.`,
    `Account: ${input.account.displayName}.`,
    `Account type: ${input.account.accountType}.`,
    `Account id: ${input.account.id}.`,
    `Allowed transaction classes: ${input.allowedTransactionClasses.join(", ")}.`,
    `Allowed category codes: ${input.allowedCategories
      .map((category) => `${category.code} (${category.displayName})`)
      .join(", ")}.`,
    `Transaction date: ${input.transaction.transactionDate}.`,
    `Posted date: ${input.transaction.postedDate ?? "null"}.`,
    `Amount: ${input.transaction.amountOriginal} ${input.transaction.currencyOriginal}.`,
    `Description: ${input.transaction.descriptionRaw}.`,
    `Existing merchant: ${input.transaction.merchantNormalized ?? "null"}.`,
    `Existing counterparty: ${input.transaction.counterpartyName ?? "null"}.`,
    `Security id: ${input.transaction.securityId ?? "null"}.`,
    `Quantity: ${input.transaction.quantity ?? "null"}.`,
    `Unit price: ${input.transaction.unitPriceOriginal ?? "null"}.`,
    input.account.assetDomain === "investment"
      ? "For investment accounts, prefer investment_trade_buy or investment_trade_sell when a company, fund, ETF, or index instrument is clearly named. Use transfer_internal for broker cash movements between owned accounts and leave statement-period rows as unknown."
      : "For cash accounts, do not use investment classes unless the transaction data explicitly supports them.",
    `Current raw payload: ${JSON.stringify(input.transaction.rawPayload)}.`,
    `Deterministic hint: ${JSON.stringify(input.deterministicHint)}.`,
    `Portfolio state: ${JSON.stringify(input.portfolioState ?? null)}.`,
    ...reviewExamples,
    ...reviewContext,
  ].join("\n");
}

export async function analyzeBankTransaction(
  client: LLMTaskClient,
  input: AnalyzeBankTransactionInput,
  modelName: string,
) {
  try {
    const output = await client.generateJson({
      systemPrompt: buildSystemPrompt(input.account.assetDomain),
      userPrompt: buildUserPrompt(input),
      modelName,
      responseSchema: transactionAnalysisResponseSchema,
      responseJsonSchema: transactionAnalysisJsonSchema,
      schemaName: "transaction_enrichment",
      temperature: 0,
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

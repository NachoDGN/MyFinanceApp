import { z } from "zod";

import { LLMError, type LLMTaskClient } from "../types";

export const transactionAnalysisResponseSchema = z.object({
  transaction_class: z.string().min(1),
  category_code: z.string().nullable().optional(),
  merchant_normalized: z.string().nullable().optional(),
  counterparty_name: z.string().nullable().optional(),
  economic_entity_override: z.string().nullable().optional(),
  security_hint: z.string().nullable().optional(),
  confidence: z.number().min(0).max(1),
  explanation: z.string().min(1).max(240),
  reason: z.string().min(1).max(320),
});

const transactionAnalysisJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: ["transaction_class", "confidence", "explanation", "reason"],
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
}

export type TransactionAnalysisOutput = z.infer<typeof transactionAnalysisResponseSchema>;

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
      ? "Never invent security ids or ticker symbols."
      : "Never invent merchants, counterparties, or categories.",
  ].join(" ");
}

function buildUserPrompt(input: AnalyzeBankTransactionInput) {
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
    `Current raw payload: ${JSON.stringify(input.transaction.rawPayload)}.`,
    `Deterministic hint: ${JSON.stringify(input.deterministicHint)}.`,
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
      error: error instanceof Error ? error.message : "Unknown LLM classification failure.",
      rawOutput:
        error instanceof LLMError && error.rawOutput
          ? { invalidOutput: error.rawOutput }
          : null,
    } satisfies AnalyzeBankTransactionResult;
  }
}

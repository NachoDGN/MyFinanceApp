import { z } from "zod";

import { resolveModelProvider } from "../config";
import { LLMError, type LLMTaskClient } from "../types";

const historicalFundPriceResponseSchema = z.object({
  isin: z.string().min(1),
  target_date: z.string().min(1),
  security: z.string().nullable(),
  security_type: z.string().nullable(),
  share_class: z.string().nullable(),
  currency: z.string().nullable(),
  historical_nav: z.number().nullable(),
  historical_nav_date: z.string().nullable(),
  match_status: z.enum(["exact", "unresolved", "ambiguous"]),
  identity_source: z.object({
    name: z.string().nullable(),
    url: z.string().nullable(),
  }),
  historical_price_source: z.object({
    name: z.string().nullable(),
    url: z.string().nullable(),
  }),
  explanation: z.string().nullable(),
});

const historicalFundPriceJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: [
    "isin",
    "target_date",
    "security",
    "security_type",
    "share_class",
    "currency",
    "historical_nav",
    "historical_nav_date",
    "match_status",
    "identity_source",
    "historical_price_source",
    "explanation",
  ],
  properties: {
    isin: { type: "string" },
    target_date: { type: "string" },
    security: { type: ["string", "null"] },
    security_type: { type: ["string", "null"] },
    share_class: { type: ["string", "null"] },
    currency: { type: ["string", "null"] },
    historical_nav: { type: ["number", "null"] },
    historical_nav_date: { type: ["string", "null"] },
    match_status: {
      type: "string",
      enum: ["exact", "unresolved", "ambiguous"],
    },
    identity_source: {
      type: "object",
      additionalProperties: false,
      required: ["name", "url"],
      properties: {
        name: { type: ["string", "null"] },
        url: { type: ["string", "null"] },
      },
    },
    historical_price_source: {
      type: "object",
      additionalProperties: false,
      required: ["name", "url"],
      properties: {
        name: { type: ["string", "null"] },
        url: { type: ["string", "null"] },
      },
    },
    explanation: { type: ["string", "null"] },
  },
} satisfies Record<string, unknown>;

export interface LookupHistoricalFundPriceInput {
  isin: string;
  targetDate: string;
  securityNameHint?: string | null;
  transactionDescription?: string | null;
  transactionCurrency?: string | null;
}

export type HistoricalFundPriceOutput = z.infer<
  typeof historicalFundPriceResponseSchema
>;

export interface LookupHistoricalFundPriceResult {
  analysisStatus: "done" | "failed";
  model: string;
  output: HistoricalFundPriceOutput | null;
  error: string | null;
  rawOutput: Record<string, unknown> | null;
}

function buildSystemPrompt() {
  return [
    "You are an evidence-driven fund-lookup agent.",
    "Your task is to find the historical NAV per share for the exact fund identified by a given ISIN on a specific date.",
    "Treat the ISIN as the primary identifier and confirm the exact security from authoritative sources before looking for the historical NAV.",
    "Resolve identity first, then historical pricing.",
    "Do not infer the fund from name similarity.",
    "Prioritize official issuer pages, official factsheets, KIIDs/KIDs, prospectuses, regulator-grade listings, and reputable fund databases that explicitly reference the same ISIN.",
    "For pricing, prefer official issuer price-history pages, official daily price tables, historical NAV pages, official past-prices tools, and issuer-hosted CSV or downloadable price files over identity-only documents.",
    "Do not stop after finding a factsheet or brochure that proves identity if it does not expose the dated NAV per share you need.",
    "When an official factsheet confirms the ISIN but does not contain the requested historical NAV, continue searching official issuer pricing pages using the exact ISIN plus the target date and terms such as NAV, historical price, price history, past prices, daily prices, or valuation.",
    "If the issuer has both factsheets and dedicated prices pages, use the factsheet for identity and the prices page for the dated NAV.",
    "Once identity is locked from the exact ISIN, reputable fund-history pages keyed to that same ISIN and share-class currency, such as Financial Times fund tearsheets on markets.ft.com/data/funds/tearsheet/historical, are acceptable sources for the dated NAV when official issuer pages do not expose an accessible historical value.",
    "For Vanguard and similar fund issuers, explicitly look for official prices or price-history pages rather than stopping at PDF factsheets that summarize performance only.",
    "If the security is an open-ended fund or mutual/index fund, retrieve NAV, not a market quote.",
    "If the security is an ETF, retrieve the correct historical market price for the relevant listing and do not mix it with mutual-fund NAV data.",
    "Never guess, never interpolate, never derive the value from returns, and never return a current NAV when asked for a historical one.",
    "Never return a value unless it is tied to the same ISIN.",
    "If evidence is conflicting or incomplete, return null for the historical NAV and explain why.",
    "If the requested date had no published value, you may return the nearest prior official valuation date only when the source makes that explicit, and you must say that clearly in the explanation.",
    "Return one strict JSON object only.",
  ].join(" ");
}

function buildUserPrompt(input: LookupHistoricalFundPriceInput) {
  return [
    `ISIN = ${input.isin}`,
    `TARGET_DATE = ${input.targetDate}`,
    `SECURITY_NAME_HINT = ${input.securityNameHint ?? "null"}`,
    `TRANSACTION_DESCRIPTION = ${input.transactionDescription ?? "null"}`,
    `TRANSACTION_CURRENCY = ${input.transactionCurrency ?? "null"}`,
    "SEARCH_WORKFLOW = 1) confirm identity with the exact ISIN, 2) search official issuer price-history or NAV pages for the exact ISIN on the target date, 3) only if the target date has no published value, search the nearest prior official valuation date and state that explicitly.",
    "PRICE_SEARCH_TERMS = exact ISIN + target date + NAV + historical price + price history + past prices + daily prices + valuation.",
    "SECONDARY_PRICE_SOURCE_HINT = after identity is exact, search markets.ft.com/data/funds/tearsheet/historical with the exact ISIN and matching share-class currency when official issuer pricing pages do not expose the dated NAV.",
    "Return JSON only in the required schema.",
  ].join("\n");
}

export async function lookupHistoricalFundPrice(
  client: LLMTaskClient,
  input: LookupHistoricalFundPriceInput,
  modelName: string,
) {
  try {
    const enableWebSearch = resolveModelProvider(modelName) === "openai";
    const output = await client.generateJson({
      systemPrompt: buildSystemPrompt(),
      userPrompt: buildUserPrompt(input),
      modelName,
      responseSchema: historicalFundPriceResponseSchema,
      responseJsonSchema: historicalFundPriceJsonSchema,
      schemaName: "historical_fund_price_lookup",
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
    } satisfies LookupHistoricalFundPriceResult;
  } catch (error) {
    return {
      analysisStatus: "failed",
      model: modelName,
      output: null,
      error:
        error instanceof Error
          ? error.message
          : "Unknown historical fund price lookup failure.",
      rawOutput:
        error instanceof LLMError
          ? error.providerError ??
            (error.rawOutput ? { invalidOutput: error.rawOutput } : null)
          : null,
    } satisfies LookupHistoricalFundPriceResult;
  }
}

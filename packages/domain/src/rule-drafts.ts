import { z } from "zod";

import type { DomainDataset, RuleDraftParseResult } from "./types";

const supportedConditionKeys = [
  "normalized_description_regex",
  "merchant_equals",
  "counterparty_equals",
  "amount_sign",
  "amount_min",
  "amount_max",
  "account_id",
  "account_type",
  "current_category_code",
] as const;

const supportedOutputKeys = [
  "transaction_class",
  "category_code",
  "merchant_normalized",
  "counterparty_name",
  "economic_entity_id_override",
  "review_suppression",
] as const;

const ruleDraftResponseSchema = z.object({
  title: z.string().min(1).max(120),
  summary: z.string().min(1).max(240),
  priority: z.number().int().min(1).max(999),
  scope_json: z.record(z.string(), z.unknown()),
  conditions_json: z.record(z.string(), z.unknown()),
  outputs_json: z.record(z.string(), z.unknown()),
  confidence: z.number().min(0).max(1),
  explanation: z.array(z.string()).max(6).default([]),
});

function toJsonSchema() {
  return {
    type: "object",
    additionalProperties: false,
    required: [
      "title",
      "summary",
      "priority",
      "scope_json",
      "conditions_json",
      "outputs_json",
      "confidence",
      "explanation",
    ],
    properties: {
      title: { type: "string" },
      summary: { type: "string" },
      priority: { type: "integer", minimum: 1, maximum: 999 },
      scope_json: { type: "object", additionalProperties: true },
      conditions_json: { type: "object", additionalProperties: true },
      outputs_json: { type: "object", additionalProperties: true },
      confidence: { type: "number", minimum: 0, maximum: 1 },
      explanation: {
        type: "array",
        items: { type: "string" },
        maxItems: 6,
      },
    },
  };
}

function extractResponseText(payload: Record<string, unknown>) {
  if (typeof payload.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text;
  }

  const output = Array.isArray(payload.output) ? payload.output : [];
  for (const item of output) {
    if (!item || typeof item !== "object") continue;
    const content = Array.isArray((item as { content?: unknown[] }).content)
      ? (item as { content: unknown[] }).content
      : [];
    for (const chunk of content) {
      if (!chunk || typeof chunk !== "object") continue;
      const textValue = (chunk as { text?: unknown }).text;
      if (typeof textValue === "string" && textValue.trim()) {
        return textValue;
      }
    }
  }

  throw new Error("The model response did not contain a structured JSON payload.");
}

export function getRuleParserConfig() {
  return {
    apiKey: process.env.OPENAI_API_KEY ?? "",
    model: process.env.OPENAI_RULES_MODEL ?? "gpt-4.1-mini",
  };
}

export function isRuleParserConfigured() {
  return Boolean(getRuleParserConfig().apiKey);
}

function buildRulePrompt(requestText: string, dataset: DomainDataset) {
  return [
    "Convert the user's natural-language rule request into deterministic transaction rule logic.",
    "Use only the supported condition keys and output keys provided.",
    "Do not invent taxonomy codes, entity ids, account ids, or transaction classes.",
    "If the request is ambiguous, make the narrowest safe rule and lower confidence.",
    "",
    `Supported condition keys: ${supportedConditionKeys.join(", ")}`,
    `Supported output keys: ${supportedOutputKeys.join(", ")}`,
    `Allowed transaction classes: ${[
      "income",
      "expense",
      "transfer_internal",
      "transfer_external",
      "investment_trade_buy",
      "investment_trade_sell",
      "dividend",
      "interest",
      "fee",
      "refund",
      "reimbursement",
      "owner_contribution",
      "owner_draw",
      "loan_inflow",
      "loan_principal_payment",
      "loan_interest_payment",
      "fx_conversion",
      "balance_adjustment",
      "unknown",
    ].join(", ")}`,
    `Allowed category codes: ${dataset.categories.map((category) => category.code).join(", ")}`,
    `Entities: ${dataset.entities
      .map((entity) => `${entity.displayName} [slug=${entity.slug}, id=${entity.id}]`)
      .join("; ")}`,
    `Accounts: ${dataset.accounts
      .map(
        (account) =>
          `${account.displayName} [id=${account.id}, type=${account.accountType}, institution=${account.institutionName}]`,
      )
      .join("; ")}`,
    "",
    `User request: ${requestText}`,
  ].join("\n");
}

function fallbackRuleDraft(requestText: string, dataset: DomainDataset): RuleDraftParseResult {
  const normalized = requestText.toUpperCase();
  const merchantMatch =
    requestText.match(/contains\s+["']?([^,"'\n]+?)["']?(?:\sand\b|\son\b|\sclassify\b|,|$)/i)?.[1] ??
    requestText.match(/merchant\s+["']?([^,"'\n]+?)["']?(?:\sand\b|\son\b|\sclassify\b|,|$)/i)?.[1] ??
    requestText.match(/description\s+["']?([^,"'\n]+?)["']?(?:\sand\b|\son\b|\sclassify\b|,|$)/i)?.[1] ??
    requestText.match(/"([^"]+)"/)?.[1] ??
    requestText.match(/'([^']+)'/)?.[1] ??
    null;

  const matchedCategory = dataset.categories.find((category) => {
    const label = category.displayName.toUpperCase().replace(/&/g, "AND");
    return normalized.includes(category.code.toUpperCase()) || normalized.includes(label);
  });
  const matchedEntity = [...dataset.entities]
    .map((entity) => ({
      entity,
      score: [
        normalized.includes(entity.slug.toUpperCase()) ? 2 : 0,
        normalized.includes(entity.displayName.toUpperCase()) ? 3 : 0,
        entity.entityKind === "company" && normalized.includes(entity.displayName.toUpperCase()) ? 3 : 0,
      ].reduce((sum, score) => sum + score, 0),
    }))
    .sort(
      (left, right) =>
        right.score - left.score || right.entity.displayName.length - left.entity.displayName.length,
    )
    .find((item) => item.score > 0)?.entity;
  const matchedAccount = [...dataset.accounts]
    .map((account) => ({
      account,
      score: [
        normalized.includes(account.institutionName.toUpperCase()) ? 2 : 0,
        normalized.includes(account.displayName.toUpperCase()) ? 3 : 0,
        account.accountSuffix ? (normalized.includes(account.accountSuffix.toUpperCase()) ? 2 : 0) : 0,
      ].reduce((sum, score) => sum + score, 0),
    }))
    .sort((left, right) => right.score - left.score)
    .find((item) => item.score > 0)?.account;
  const transactionClass = normalized.includes("DIVIDEND")
    ? "dividend"
    : normalized.includes("INTEREST")
      ? "interest"
      : normalized.includes("INCOME") || normalized.includes("PAYMENT")
        ? "income"
        : "expense";

  const conditionsJson: Record<string, unknown> = merchantMatch
    ? { normalized_description_regex: merchantMatch.toUpperCase().trim().replace(/\s+/g, ".*") }
    : { normalized_description_regex: normalized.split(/\s+/).slice(0, 4).join(".*") };
  const scopeJson: Record<string, unknown> = matchedAccount
    ? { account_id: matchedAccount.id }
    : matchedEntity
      ? { entity_id: matchedEntity.id }
      : { global: true };

  const outputsJson: Record<string, unknown> = {
    transaction_class: transactionClass,
    category_code:
      matchedCategory?.code ??
      (transactionClass === "income" ? "uncategorized_income" : "uncategorized_expense"),
  };
  if (matchedEntity) {
    outputsJson.economic_entity_id_override = matchedEntity.id;
  }
  if (merchantMatch) {
    outputsJson.merchant_normalized = merchantMatch.trim().toUpperCase();
  }

  return {
    title: merchantMatch ? `Rule for ${merchantMatch}` : "Drafted rule",
    summary: "Fallback parser created a narrow draft because no OpenAI key is configured.",
    priority: 60,
    scopeJson,
    conditionsJson,
    outputsJson,
    confidence: "0.55",
    explanation: [
      "Fallback parser inferred category and class from the request text.",
      "Review the generated regex and scope before applying.",
    ],
    parseSource: "fallback",
    model: null,
    generatedAt: new Date().toISOString(),
  };
}

export async function parseRuleDraftRequest(
  requestText: string,
  dataset: DomainDataset,
): Promise<RuleDraftParseResult> {
  const { apiKey, model } = getRuleParserConfig();
  if (!apiKey) {
    return fallbackRuleDraft(requestText, dataset);
  }

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: buildRulePrompt(requestText, dataset) }],
        },
      ],
      text: {
        format: {
          type: "json_schema",
          name: "rule_draft_parse",
          schema: toJsonSchema(),
          strict: true,
        },
      },
    }),
  });

  if (!response.ok) {
    throw new Error(`OpenAI rule parser failed with status ${response.status}.`);
  }

  const payload = (await response.json()) as Record<string, unknown>;
  const parsed = ruleDraftResponseSchema.parse(JSON.parse(extractResponseText(payload)));

  return {
    title: parsed.title,
    summary: parsed.summary,
    priority: parsed.priority,
    scopeJson: parsed.scope_json,
    conditionsJson: parsed.conditions_json,
    outputsJson: parsed.outputs_json,
    confidence: parsed.confidence.toFixed(2),
    explanation: parsed.explanation,
    parseSource: "llm",
    model,
    generatedAt: new Date().toISOString(),
  };
}

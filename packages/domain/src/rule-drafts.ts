import { createLLMClient, isModelConfigured, parseRuleDraftWithLLM } from "@myfinance/llm";

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

export function getRuleParserConfig() {
  return {
    model: process.env.LLM_RULES_MODEL ?? process.env.OPENAI_RULES_MODEL ?? "gpt-4.1-mini",
  };
}

export function isRuleParserConfigured() {
  return isModelConfigured(getRuleParserConfig().model);
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
    summary: "Fallback parser created a narrow draft because no LLM credentials are configured.",
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
  const { model } = getRuleParserConfig();
  if (!isModelConfigured(model)) {
    return fallbackRuleDraft(requestText, dataset);
  }

  const parsed = await parseRuleDraftWithLLM(
    createLLMClient(),
    {
      requestText,
      supportedConditionKeys,
      supportedOutputKeys,
      allowedTransactionClasses: [
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
      ],
      allowedCategoryCodes: dataset.categories.map((category) => category.code),
      entities: dataset.entities.map((entity) => ({
        displayName: entity.displayName,
        slug: entity.slug,
        id: entity.id,
      })),
      accounts: dataset.accounts.map((account) => ({
        displayName: account.displayName,
        id: account.id,
        accountType: account.accountType,
        institutionName: account.institutionName,
      })),
    },
    model,
  );

  return {
    title: parsed.title,
    summary: parsed.summary,
    priority: parsed.priority,
    scopeJson: parsed.scope_json,
    conditionsJson: parsed.conditions_json,
    outputsJson: parsed.outputs_json,
    confidence: parsed.confidence.toFixed(2),
    explanation: parsed.explanation ?? [],
    parseSource: "llm",
    model,
    generatedAt: new Date().toISOString(),
  };
}

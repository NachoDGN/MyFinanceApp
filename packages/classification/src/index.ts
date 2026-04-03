import { z } from "zod";

import type { Account, ClassificationRule, DomainDataset, Scope, Transaction } from "@myfinance/domain";

export const NON_AI_RULE_SUMMARIES = [
  {
    id: "description_normalization",
    title: "Description normalization",
    summary:
      "Whitespace, repeated spaces, SEPA markers, and card-ending boilerplate are normalized before any rule logic runs.",
    evidence: ["trim", "collapse spaces", "remove SEPA", "remove CARD ENDING ####"],
  },
  {
    id: "saved_rule_engine",
    title: "Saved rule engine",
    summary:
      "Active user rules are evaluated by priority and can currently match normalized-description regexes and merchant equality.",
    evidence: ["normalized_description_regex", "merchant_equals", "priority ascending"],
  },
  {
    id: "transfer_matcher",
    title: "Internal transfer matcher",
    summary:
      "Owned-account transfers are detected by opposite sign, near date, similar amount, currency match, and account alias hints.",
    evidence: ["3-day window", "same currency", "opposite sign", "matching aliases"],
  },
  {
    id: "investment_parser",
    title: "Investment parser",
    summary:
      "Brokerage rows are deterministically parsed for buy, sell, dividend, interest, fee, and FX conversion patterns before any LLM is used.",
    evidence: ["DIVIDEND", "INTEREST", "FEE", "FX", "@ quantity", "BUY/SELL quantity name"],
  },
  {
    id: "fallback_buckets",
    title: "Fallback buckets",
    summary:
      "When deterministic logic cannot safely decide, the system falls back to unknown or uncategorized codes instead of inventing categories.",
    evidence: ["unknown", "uncategorized_income", "uncategorized_expense", "uncategorized_investment"],
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

export function normalizeDescription(input: string): {
  raw: string;
  clean: string;
  comparison: string;
} {
  const clean = input
    .trim()
    .replace(/\s+/g, " ")
    .replace(/\bSEPA\b/gi, "")
    .replace(/\bCARD ENDING \d{4}\b/gi, "")
    .trim();

  return {
    raw: input,
    clean,
    comparison: clean.toUpperCase(),
  };
}

export function applyRuleMatch(
  transaction: Transaction,
  rules: ClassificationRule[],
): ClassificationRule | null {
  const ordered = [...rules]
    .filter((rule) => rule.active)
    .sort((a, b) => a.priority - b.priority);

  const comparison = normalizeDescription(transaction.descriptionRaw).comparison;

  for (const rule of ordered) {
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

  return candidateRows.find((candidate) => {
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
        Math.abs(Number(transaction.amountBaseEur)) - Math.abs(Number(candidate.amountBaseEur)),
      ) < 0.01;
    const sameCurrency = candidate.currencyOriginal === transaction.currencyOriginal;
    const aliasHint = ownedAccounts.some(
      (account) =>
        account.id === candidate.accountId &&
        account.matchingAliases.some((alias) =>
          normalizeDescription(transaction.descriptionRaw).comparison.includes(alias.toUpperCase()),
        ),
    );
    return dateDelta && oppositeSign && sameMagnitude && sameCurrency && aliasHint;
  }) ?? null;
}

export function parseInvestmentEvent(transaction: Transaction): {
  transactionClass:
    | "investment_trade_buy"
    | "investment_trade_sell"
    | "dividend"
    | "interest"
    | "fee"
    | "fx_conversion"
    | "unknown";
  quantity?: string;
  securityHint?: string;
  unitPriceOriginal?: string;
} {
  const comparison = normalizeDescription(transaction.descriptionRaw).comparison;

  if (comparison.includes("DIVIDEND")) {
    return { transactionClass: "dividend" };
  }
  if (comparison.includes("INTEREST")) {
    return { transactionClass: "interest" };
  }
  if (comparison.includes("COMMISSION") || comparison.includes("FEE")) {
    return { transactionClass: "fee" };
  }
  if (comparison.includes("FX") || comparison.includes("CONVERSION")) {
    return { transactionClass: "fx_conversion" };
  }

  const quantityMatch = comparison.match(/@\s*([0-9]+(?:\.[0-9]+)?)/);
  const buyMatch = comparison.match(/(BUY|SELL)\s+([0-9]+(?:\.[0-9]+)?)\s+(.+)/);

  if (quantityMatch) {
    const quantity = quantityMatch[1];
    const securityHint = comparison.split("@")[0]?.trim() ?? comparison;
    const gross = Math.abs(Number(transaction.amountOriginal));
    const unitPriceOriginal = quantity === "0" ? undefined : (gross / Number(quantity)).toFixed(2);
    return {
      transactionClass: Number(transaction.amountOriginal) < 0 ? "investment_trade_buy" : "investment_trade_sell",
      quantity,
      securityHint,
      unitPriceOriginal,
    };
  }

  if (buyMatch) {
    return {
      transactionClass: buyMatch[1] === "BUY" ? "investment_trade_buy" : "investment_trade_sell",
      quantity: buyMatch[2],
      securityHint: buyMatch[3],
    };
  }

  return { transactionClass: "unknown" };
}

export function buildCashAccountPrompt(scope: Scope): string {
  return [
    "You classify cash and company account transactions into existing taxonomy codes only.",
    `Scope kind: ${scope.kind}.`,
    "Return JSON only. If uncertain, use unknown or uncategorized codes.",
  ].join(" ");
}

export function buildInvestmentAccountPrompt(scope: Scope): string {
  return [
    "You classify brokerage and investment account transactions with security-aware structured output.",
    `Scope kind: ${scope.kind}.`,
    "Return JSON only. Never invent categories or security symbols.",
  ].join(" ");
}

const allowedTransactionClasses = [
  "income",
  "expense",
  "transfer_internal",
  "transfer_external",
  "suspected_internal_transfer_pending",
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
] as const;

const transactionEnrichmentResponseSchema = z.object({
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
  confidence: string | null;
  explanation: string | null;
  reason: string | null;
  error: string | null;
  rawOutput: Record<string, unknown> | null;
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
  llmPayload: Record<string, unknown>;
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

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function toJsonSchema() {
  return {
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
  };
}

function normalizeOptionalText(value: string | null | undefined) {
  const text = value?.trim() ?? "";
  return text || null;
}

function getFallbackCategory(transaction: Transaction, account: Account) {
  if (account.assetDomain === "investment") {
    return "uncategorized_investment";
  }
  return transaction.categoryCode ?? null;
}

function buildDeterministicClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
): DeterministicClassification {
  const matchedRule = applyRuleMatch(transaction, dataset.rules);
  if (matchedRule) {
    return {
      transactionClass:
        typeof matchedRule.outputsJson.transaction_class === "string"
          ? matchedRule.outputsJson.transaction_class
          : transaction.transactionClass,
      categoryCode:
        typeof matchedRule.outputsJson.category_code === "string"
          ? matchedRule.outputsJson.category_code
          : transaction.categoryCode ?? getFallbackCategory(transaction, account),
      merchantNormalized:
        typeof matchedRule.outputsJson.merchant_normalized === "string"
          ? matchedRule.outputsJson.merchant_normalized
          : transaction.merchantNormalized ?? null,
      counterpartyName:
        typeof matchedRule.outputsJson.counterparty_name === "string"
          ? matchedRule.outputsJson.counterparty_name
          : transaction.counterpartyName ?? null,
      economicEntityId:
        typeof matchedRule.outputsJson.economic_entity_id_override === "string"
          ? matchedRule.outputsJson.economic_entity_id_override
          : transaction.economicEntityId,
      classificationStatus: "rule",
      classificationSource: "user_rule",
      classificationConfidence: "1.00",
      explanation: "Matched an existing saved classification rule.",
      needsReview: false,
      reviewReason: null,
    };
  }

  const transferMatch = detectInternalTransfer(transaction, dataset.transactions, dataset.accounts);
  if (transferMatch) {
    return {
      transactionClass: "transfer_internal",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: transaction.merchantNormalized ?? null,
      counterpartyName: transferMatch.counterpartyName ?? transferMatch.merchantNormalized ?? null,
      economicEntityId: transaction.economicEntityId,
      classificationStatus: "transfer_match",
      classificationSource: "transfer_matcher",
      classificationConfidence: "1.00",
      explanation: "Matched an opposite-signed owned-account transfer candidate.",
      needsReview: false,
      reviewReason: null,
    };
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
            ? "Security mapping requires review."
            : null,
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
  };
}

export function getTransactionClassifierConfig() {
  return {
    apiKey: process.env.OPENAI_API_KEY ?? "",
    model: process.env.OPENAI_TRANSACTION_MODEL ?? "gpt-4.1-mini",
    lowConfidenceCutoff: Number(process.env.OPENAI_TRANSACTION_LOW_CONFIDENCE ?? "0.70"),
  };
}

export function isTransactionClassifierConfigured() {
  return Boolean(getTransactionClassifierConfig().apiKey);
}

function buildAllowedCategories(dataset: DomainDataset, account: Account) {
  const entityKind =
    dataset.entities.find((entity) => entity.id === account.entityId)?.entityKind ?? "personal";
  const scopeKinds =
    account.assetDomain === "investment"
      ? new Set(["investment", "system", "both"])
      : new Set([entityKind === "company" ? "company" : "personal", "system", "both"]);

  return dataset.categories.filter((category) => scopeKinds.has(category.scopeKind));
}

async function requestLlmClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
): Promise<LlmClassification> {
  const { apiKey, model } = getTransactionClassifierConfig();
  if (!apiKey) {
    return {
      analysisStatus: "skipped",
      model: null,
      transactionClass: null,
      categoryCode: null,
      merchantNormalized: null,
      counterpartyName: null,
      economicEntityId: null,
      securityHint: null,
      confidence: null,
      explanation: null,
      reason: null,
      error: "OPENAI_API_KEY is not configured.",
      rawOutput: null,
    };
  }

  const allowedCategories = buildAllowedCategories(dataset, account)
    .map((category) => `${category.code} (${category.displayName})`)
    .join(", ");
  const prompt = [
    account.assetDomain === "investment"
      ? buildInvestmentAccountPrompt({ kind: "account", accountId: account.id })
      : buildCashAccountPrompt({ kind: "account", accountId: account.id }),
    `Institution: ${account.institutionName}. Account: ${account.displayName}. Account type: ${account.accountType}.`,
    `Allowed transaction classes: ${allowedTransactionClasses.join(", ")}.`,
    `Allowed category codes: ${allowedCategories}.`,
    `Transaction date: ${transaction.transactionDate}. Posted date: ${transaction.postedDate ?? "null"}.`,
    `Amount: ${transaction.amountOriginal} ${transaction.currencyOriginal}.`,
    `Description: ${transaction.descriptionRaw}.`,
    `Existing merchant: ${transaction.merchantNormalized ?? "null"}. Existing counterparty: ${transaction.counterpartyName ?? "null"}.`,
    `Security id: ${transaction.securityId ?? "null"}. Quantity: ${transaction.quantity ?? "null"}. Unit price: ${transaction.unitPriceOriginal ?? "null"}.`,
    `Current raw payload: ${JSON.stringify(transaction.rawPayload)}.`,
    `Deterministic hint: ${JSON.stringify({
      transactionClass: deterministic.transactionClass,
      categoryCode: deterministic.categoryCode,
      explanation: deterministic.explanation,
      source: deterministic.classificationSource,
    })}.`,
    "Return a strict JSON object. Keep the explanation to one short sentence. Use null instead of guessing unsupported values.",
  ].join("\n");

  let lastError: Error | null = null;
  for (const delay of [0, 1000, 2000, 4000]) {
    if (delay > 0) {
      await sleep(delay);
    }

    try {
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
              content: [{ type: "input_text", text: prompt }],
            },
          ],
          text: {
            format: {
              type: "json_schema",
              name: "transaction_enrichment",
              schema: toJsonSchema(),
              strict: true,
            },
          },
        }),
      });

      if (!response.ok) {
        throw new Error(`OpenAI transaction classifier failed with status ${response.status}.`);
      }

      const payload = (await response.json()) as Record<string, unknown>;
      const parsed = transactionEnrichmentResponseSchema.parse(
        JSON.parse(extractResponseText(payload)),
      );

      return {
        analysisStatus: "done",
        model,
        transactionClass: parsed.transaction_class,
        categoryCode: normalizeOptionalText(parsed.category_code ?? null),
        merchantNormalized: normalizeOptionalText(parsed.merchant_normalized ?? null),
        counterpartyName: normalizeOptionalText(parsed.counterparty_name ?? null),
        economicEntityId: normalizeOptionalText(parsed.economic_entity_override ?? null),
        securityHint: normalizeOptionalText(parsed.security_hint ?? null),
        confidence: parsed.confidence.toFixed(2),
        explanation: parsed.explanation,
        reason: parsed.reason,
        error: null,
        rawOutput: parsed,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error("Unknown LLM classification failure.");
    }
  }

  return {
    analysisStatus: "failed",
    model,
    transactionClass: null,
    categoryCode: null,
    merchantNormalized: null,
    counterpartyName: null,
    economicEntityId: null,
    securityHint: null,
    confidence: null,
    explanation: null,
    reason: null,
    error: lastError?.message ?? "Unknown LLM classification failure.",
    rawOutput: null,
  };
}

export async function enrichImportedTransaction(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
): Promise<TransactionEnrichmentDecision> {
  const deterministic = buildDeterministicClassification(dataset, account, transaction);
  const llm = await requestLlmClassification(dataset, account, transaction, deterministic);
  const { lowConfidenceCutoff } = getTransactionClassifierConfig();
  const allowedCategoryCodes = new Set(dataset.categories.map((category) => category.code));
  const allowedClassSet = new Set<string>(allowedTransactionClasses);

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

  if (llm.analysisStatus === "done") {
    const llmTransactionClass =
      llm.transactionClass && allowedClassSet.has(llm.transactionClass)
        ? llm.transactionClass
        : deterministic.transactionClass;
    const llmCategoryCode =
      llm.categoryCode && allowedCategoryCodes.has(llm.categoryCode)
        ? llm.categoryCode
        : deterministic.categoryCode;

    const deterministicWins = new Set(["user_rule", "transfer_matcher", "investment_parser"]).has(
      deterministic.classificationSource,
    );
    if (!deterministicWins) {
      transactionClass = llmTransactionClass;
      categoryCode = llmCategoryCode;
      economicEntityId = llm.economicEntityId ?? deterministic.economicEntityId;
      classificationStatus = "llm";
      classificationSource = "llm";
      classificationConfidence = llm.confidence ?? deterministic.classificationConfidence;
    }

    merchantNormalized = llm.merchantNormalized ?? deterministic.merchantNormalized;
    counterpartyName = llm.counterpartyName ?? deterministic.counterpartyName;

    if ((Number(llm.confidence ?? "0") || 0) < lowConfidenceCutoff) {
      needsReview = true;
      reviewReason = `Low-confidence ${transactionClass} classification.`;
    } else if (transactionClass !== "unknown" && !deterministic.needsReview) {
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
    transaction.quantity &&
    !reviewReason
  ) {
    needsReview = true;
    reviewReason = "Security mapping requires review.";
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
    llmPayload: {
      analysisStatus: llm.analysisStatus,
      model: llm.model,
      explanation: llm.explanation ?? deterministic.explanation,
      reason: llm.reason ?? deterministic.explanation,
      confidence: llm.confidence ?? deterministic.classificationConfidence,
      deterministic,
      llm,
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
      },
      analyzedAt: new Date().toISOString(),
    },
  };
}

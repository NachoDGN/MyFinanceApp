import { z } from "zod";

import type { Account, ClassificationRule, Scope, Transaction } from "@myfinance/domain";

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

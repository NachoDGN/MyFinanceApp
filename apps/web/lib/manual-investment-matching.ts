import type { Transaction } from "@myfinance/domain";

export function normalizeMatcherText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

export function parseManualInvestmentMatcherTerms(matcherText: string) {
  return [
    ...new Set(
      matcherText
        .split(/[\n,]+/)
        .map((term) => normalizeMatcherText(term.trim()))
        .filter(Boolean),
    ),
  ];
}

function serializeMatchPayload(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return "";
  }
}

export function buildManualInvestmentMatchHaystack(transaction: Transaction) {
  const rawPayload =
    transaction.rawPayload && typeof transaction.rawPayload === "object"
      ? transaction.rawPayload
      : {};

  return normalizeMatcherText(
    [
      transaction.descriptionRaw,
      transaction.descriptionClean,
      transaction.merchantNormalized,
      transaction.counterpartyName,
      serializeMatchPayload(
        (rawPayload as Record<string, unknown>).providerContext,
      ),
      serializeMatchPayload(
        (rawPayload as Record<string, unknown>).providerRaw,
      ),
    ]
      .filter(Boolean)
      .join(" "),
  );
}

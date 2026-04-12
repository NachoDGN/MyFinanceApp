import type {
  Account,
  ClassificationRule,
  DomainDataset,
  Transaction,
} from "@myfinance/domain";
import {
  buildAllowedTransactionClassesForAccount,
  getAllowedCategoryCodesForAccount,
  isUncategorizedCategoryCode,
  normalizeDescription,
  resolveConstrainedEconomicEntityId,
} from "@myfinance/domain";

import {
  normalizeTradeQuantity,
  parseInvestmentEvent,
  type DeterministicClassification,
} from "./investment-support";
import {
  normalizeOptionalText,
  readOptionalRecord,
  readOptionalString,
} from "./utils";

export function applyRuleMatch(
  transaction: Transaction,
  rules: ClassificationRule[],
): ClassificationRule | null {
  const ordered = [...rules]
    .filter((rule) => rule.active)
    .sort((a, b) => a.priority - b.priority);

  const comparison = normalizeDescription(
    transaction.descriptionRaw,
  ).comparison;

  for (const rule of ordered) {
    const scopeAccountId =
      typeof rule.scopeJson.account_id === "string"
        ? rule.scopeJson.account_id
        : null;
    const scopeEntityId =
      typeof rule.scopeJson.entity_id === "string"
        ? rule.scopeJson.entity_id
        : null;
    if (scopeAccountId && scopeAccountId !== transaction.accountId) {
      continue;
    }
    if (
      scopeEntityId &&
      scopeEntityId !== transaction.economicEntityId &&
      scopeEntityId !== transaction.accountEntityId
    ) {
      continue;
    }

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

  return (
    candidateRows.find((candidate) => {
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
          Math.abs(Number(transaction.amountBaseEur)) -
            Math.abs(Number(candidate.amountBaseEur)),
        ) < 0.01;
      const sameCurrency =
        candidate.currencyOriginal === transaction.currencyOriginal;
      const aliasHint = ownedAccounts.some(
        (account) =>
          account.id === candidate.accountId &&
          account.matchingAliases.some((alias) =>
            normalizeDescription(
              transaction.descriptionRaw,
            ).comparison.includes(alias.toUpperCase()),
          ),
      );
      return (
        dateDelta && oppositeSign && sameMagnitude && sameCurrency && aliasHint
      );
    }) ?? null
  );
}

export function resolveValidEconomicEntityOverride(
  dataset: DomainDataset,
  value: string | null | undefined,
) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) {
    return null;
  }

  return dataset.entities.some((entity) => entity.id === normalized)
    ? normalized
    : null;
}

function getFallbackCategory(transaction: Transaction, account: Account) {
  if (account.assetDomain === "investment") {
    return "uncategorized_investment";
  }
  return transaction.categoryCode ?? null;
}

type DeterministicClassificationDraft = Pick<
  DeterministicClassification,
  | "transactionClass"
  | "categoryCode"
  | "classificationStatus"
  | "classificationSource"
  | "classificationConfidence"
  | "explanation"
  | "needsReview"
  | "reviewReason"
> &
  Partial<
    Pick<
      DeterministicClassification,
      | "merchantNormalized"
      | "counterpartyName"
      | "economicEntityId"
      | "securityHint"
      | "quantity"
      | "unitPriceOriginal"
    >
  >;

function buildDeterministicResult(
  transaction: Transaction,
  draft: DeterministicClassificationDraft,
): DeterministicClassification {
  return {
    transactionClass: draft.transactionClass,
    categoryCode: draft.categoryCode,
    merchantNormalized:
      draft.merchantNormalized === undefined
        ? (transaction.merchantNormalized ?? null)
        : draft.merchantNormalized,
    counterpartyName:
      draft.counterpartyName === undefined
        ? (transaction.counterpartyName ?? null)
        : draft.counterpartyName,
    economicEntityId:
      draft.economicEntityId === undefined
        ? transaction.economicEntityId
        : draft.economicEntityId,
    classificationStatus: draft.classificationStatus,
    classificationSource: draft.classificationSource,
    classificationConfidence: draft.classificationConfidence,
    explanation: draft.explanation,
    needsReview: draft.needsReview,
    reviewReason: draft.reviewReason,
    securityHint: draft.securityHint === undefined ? null : draft.securityHint,
    quantity:
      draft.quantity === undefined
        ? (transaction.quantity ?? null)
        : draft.quantity,
    unitPriceOriginal:
      draft.unitPriceOriginal === undefined
        ? (transaction.unitPriceOriginal ?? null)
        : draft.unitPriceOriginal,
  };
}

export function extractProviderContext(transaction: Transaction) {
  const rawPayload = readOptionalRecord(transaction.rawPayload);
  return (
    readOptionalRecord(rawPayload?.providerContext) ??
    readOptionalRecord(rawPayload?.provider_context) ??
    readOptionalRecord(rawPayload?.ProviderContext) ??
    null
  );
}

function buildRevolutDeterministicClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
): DeterministicClassification | null {
  if (transaction.providerName !== "revolut_business") {
    return null;
  }

  const providerContext = extractProviderContext(transaction);
  if (!providerContext) {
    return null;
  }

  const revolutTransaction = readOptionalRecord(providerContext.transaction);
  const merchant = readOptionalRecord(providerContext.merchant);
  const revolutType = readOptionalString(revolutTransaction?.type);
  if (!revolutType) {
    return null;
  }

  const merchantName = readOptionalString(merchant?.name);
  const merchantCategoryCode = readOptionalString(merchant?.categoryCode);
  const allowedCategoryCodes = getAllowedCategoryCodesForAccount(
    dataset,
    account,
  );
  const refundTypes = new Set([
    "refund",
    "card_refund",
    "charge_refund",
    "tax_refund",
  ]);
  const travelMerchantCategoryCodes = new Set(["3001", "4722"]);
  const canApplyTravelCategory =
    allowedCategoryCodes.has("travel") &&
    travelMerchantCategoryCodes.has(merchantCategoryCode ?? "") &&
    (transaction.categoryCode === null ||
      isUncategorizedCategoryCode(transaction.categoryCode));

  if (revolutType === "exchange") {
    return buildDeterministicResult(transaction, {
      transactionClass: "fx_conversion",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: merchantName,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.98",
      explanation:
        "Revolut marks this transaction as an exchange, so it is treated as an FX conversion.",
      needsReview: false,
      reviewReason: null,
      quantity: null,
      unitPriceOriginal: null,
    });
  }

  if (revolutType === "fee") {
    return buildDeterministicResult(transaction, {
      transactionClass: "fee",
      categoryCode: transaction.categoryCode ?? null,
      merchantNormalized: merchantName,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.96",
      explanation:
        "Revolut marks this transaction as a fee, so it is treated as bank fees.",
      needsReview: false,
      reviewReason: null,
      quantity: null,
      unitPriceOriginal: null,
    });
  }

  if (canApplyTravelCategory && revolutType === "card_payment") {
    return buildDeterministicResult(transaction, {
      transactionClass: "expense",
      categoryCode: "travel",
      merchantNormalized: merchantName,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.97",
      explanation:
        "Revolut supplied a travel-related merchant category code, so this card payment is categorized as travel.",
      needsReview: false,
      reviewReason: null,
      quantity: null,
      unitPriceOriginal: null,
    });
  }

  if (refundTypes.has(revolutType)) {
    return buildDeterministicResult(transaction, {
      transactionClass: "refund",
      categoryCode: canApplyTravelCategory ? "travel" : (transaction.categoryCode ?? null),
      merchantNormalized: merchantName,
      classificationStatus: "rule",
      classificationSource: "system_fallback",
      classificationConfidence: "0.96",
      explanation:
        canApplyTravelCategory
          ? "Revolut marks this transaction as a refund and the merchant category code indicates travel, so the refund stays in travel."
          : "Revolut marks this transaction as a refund, so it is treated as money returning from a prior charge.",
      needsReview: false,
      reviewReason: null,
      quantity: null,
      unitPriceOriginal: null,
    });
  }

  return null;
}

export function buildDeterministicClassification(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
): DeterministicClassification {
  const matchedRule = applyRuleMatch(transaction, dataset.rules);
  if (matchedRule) {
    const allowedTransactionClasses = new Set(
      buildAllowedTransactionClassesForAccount(account),
    );
    const allowedCategoryCodes = getAllowedCategoryCodesForAccount(
      dataset,
      account,
    );
    const requestedTransactionClass =
      typeof matchedRule.outputsJson.transaction_class === "string"
        ? matchedRule.outputsJson.transaction_class
        : null;
    const requestedCategoryCode =
      typeof matchedRule.outputsJson.category_code === "string"
        ? matchedRule.outputsJson.category_code
        : null;
    const requestedEconomicEntityId =
      typeof matchedRule.outputsJson.economic_entity_id_override === "string"
        ? matchedRule.outputsJson.economic_entity_id_override
        : null;
    const rejectedRuleOutputs: string[] = [];
    if (
      requestedTransactionClass &&
      !allowedTransactionClasses.has(
        requestedTransactionClass as Transaction["transactionClass"],
      )
    ) {
      rejectedRuleOutputs.push("transaction class");
    }
    if (
      requestedCategoryCode &&
      !allowedCategoryCodes.has(requestedCategoryCode)
    ) {
      rejectedRuleOutputs.push("category");
    }
    if (
      requestedEconomicEntityId &&
      resolveConstrainedEconomicEntityId(
        dataset,
        account,
        requestedEconomicEntityId,
        transaction.economicEntityId,
      ) !== requestedEconomicEntityId
    ) {
      rejectedRuleOutputs.push("economic entity");
    }

    const transactionClass =
      requestedTransactionClass &&
      allowedTransactionClasses.has(
        requestedTransactionClass as Transaction["transactionClass"],
      )
        ? requestedTransactionClass
        : transaction.transactionClass;
    const categoryCode =
      requestedCategoryCode && allowedCategoryCodes.has(requestedCategoryCode)
        ? requestedCategoryCode
        : (transaction.categoryCode ??
          getFallbackCategory(transaction, account));
    const economicEntityId = resolveConstrainedEconomicEntityId(
      dataset,
      account,
      requestedEconomicEntityId,
      transaction.economicEntityId,
    );

    return buildDeterministicResult(transaction, {
      transactionClass,
      categoryCode,
      merchantNormalized:
        typeof matchedRule.outputsJson.merchant_normalized === "string"
          ? matchedRule.outputsJson.merchant_normalized
          : (transaction.merchantNormalized ?? null),
      counterpartyName:
        typeof matchedRule.outputsJson.counterparty_name === "string"
          ? matchedRule.outputsJson.counterparty_name
          : (transaction.counterpartyName ?? null),
      economicEntityId,
      classificationStatus: "rule",
      classificationSource: "user_rule",
      classificationConfidence:
        rejectedRuleOutputs.length === 0 ? "1.00" : "0.00",
      explanation:
        rejectedRuleOutputs.length === 0
          ? "Matched an existing saved classification rule."
          : "A saved rule matched, but some requested outputs were incompatible with this account.",
      needsReview: rejectedRuleOutputs.length > 0,
      reviewReason:
        rejectedRuleOutputs.length > 0
          ? `Saved rule requested an incompatible ${rejectedRuleOutputs.join(", ")} for this account.`
          : null,
    });
  }

  const transferMatch = detectInternalTransfer(
    transaction,
    dataset.transactions,
    dataset.accounts,
  );
  if (transferMatch) {
    return buildDeterministicResult(transaction, {
      transactionClass: "transfer_internal",
      categoryCode: transaction.categoryCode ?? null,
      counterpartyName:
        transferMatch.counterpartyName ??
        transferMatch.merchantNormalized ??
        null,
      classificationStatus: "transfer_match",
      classificationSource: "transfer_matcher",
      classificationConfidence: "1.00",
      explanation:
        "Matched an opposite-signed owned-account transfer candidate.",
      needsReview: false,
      reviewReason: null,
    });
  }

  const revolutDeterministic =
    buildRevolutDeterministicClassification(dataset, account, transaction);
  if (revolutDeterministic) {
    return revolutDeterministic;
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
              : parsed.transactionClass === "transfer_internal"
                ? "uncategorized_investment"
                : parsed.transactionClass === "investment_trade_buy"
                  ? "stock_buy"
                  : "uncategorized_investment";

      return buildDeterministicResult(transaction, {
        transactionClass: parsed.transactionClass,
        categoryCode,
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        explanation: "Matched the deterministic investment statement parser.",
        needsReview: !transaction.securityId && Boolean(parsed.securityHint),
        reviewReason:
          !transaction.securityId && parsed.securityHint
            ? `Parsed investment trade for "${parsed.securityHint}", but the system has not matched it to a tracked security yet.`
            : null,
        securityHint: parsed.securityHint ?? null,
        quantity: normalizeTradeQuantity(
          parsed.transactionClass,
          transaction.quantity ?? parsed.quantity ?? null,
        ),
        unitPriceOriginal:
          transaction.unitPriceOriginal ?? parsed.unitPriceOriginal ?? null,
      });
    }
  }

  return buildDeterministicResult(transaction, {
    transactionClass: "unknown",
    categoryCode: getFallbackCategory(transaction, account),
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    explanation: "No deterministic classifier matched the imported row.",
    needsReview: true,
    reviewReason: "Needs LLM enrichment.",
  });
}

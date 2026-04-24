import { createHash, randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import type {
  DomainDataset,
  ImportExecutionInput,
  Transaction,
} from "@myfinance/domain";
import {
  extractIsinFromText,
  isCreditCardSettlementText,
  normalizeSecurityIdentifier,
  normalizeSecurityText,
} from "@myfinance/domain";

import type { CanonicalImportRow } from "./types";

function normalizeDescriptionForImport(value: string) {
  return normalizeSecurityText(value);
}

function normalizeFingerprintText(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function truncateDecimalTowardsZero(value: Decimal) {
  return value.isNegative() ? value.ceil() : value.floor();
}

function matchesRoundedWholeValue(left: string, right: string) {
  const leftValue = new Decimal(left);
  const rightValue = new Decimal(right);
  if (leftValue.eq(rightValue)) {
    return false;
  }
  if (leftValue.minus(rightValue).abs().gte(1)) {
    return false;
  }

  return (
    (leftValue.isInteger() &&
      truncateDecimalTowardsZero(rightValue).eq(leftValue)) ||
    (rightValue.isInteger() &&
      truncateDecimalTowardsZero(leftValue).eq(rightValue))
  );
}

function resolveSecurityId(
  dataset: DomainDataset,
  row: Pick<
    CanonicalImportRow,
    "external_reference" | "security_isin" | "security_symbol" | "security_name"
  >,
) {
  const securityIsin =
    normalizeSecurityIdentifier(row.security_isin) ||
    extractIsinFromText(row.external_reference);
  const symbol = normalizeFingerprintText(row.security_symbol);
  const securityName = normalizeFingerprintText(row.security_name);

  if (!securityIsin && !symbol && !securityName) {
    return null;
  }

  if (securityIsin) {
    const exactSecurity = dataset.securities.find(
      (security) => normalizeSecurityIdentifier(security.isin) === securityIsin,
    );
    if (exactSecurity) {
      return exactSecurity.id;
    }
  }

  const directMatch = dataset.securities.find((security) => {
    const candidates = [
      security.providerSymbol,
      security.canonicalSymbol,
      security.displaySymbol,
      security.name,
    ].map((value) => normalizeFingerprintText(value));

    return (
      (securityIsin &&
        normalizeFingerprintText(security.isin) ===
          normalizeFingerprintText(securityIsin)) ||
      (symbol && candidates.includes(symbol)) ||
      (securityName && candidates.includes(securityName))
    );
  });
  if (directMatch) {
    return directMatch.id;
  }

  const aliasMatch = dataset.securityAliases.find((alias) => {
    const aliasText = normalizeFingerprintText(alias.aliasTextNormalized);
    return (
      (securityIsin && aliasText === normalizeFingerprintText(securityIsin)) ||
      (symbol && aliasText === symbol) ||
      (securityName && aliasText === securityName)
    );
  });

  return aliasMatch?.securityId ?? null;
}

function safeParseRawRowJson(row: CanonicalImportRow) {
  if (!row.raw_row_json) return {};
  try {
    const parsed = JSON.parse(row.raw_row_json) as Record<string, unknown>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

type InvestmentDuplicateCandidate = {
  amountOriginal: string;
  unitPriceOriginal: string | null;
};

function buildInvestmentDuplicateSignature(
  accountId: string,
  row: {
    transactionDate: string;
    postedDate: string;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    quantity: string | null;
    securityId: string | null;
    transactionTypeRaw: string | null;
  },
) {
  if (!row.securityId) {
    return null;
  }

  return [
    "investment",
    accountId,
    row.transactionDate,
    row.postedDate,
    row.currencyOriginal,
    row.securityId,
    normalizeFingerprintText(row.descriptionRaw),
    normalizeFingerprintText(row.quantity),
    normalizeFingerprintText(row.transactionTypeRaw),
    new Decimal(row.amountOriginal).isNegative() ? "outflow" : "inflow",
  ].join("|");
}

function isRoundedInvestmentDuplicate(
  candidate: InvestmentDuplicateCandidate,
  existing: InvestmentDuplicateCandidate,
) {
  if (
    candidate.unitPriceOriginal &&
    existing.unitPriceOriginal &&
    new Decimal(candidate.unitPriceOriginal)
      .minus(existing.unitPriceOriginal)
      .abs()
      .gte(1)
  ) {
    return false;
  }

  return (
    matchesRoundedWholeValue(
      candidate.amountOriginal,
      existing.amountOriginal,
    ) ||
    (candidate.unitPriceOriginal !== null &&
      existing.unitPriceOriginal !== null &&
      matchesRoundedWholeValue(
        candidate.unitPriceOriginal,
        existing.unitPriceOriginal,
      ))
  );
}

function buildImportFingerprint(
  accountId: string,
  row: {
    transactionDate: string;
    postedDate: string;
    amountOriginal: string;
    currencyOriginal: string;
    descriptionRaw: string;
    externalReference: string;
    securityIsin: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    securitySymbol: string | null;
    securityName: string | null;
    transactionTypeRaw: string | null;
  },
) {
  const fingerprintReference = row.externalReference || row.securityIsin || "";
  return createHash("sha256")
    .update(
      [
        accountId,
        row.transactionDate,
        row.postedDate,
        row.amountOriginal,
        row.currencyOriginal,
        normalizeFingerprintText(row.descriptionRaw),
        normalizeFingerprintText(fingerprintReference),
        normalizeFingerprintText(row.quantity),
        normalizeFingerprintText(row.unitPriceOriginal),
        normalizeFingerprintText(row.securitySymbol),
        normalizeFingerprintText(row.securityName),
        normalizeFingerprintText(row.transactionTypeRaw),
      ].join("|"),
    )
    .digest("hex");
}

function resolveFxRateToEur(
  dataset: DomainDataset,
  currencyOriginal: string,
  transactionDate: string,
) {
  if (currencyOriginal === "EUR") {
    return "1.00000000";
  }

  const directRate = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === currencyOriginal &&
        row.quoteCurrency === "EUR" &&
        row.asOfDate <= transactionDate,
    )
    .sort((left, right) => right.asOfDate.localeCompare(left.asOfDate))[0];

  return directRate?.rate ?? null;
}

export function buildImportedTransactions(
  dataset: DomainDataset,
  input: Required<ImportExecutionInput>,
  importBatchId: string,
  rows: CanonicalImportRow[],
) {
  const account = dataset.accounts.find((row) => row.id === input.accountId);
  if (!account) {
    throw new Error(`Account ${input.accountId} not found.`);
  }

  const accountsById = new Map(
    dataset.accounts.map((datasetAccount) => [
      datasetAccount.id,
      datasetAccount,
    ]),
  );
  const existingFingerprints = new Set(
    dataset.transactions.map((transaction) => transaction.sourceFingerprint),
  );
  const existingInvestmentDuplicateCandidates = new Map<
    string,
    InvestmentDuplicateCandidate[]
  >();
  for (const transaction of dataset.transactions) {
    const transactionAccount = accountsById.get(transaction.accountId);
    if (
      transactionAccount?.assetDomain !== "investment" ||
      transaction.voidedAt ||
      transaction.excludeFromAnalytics
    ) {
      continue;
    }

    const transactionTypeRaw =
      typeof transaction.rawPayload?._import === "object" &&
      transaction.rawPayload._import &&
      "transaction_type_raw" in transaction.rawPayload._import &&
      typeof transaction.rawPayload._import.transaction_type_raw === "string"
        ? transaction.rawPayload._import.transaction_type_raw
        : null;
    const duplicateSignature = buildInvestmentDuplicateSignature(
      transaction.accountId,
      {
        transactionDate: transaction.transactionDate,
        postedDate: transaction.postedDate ?? transaction.transactionDate,
        amountOriginal: transaction.amountOriginal,
        currencyOriginal: transaction.currencyOriginal,
        descriptionRaw: transaction.descriptionRaw,
        quantity: transaction.quantity ?? null,
        securityId: transaction.securityId ?? null,
        transactionTypeRaw,
      },
    );
    if (!duplicateSignature) {
      continue;
    }

    const candidates =
      existingInvestmentDuplicateCandidates.get(duplicateSignature) ?? [];
    candidates.push({
      amountOriginal: transaction.amountOriginal,
      unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    });
    existingInvestmentDuplicateCandidates.set(duplicateSignature, candidates);
  }
  const inserted: Transaction[] = [];
  let duplicateCount = 0;
  const createdAt = new Date().toISOString();

  for (const row of rows) {
    const transactionDate = String(row.transaction_date ?? "").slice(0, 10);
    const descriptionRaw = String(row.description_raw ?? "").trim();
    if (!transactionDate || !descriptionRaw) {
      continue;
    }

    const postedDate =
      String(row.posted_date ?? "").slice(0, 10) || transactionDate;
    const amountOriginal = new Decimal(
      String(row.amount_original_signed ?? "0"),
    ).toFixed(8);
    const currencyOriginal = String(
      row.currency_original ?? account.defaultCurrency ?? "EUR",
    ).toUpperCase();
    const externalReference = String(row.external_reference ?? "");
    const quantity = row.quantity
      ? new Decimal(String(row.quantity)).toFixed(8)
      : null;
    const securityIsin =
      normalizeSecurityIdentifier(row.security_isin) ||
      extractIsinFromText(externalReference) ||
      null;
    const unitPriceOriginal = row.unit_price_original
      ? new Decimal(String(row.unit_price_original)).toFixed(8)
      : quantity &&
          !new Decimal(quantity).eq(0) &&
          !new Decimal(amountOriginal).eq(0)
        ? new Decimal(amountOriginal)
            .abs()
            .div(new Decimal(quantity).abs())
            .toFixed(8)
        : null;
    const securitySymbol = String(row.security_symbol ?? "").trim() || null;
    const securityName = String(row.security_name ?? "").trim() || null;
    const transactionTypeRaw =
      String(row.transaction_type_raw ?? "").trim() || null;
    const sourceFingerprint = buildImportFingerprint(input.accountId, {
      transactionDate,
      postedDate,
      amountOriginal,
      currencyOriginal,
      descriptionRaw,
      externalReference,
      securityIsin,
      quantity,
      unitPriceOriginal,
      securitySymbol,
      securityName,
      transactionTypeRaw,
    });

    if (existingFingerprints.has(sourceFingerprint)) {
      duplicateCount += 1;
      continue;
    }

    const importedFxRate = row.fx_rate
      ? new Decimal(String(row.fx_rate)).toFixed(8)
      : null;
    const fxRateToEur =
      importedFxRate ??
      resolveFxRateToEur(dataset, currencyOriginal, transactionDate);
    const amountBaseEur = new Decimal(amountOriginal)
      .times(new Decimal(fxRateToEur ?? "1"))
      .toFixed(8);
    const rawPayload = {
      ...safeParseRawRowJson(row),
      _import: {
        posted_date: postedDate,
        balance_original: row.balance_original ?? null,
        external_reference: externalReference || null,
        transaction_type_raw: transactionTypeRaw,
        security_isin: securityIsin,
        security_symbol: securitySymbol,
        security_name: securityName,
        quantity,
        unit_price_original: unitPriceOriginal,
        fees_original: row.fees_original ?? null,
        fx_rate: importedFxRate,
      },
    } satisfies Record<string, unknown>;
    const securityId = resolveSecurityId(dataset, row);
    if (account.assetDomain === "investment") {
      const duplicateSignature = buildInvestmentDuplicateSignature(account.id, {
        transactionDate,
        postedDate,
        amountOriginal,
        currencyOriginal,
        descriptionRaw,
        quantity,
        securityId,
        transactionTypeRaw,
      });
      if (duplicateSignature) {
        const candidate = {
          amountOriginal,
          unitPriceOriginal,
        } satisfies InvestmentDuplicateCandidate;
        const existingCandidates =
          existingInvestmentDuplicateCandidates.get(duplicateSignature) ?? [];
        if (
          existingCandidates.some((existing) =>
            isRoundedInvestmentDuplicate(candidate, existing),
          )
        ) {
          duplicateCount += 1;
          continue;
        }
        existingCandidates.push(candidate);
        existingInvestmentDuplicateCandidates.set(
          duplicateSignature,
          existingCandidates,
        );
      }
    }
    const initialReviewReasons = [
      "Queued for automatic transaction analysis.",
      isCreditCardSettlementText(descriptionRaw)
        ? "Upload the matching credit-card statement to resolve category KPIs."
        : null,
      currencyOriginal !== "EUR" && !fxRateToEur
        ? "Missing FX rate for base-currency conversion."
        : null,
      account.assetDomain === "investment" &&
      !securityId &&
      (securitySymbol || securityName)
        ? "Security mapping unresolved."
        : null,
    ].filter(Boolean);

    inserted.push({
      id: randomUUID(),
      userId: dataset.profile.id,
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      importBatchId,
      providerName: null,
      providerRecordId: null,
      sourceFingerprint,
      duplicateKey: sourceFingerprint,
      transactionDate,
      postedDate,
      amountOriginal,
      currencyOriginal,
      amountBaseEur,
      fxRateToEur,
      descriptionRaw,
      descriptionClean: normalizeDescriptionForImport(descriptionRaw),
      merchantNormalized: null,
      counterpartyName: null,
      transactionClass: "unknown",
      categoryCode:
        account.assetDomain === "investment"
          ? "uncategorized_investment"
          : null,
      subcategoryCode: null,
      transferGroupId: null,
      relatedAccountId: null,
      relatedTransactionId: null,
      transferMatchStatus: "not_transfer",
      crossEntityFlag: false,
      reimbursementStatus: "none",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: initialReviewReasons.join(" "),
      excludeFromAnalytics: false,
      correctionOfTransactionId: null,
      voidedAt: null,
      manualNotes: null,
      llmPayload: {
        analysisStatus: "pending",
        explanation: null,
        model: null,
        error: null,
        queuedAt: createdAt,
      },
      rawPayload,
      securityId,
      quantity,
      unitPriceOriginal,
      creditCardStatementStatus: isCreditCardSettlementText(descriptionRaw)
        ? "upload_required"
        : "not_applicable",
      linkedCreditCardAccountId: null,
      createdAt,
      updatedAt: createdAt,
    });
    existingFingerprints.add(sourceFingerprint);
  }

  return {
    inserted,
    duplicateCount,
  };
}

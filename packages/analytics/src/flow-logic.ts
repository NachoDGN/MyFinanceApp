import { Decimal } from "decimal.js";

import type { DomainDataset, Transaction } from "@myfinance/domain";
import {
  isCreditCardSettlementTransaction,
  isTransactionResolvedForAnalytics,
  resolveFxRate,
  todayIso,
} from "@myfinance/domain";

export function sumStrings(values: Array<string | null | undefined>) {
  return values
    .reduce((sum, value) => sum.plus(new Decimal(value ?? 0)), new Decimal(0))
    .toFixed(2);
}

export function safeDividePercent(numerator: Decimal, denominator: Decimal) {
  if (denominator.eq(0)) return null;
  return numerator.div(denominator).mul(100).toFixed(2);
}

export function toDisplayAmount(
  dataset: DomainDataset,
  amountEur: string | null,
  currency: string,
  asOfDate = todayIso(),
) {
  if (amountEur === null) return null;
  if (currency === "EUR") return new Decimal(amountEur).toFixed(2);
  return new Decimal(amountEur)
    .mul(resolveFxRate(dataset, "EUR", currency, asOfDate))
    .toFixed(2);
}

export function amountMagnitudeEur(transaction: Transaction) {
  return new Decimal(transaction.amountBaseEur).abs();
}

export function humanizeTransactionClass(
  transactionClass: Transaction["transactionClass"],
) {
  return transactionClass
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function isIncomeLike(transaction: Transaction) {
  return ["income", "dividend", "interest"].includes(
    transaction.transactionClass,
  );
}

export function isExcludedIncome(transaction: Transaction) {
  return [
    "owner_contribution",
    "reimbursement",
    "refund",
    "transfer_internal",
    "transfer_external",
    "loan_inflow",
    "investment_trade_sell",
  ].includes(transaction.transactionClass);
}

export function isExcludedFromFlowAnalytics(transaction: Transaction) {
  return (
    transaction.excludeFromAnalytics === true ||
    Boolean(transaction.voidedAt) ||
    isCreditCardSettlementTransaction(transaction)
  );
}

export function isUnresolvedCashFlow(transaction: Transaction) {
  return (
    transaction.transactionClass === "unknown" &&
    !isExcludedFromFlowAnalytics(transaction)
  );
}

export function isSpendingLike(transaction: Transaction) {
  return [
    "expense",
    "fee",
    "refund",
    "loan_principal_payment",
    "loan_interest_payment",
  ].includes(transaction.transactionClass);
}

export function incomeContributionEur(transaction: Transaction) {
  if (isExcludedFromFlowAnalytics(transaction)) {
    return null;
  }

  if (isUnresolvedCashFlow(transaction)) {
    const amount = new Decimal(transaction.amountBaseEur);
    return amount.gt(0) ? amount : null;
  }

  if (!isTransactionResolvedForAnalytics(transaction)) {
    return null;
  }

  if (!isIncomeLike(transaction) || isExcludedIncome(transaction)) {
    return null;
  }

  return new Decimal(transaction.amountBaseEur);
}

export function spendingContributionEur(transaction: Transaction) {
  if (isExcludedFromFlowAnalytics(transaction)) {
    return null;
  }

  if (isUnresolvedCashFlow(transaction)) {
    const amount = new Decimal(transaction.amountBaseEur);
    return amount.lt(0) ? amount.abs() : null;
  }

  if (!isTransactionResolvedForAnalytics(transaction)) {
    return null;
  }

  if (!isSpendingLike(transaction)) {
    return null;
  }

  if (transaction.transactionClass === "refund") {
    return new Decimal(transaction.amountBaseEur).neg();
  }

  return amountMagnitudeEur(transaction);
}

export function hasIncomeContribution(transaction: Transaction) {
  return incomeContributionEur(transaction) !== null;
}

export function hasSpendingContribution(transaction: Transaction) {
  return spendingContributionEur(transaction) !== null;
}

export function hasFlowContribution(transaction: Transaction) {
  return (
    hasIncomeContribution(transaction) || hasSpendingContribution(transaction)
  );
}

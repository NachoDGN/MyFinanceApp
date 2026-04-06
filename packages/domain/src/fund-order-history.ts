import { differenceInCalendarDays, parse } from "date-fns";
import Decimal from "decimal.js";

import type {
  DomainDataset,
  HoldingAdjustment,
  Security,
  Transaction,
} from "./types";

const FINALIZED_STATUS = "Finalizada";
const REJECTED_STATUS = "Rechazada";
const DEFAULT_MATCH_DAY_WINDOW = 10;
const DEFAULT_MATCH_AMOUNT_TOLERANCE_EUR = new Decimal(5);

export interface FundOrderHistoryRow {
  orderDate: string;
  orderKind: string;
  amountEur: string;
  fundName: string;
  cadence: string;
  status: string;
  quantity: string | null;
}

export interface FundOrderHistoryIssue {
  row: FundOrderHistoryRow;
  reason: string;
}

export interface FundOrderHistoryTransactionPatch {
  transactionId: string;
  securityId: string;
  fundName: string;
  orderDate: string;
  transactionDate: string;
  postedDate: string;
  quantity: string;
  unitPriceOriginal: string | null;
  actualAmountEur: string;
  orderAmountEur: string;
  amountDiffEur: string;
  dayDistance: number;
}

export interface FundOrderHistoryOpeningPosition {
  securityId: string;
  fundName: string;
  orderDate: string;
  orderKind: string;
  quantity: string;
  costBasisEur: string;
}

export interface FundOrderHistoryImportPlan {
  parsedRows: FundOrderHistoryRow[];
  rejectedRows: FundOrderHistoryRow[];
  finalizedRows: FundOrderHistoryRow[];
  unresolvedRows: FundOrderHistoryIssue[];
  matchedTransactionPatches: FundOrderHistoryTransactionPatch[];
  openingPositions: FundOrderHistoryOpeningPosition[];
}

export interface FundOrderHistoryExistingOpeningAdjustment {
  adjustmentId: string;
  securityId: string;
  fundName: string;
  orderDate: string;
  quantity: string;
  costBasisEur: string;
}

export interface FundOrderHistoryReconciliationPlan {
  staleOpeningAdjustments: FundOrderHistoryExistingOpeningAdjustment[];
  existingOpeningPositions: FundOrderHistoryExistingOpeningAdjustment[];
  openingPositionsToCreate: FundOrderHistoryOpeningPosition[];
}

export function parseMyInvestorFundOrderHistoryText(
  text: string,
): FundOrderHistoryRow[] {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const rows: FundOrderHistoryRow[] = [];
  let activeDate: string | null = null;

  for (let index = 0; index < lines.length; ) {
    const line = lines[index] ?? "";
    if (isFundOrderDateHeader(line)) {
      activeDate = normalizeFundOrderDate(line);
      index += 1;
      continue;
    }

    if (!activeDate) {
      throw new Error(`Expected a date header before "${line}".`);
    }

    const block = lines.slice(index, index + 6);
    if (block.length < 6) {
      throw new Error(
        `Incomplete fund-order block for ${activeDate} near "${line}".`,
      );
    }

    const [orderKind, amountText, fundName, cadence, status, quantityText] =
      block;
    rows.push({
      orderDate: activeDate,
      orderKind,
      amountEur: normalizeSpanishDecimal(amountText.replace("€", "")),
      fundName,
      cadence,
      status,
      quantity:
        quantityText === "-"
          ? null
          : normalizeSpanishDecimal(
              quantityText.replace(/participaciones?/i, ""),
            ),
    });
    index += 6;
  }

  return rows;
}

export function buildFundOrderHistoryImportPlan(
  dataset: DomainDataset,
  accountId: string,
  rows: readonly FundOrderHistoryRow[],
  options?: {
    matchDayWindow?: number;
    matchAmountToleranceEur?: string;
  },
): FundOrderHistoryImportPlan {
  const finalizedRows = rows.filter(
    (row) => row.status === FINALIZED_STATUS && row.quantity !== null,
  );
  const rejectedRows = rows.filter((row) => row.status === REJECTED_STATUS);
  const unresolvedRows: FundOrderHistoryIssue[] = [];
  const matchedTransactionPatches: FundOrderHistoryTransactionPatch[] = [];
  const openingPositions: FundOrderHistoryOpeningPosition[] = [];
  const matchDayWindow = options?.matchDayWindow ?? DEFAULT_MATCH_DAY_WINDOW;
  const matchAmountToleranceEur = new Decimal(
    options?.matchAmountToleranceEur ?? DEFAULT_MATCH_AMOUNT_TOLERANCE_EUR,
  );

  const transactionsBySecurityId = buildFundBuyTransactionsBySecurityId(
    dataset,
    accountId,
  );
  const securityByNormalizedName = buildFundSecurityLookup(dataset);
  const usedTransactionIds = new Set<string>();

  for (const row of finalizedRows
    .slice()
    .sort((left, right) =>
      `${left.orderDate}:${left.fundName}:${left.amountEur}`.localeCompare(
        `${right.orderDate}:${right.fundName}:${right.amountEur}`,
      ),
    )) {
    const resolvedSecurity = securityByNormalizedName.get(
      normalizeFundOrderText(row.fundName),
    );

    if (!resolvedSecurity) {
      unresolvedRows.push({
        row,
        reason: `No fund security matched "${row.fundName}".`,
      });
      continue;
    }

    const quantity = row.quantity;
    if (!quantity) {
      unresolvedRows.push({
        row,
        reason: `No quantity was available for "${row.fundName}".`,
      });
      continue;
    }

    const candidates =
      transactionsBySecurityId.get(resolvedSecurity.id)?.filter(
        (transaction) => !usedTransactionIds.has(transaction.id),
      ) ?? [];

    const bestMatch = candidates
      .map((transaction) => ({
        transaction,
        metrics: scoreFundOrderTransactionMatch(row, transaction),
      }))
      .filter(
        ({ metrics }) =>
          metrics.dayDistance <= matchDayWindow &&
          metrics.amountDiffEur.lte(matchAmountToleranceEur),
      )
      .sort((left, right) => {
        if (left.metrics.dayDistance !== right.metrics.dayDistance) {
          return left.metrics.dayDistance - right.metrics.dayDistance;
        }
        const amountDelta = left.metrics.amountDiffEur.comparedTo(
          right.metrics.amountDiffEur,
        );
        if (amountDelta !== 0) {
          return amountDelta;
        }
        return `${left.transaction.transactionDate}:${left.transaction.id}`.localeCompare(
          `${right.transaction.transactionDate}:${right.transaction.id}`,
        );
      })[0];

    if (!bestMatch) {
      openingPositions.push({
        securityId: resolvedSecurity.id,
        fundName: row.fundName,
        orderDate: row.orderDate,
        orderKind: row.orderKind,
        quantity,
        costBasisEur: row.amountEur,
      });
      continue;
    }

    usedTransactionIds.add(bestMatch.transaction.id);
    matchedTransactionPatches.push({
      transactionId: bestMatch.transaction.id,
      securityId: resolvedSecurity.id,
      fundName: row.fundName,
      orderDate: row.orderDate,
      transactionDate: bestMatch.transaction.transactionDate,
      postedDate:
        bestMatch.transaction.postedDate ??
        bestMatch.transaction.transactionDate,
      quantity,
      unitPriceOriginal: buildFundOrderUnitPriceOriginal(
        bestMatch.transaction,
        quantity,
      ),
      actualAmountEur: absoluteDecimal(
        bestMatch.transaction.amountBaseEur,
      ).toFixed(8),
      orderAmountEur: row.amountEur,
      amountDiffEur: bestMatch.metrics.amountDiffEur.toFixed(8),
      dayDistance: bestMatch.metrics.dayDistance,
    });
  }

  return {
    parsedRows: [...rows],
    rejectedRows,
    finalizedRows,
    unresolvedRows,
    matchedTransactionPatches,
    openingPositions,
  };
}

export function reconcileFundOrderHistoryImportPlan(
  dataset: DomainDataset,
  accountId: string,
  plan: FundOrderHistoryImportPlan,
): FundOrderHistoryReconciliationPlan {
  const relevantSecurityIds = new Set(
    [
      ...plan.matchedTransactionPatches.map((patch) => patch.securityId),
      ...plan.openingPositions.map((position) => position.securityId),
    ].filter(Boolean),
  );
  const securityNamesById = new Map(
    dataset.securities.map((security) => [security.id, security.name]),
  );
  const openingAdjustmentsByKey = new Map<
    string,
    FundOrderHistoryExistingOpeningAdjustment[]
  >();

  for (const adjustment of dataset.holdingAdjustments) {
    if (
      adjustment.accountId !== accountId ||
      adjustment.reason !== "opening_position" ||
      !relevantSecurityIds.has(adjustment.securityId)
    ) {
      continue;
    }
    const normalized = normalizeExistingOpeningAdjustment(
      adjustment,
      securityNamesById,
    );
    const key = buildOpeningAdjustmentKey({
      securityId: normalized.securityId,
      orderDate: normalized.orderDate,
      quantity: normalized.quantity,
      costBasisEur: normalized.costBasisEur,
    });
    const existing = openingAdjustmentsByKey.get(key) ?? [];
    existing.push(normalized);
    openingAdjustmentsByKey.set(key, existing);
  }

  const staleOpeningAdjustments: FundOrderHistoryExistingOpeningAdjustment[] =
    [];
  const existingOpeningPositions: FundOrderHistoryExistingOpeningAdjustment[] =
    [];
  const openingPositionsToCreate: FundOrderHistoryOpeningPosition[] = [];

  for (const patch of plan.matchedTransactionPatches) {
    const key = buildOpeningAdjustmentKey({
      securityId: patch.securityId,
      orderDate: patch.orderDate,
      quantity: patch.quantity,
      costBasisEur: patch.orderAmountEur,
    });
    const existing = openingAdjustmentsByKey.get(key) ?? [];
    staleOpeningAdjustments.push(...existing);
    openingAdjustmentsByKey.delete(key);
  }

  for (const openingPosition of plan.openingPositions) {
    const key = buildOpeningAdjustmentKey(openingPosition);
    const existing = openingAdjustmentsByKey.get(key) ?? [];
    if (existing.length > 0) {
      existingOpeningPositions.push(existing[0]!);
      if (existing.length > 1) {
        staleOpeningAdjustments.push(...existing.slice(1));
      }
      openingAdjustmentsByKey.delete(key);
      continue;
    }
    openingPositionsToCreate.push(openingPosition);
  }

  return {
    staleOpeningAdjustments,
    existingOpeningPositions,
    openingPositionsToCreate,
  };
}

function isFundOrderDateHeader(value: string) {
  return /^\d{2}\/\d{2}\/\d{4}$/.test(value);
}

function normalizeFundOrderDate(value: string) {
  const [day, month, year] = value.split("/");
  return `${year}-${month}-${day}`;
}

function normalizeSpanishDecimal(value: string) {
  const normalized = value.replace(/\./g, "").replace(",", ".").trim();
  return new Decimal(normalized).toFixed(8);
}

function normalizeFundOrderText(value: string) {
  return value
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .toUpperCase();
}

function buildFundSecurityLookup(dataset: DomainDataset) {
  const byName = new Map<string, Security>();
  const exactNameMatches = dataset.securities
    .filter((security) => security.name.includes("Vanguard"))
    .sort((left, right) => {
      const leftScore = left.providerName === "manual_fund_nav" ? 1 : 0;
      const rightScore = right.providerName === "manual_fund_nav" ? 1 : 0;
      return rightScore - leftScore;
    });

  for (const security of exactNameMatches) {
    const key = normalizeFundOrderText(security.name);
    if (!byName.has(key)) {
      byName.set(key, security);
    }
  }

  return byName;
}

function normalizeExistingOpeningAdjustment(
  adjustment: HoldingAdjustment,
  securityNamesById: Map<string, string>,
): FundOrderHistoryExistingOpeningAdjustment {
  return {
    adjustmentId: adjustment.id,
    securityId: adjustment.securityId,
    fundName: securityNamesById.get(adjustment.securityId) ?? adjustment.securityId,
    orderDate: adjustment.effectiveDate,
    quantity: normalizeDecimalString(adjustment.shareDelta),
    costBasisEur: normalizeDecimalString(adjustment.costBasisDeltaEur ?? "0"),
  };
}

function buildOpeningAdjustmentKey(input: {
  securityId: string;
  orderDate: string;
  quantity: string;
  costBasisEur: string;
}) {
  return [
    input.securityId,
    input.orderDate,
    normalizeDecimalString(input.quantity),
    normalizeDecimalString(input.costBasisEur),
  ].join(":");
}

function buildFundBuyTransactionsBySecurityId(
  dataset: DomainDataset,
  accountId: string,
) {
  const bySecurityId = new Map<string, Transaction[]>();
  for (const transaction of dataset.transactions) {
    if (
      transaction.accountId !== accountId ||
      transaction.transactionClass !== "investment_trade_buy" ||
      !transaction.securityId
    ) {
      continue;
    }
    const existing = bySecurityId.get(transaction.securityId) ?? [];
    existing.push(transaction);
    bySecurityId.set(transaction.securityId, existing);
  }
  return bySecurityId;
}

function scoreFundOrderTransactionMatch(
  row: FundOrderHistoryRow,
  transaction: Transaction,
) {
  const orderDate = parse(row.orderDate, "yyyy-MM-dd", new Date());
  const transactionDate = parse(
    transaction.transactionDate,
    "yyyy-MM-dd",
    new Date(),
  );
  const postedDate = parse(
    transaction.postedDate ?? transaction.transactionDate,
    "yyyy-MM-dd",
    new Date(),
  );
  return {
    dayDistance: Math.min(
      Math.abs(differenceInCalendarDays(orderDate, transactionDate)),
      Math.abs(differenceInCalendarDays(orderDate, postedDate)),
    ),
    amountDiffEur: absoluteDecimal(transaction.amountBaseEur).minus(
      row.amountEur,
    ).abs(),
  };
}

function buildFundOrderUnitPriceOriginal(
  transaction: Transaction,
  quantity: string,
) {
  const normalizedQuantity = new Decimal(quantity);
  if (normalizedQuantity.eq(0)) {
    return null;
  }

  const amountOriginal = new Decimal(transaction.amountOriginal).abs();
  return amountOriginal.div(normalizedQuantity).toFixed(8);
}

function absoluteDecimal(value: string) {
  return new Decimal(value).abs();
}

function normalizeDecimalString(value: string) {
  return new Decimal(value).toFixed(8);
}

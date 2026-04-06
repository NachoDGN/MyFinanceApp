import { Decimal } from "decimal.js";

import {
  isTransactionResolvedForAnalytics,
  type DomainDataset,
  type HoldingRow,
} from "@myfinance/domain";

import { convertBaseEurToDisplayAmount } from "./currency";

type MutableHoldingDisplayMetric = {
  quantity: Decimal;
  openCostBasisDisplay: Decimal | null;
};

export type HoldingDisplayMetric = {
  avgCostDisplay: string | null;
  openCostBasisDisplay: string | null;
  currentValueDisplay: string | null;
  unrealizedDisplay: string | null;
  unrealizedDisplayPercent: string | null;
};

export function getHoldingDisplayMetricKey(holding: {
  entityId: string;
  accountId: string;
  securityId: string;
}) {
  return `${holding.entityId}:${holding.accountId}:${holding.securityId}`;
}

function safeDividePercent(numerator: Decimal, denominator: Decimal) {
  if (denominator.eq(0)) {
    return null;
  }
  return numerator.div(denominator).mul(100).toFixed(2);
}

function addDisplayCost(
  state: MutableHoldingDisplayMetric,
  amountDisplay: string | null,
  options?: { absolute?: boolean },
) {
  if (amountDisplay === null) {
    state.openCostBasisDisplay = null;
    return;
  }
  if (state.openCostBasisDisplay === null) {
    return;
  }
  const amount = new Decimal(amountDisplay);
  state.openCostBasisDisplay = state.openCostBasisDisplay.plus(
    options?.absolute === false ? amount : amount.abs(),
  );
}

export function buildHoldingDisplayMetricsMap(
  dataset: DomainDataset,
  holdings: HoldingRow[],
  displayCurrency: string,
  referenceDate: string,
) {
  const holdingKeys = new Set(
    holdings.map((holding) => getHoldingDisplayMetricKey(holding)),
  );
  const investmentAccountsById = new Map(
    dataset.accounts
      .filter((account) => account.assetDomain === "investment")
      .map((account) => [account.id, account]),
  );
  const stateByKey = new Map<string, MutableHoldingDisplayMetric>();

  const ensureState = (key: string) => {
    const existing = stateByKey.get(key);
    if (existing) {
      return existing;
    }

    const created: MutableHoldingDisplayMetric = {
      quantity: new Decimal(0),
      openCostBasisDisplay: new Decimal(0),
    };
    stateByKey.set(key, created);
    return created;
  };

  const events = [
    ...dataset.transactions
      .filter((transaction) => {
        if (transaction.transactionDate > referenceDate) {
          return false;
        }
        if (!transaction.securityId) {
          return false;
        }
        if (!isTransactionResolvedForAnalytics(transaction)) {
          return false;
        }
        if (!investmentAccountsById.has(transaction.accountId)) {
          return false;
        }

        const key = getHoldingDisplayMetricKey({
          entityId: transaction.economicEntityId,
          accountId: transaction.accountId,
          securityId: transaction.securityId,
        });
        if (!holdingKeys.has(key)) {
          return false;
        }

        return (
          transaction.transactionClass === "investment_trade_buy" ||
          transaction.transactionClass === "investment_trade_sell"
        );
      })
      .map((transaction) => ({
        type: "transaction" as const,
        sortKey: `${transaction.transactionDate}:1:${transaction.createdAt}`,
        transaction,
      })),
    ...dataset.holdingAdjustments
      .filter((adjustment) => {
        if (adjustment.effectiveDate > referenceDate) {
          return false;
        }
        const key = getHoldingDisplayMetricKey({
          entityId: adjustment.entityId,
          accountId: adjustment.accountId,
          securityId: adjustment.securityId,
        });
        return holdingKeys.has(key);
      })
      .map((adjustment) => ({
        type: "adjustment" as const,
        sortKey: `${adjustment.effectiveDate}:0:${adjustment.createdAt}`,
        adjustment,
      })),
  ].sort((left, right) => left.sortKey.localeCompare(right.sortKey));

  for (const event of events) {
    if (event.type === "adjustment") {
      const adjustment = event.adjustment;
      const state = ensureState(
        getHoldingDisplayMetricKey({
          entityId: adjustment.entityId,
          accountId: adjustment.accountId,
          securityId: adjustment.securityId,
        }),
      );
      state.quantity = state.quantity.plus(adjustment.shareDelta);
      addDisplayCost(
        state,
        adjustment.costBasisDeltaEur
          ? convertBaseEurToDisplayAmount(
              dataset,
              adjustment.costBasisDeltaEur,
              displayCurrency,
              adjustment.effectiveDate,
            )
          : "0.00",
        { absolute: false },
      );
      continue;
    }

    const transaction = event.transaction;
    const state = ensureState(
      getHoldingDisplayMetricKey({
        entityId: transaction.economicEntityId,
        accountId: transaction.accountId,
        securityId: transaction.securityId!,
      }),
    );
    const absoluteQuantity = new Decimal(transaction.quantity ?? 0).abs();
    if (absoluteQuantity.lte(0)) {
      continue;
    }

    if (transaction.transactionClass === "investment_trade_buy") {
      state.quantity = state.quantity.plus(absoluteQuantity);
      addDisplayCost(
        state,
        convertBaseEurToDisplayAmount(
          dataset,
          transaction.amountBaseEur,
          displayCurrency,
          transaction.transactionDate,
        ),
      );
      continue;
    }

    if (
      transaction.transactionClass === "investment_trade_sell" &&
      state.quantity.gt(0)
    ) {
      const sellQuantity = Decimal.min(state.quantity, absoluteQuantity);
      if (state.openCostBasisDisplay !== null) {
        const averageDisplayCost = state.quantity.eq(0)
          ? new Decimal(0)
          : state.openCostBasisDisplay.div(state.quantity);
        state.openCostBasisDisplay = Decimal.max(
          new Decimal(0),
          state.openCostBasisDisplay.minus(averageDisplayCost.mul(sellQuantity)),
        );
      }
      state.quantity = state.quantity.minus(sellQuantity);
    }
  }

  return new Map(
    holdings.map((holding) => {
      const key = getHoldingDisplayMetricKey(holding);
      const state = stateByKey.get(key);
      const currentValueDisplay = convertBaseEurToDisplayAmount(
        dataset,
        holding.currentValueEur,
        displayCurrency,
        referenceDate,
      );
      const openCostBasisDisplay =
        state && state.quantity.gt(0) && state.openCostBasisDisplay !== null
          ? state.openCostBasisDisplay.toFixed(2)
          : null;
      const avgCostDisplay =
        state &&
        state.quantity.gt(0) &&
        state.openCostBasisDisplay !== null
          ? state.openCostBasisDisplay.div(state.quantity).toFixed(2)
          : null;
      const unrealizedDisplay =
        currentValueDisplay !== null && openCostBasisDisplay !== null
          ? new Decimal(currentValueDisplay)
              .minus(openCostBasisDisplay)
              .toFixed(2)
          : null;
      const unrealizedDisplayPercent =
        unrealizedDisplay !== null && openCostBasisDisplay !== null
          ? safeDividePercent(
              new Decimal(unrealizedDisplay),
              new Decimal(openCostBasisDisplay),
            )
          : null;

      return [
        key,
        {
          avgCostDisplay,
          openCostBasisDisplay,
          currentValueDisplay,
          unrealizedDisplay,
          unrealizedDisplayPercent,
        } satisfies HoldingDisplayMetric,
      ] as const;
    }),
  );
}

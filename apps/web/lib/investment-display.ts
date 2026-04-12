import { Decimal } from "decimal.js";

import { type DomainDataset, type HoldingRow } from "@myfinance/domain";

import { convertBaseEurToDisplayAmount } from "./currency";

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

function resolveOpenCostBasisEur(holding: HoldingRow) {
  if (holding.currentValueEur !== null && holding.unrealizedPnlEur !== null) {
    return new Decimal(holding.currentValueEur)
      .minus(holding.unrealizedPnlEur)
      .toFixed(8);
  }

  const quantity = new Decimal(holding.quantity);
  if (quantity.lte(0)) {
    return null;
  }

  return new Decimal(holding.avgCostEur).mul(quantity).toFixed(8);
}

export function buildHoldingDisplayMetricsMap(
  dataset: DomainDataset,
  holdings: HoldingRow[],
  displayCurrency: string,
  referenceDate: string,
) {
  return new Map(
    holdings.map((holding) => {
      const openCostBasisEur = resolveOpenCostBasisEur(holding);
      const currentValueDisplay = convertBaseEurToDisplayAmount(
        dataset,
        holding.currentValueEur,
        displayCurrency,
        referenceDate,
      );
      const openCostBasisDisplay =
        openCostBasisEur === null
          ? null
          : convertBaseEurToDisplayAmount(
              dataset,
              openCostBasisEur,
              displayCurrency,
              referenceDate,
            );
      const avgCostDisplay =
        openCostBasisDisplay !== null && new Decimal(holding.quantity).gt(0)
          ? new Decimal(openCostBasisDisplay)
              .div(holding.quantity)
              .toFixed(2)
          : null;
      const unrealizedDisplay = convertBaseEurToDisplayAmount(
        dataset,
        holding.unrealizedPnlEur,
        displayCurrency,
        referenceDate,
      );
      const unrealizedDisplayPercent =
        holding.unrealizedPnlPercent ??
        (unrealizedDisplay !== null && openCostBasisDisplay !== null
          ? safeDividePercent(
              new Decimal(unrealizedDisplay),
              new Decimal(openCostBasisDisplay),
            )
          : null);

      return [
        getHoldingDisplayMetricKey(holding),
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

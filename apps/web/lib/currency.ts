import { Decimal } from "decimal.js";

import type { DomainDataset } from "@myfinance/domain";

function resolveStoredFxRate(
  dataset: DomainDataset,
  from: string,
  to: string,
  effectiveDate: string,
) {
  if (from === to) {
    return new Decimal(1);
  }

  const direct = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === from &&
        row.quoteCurrency === to &&
        row.asOfDate <= effectiveDate,
    )
    .sort(
      (left, right) =>
        right.asOfDate.localeCompare(left.asOfDate) ||
        right.asOfTimestamp.localeCompare(left.asOfTimestamp),
    )[0];

  const reverse = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === to &&
        row.quoteCurrency === from &&
        row.asOfDate <= effectiveDate,
    )
    .sort(
      (left, right) =>
        right.asOfDate.localeCompare(left.asOfDate) ||
        right.asOfTimestamp.localeCompare(left.asOfTimestamp),
    )[0];

  if (!direct && !reverse) {
    return null;
  }

  if (
    direct &&
    (!reverse ||
      direct.asOfDate > reverse.asOfDate ||
      (direct.asOfDate === reverse.asOfDate &&
        direct.asOfTimestamp >= reverse.asOfTimestamp))
  ) {
    return new Decimal(direct.rate);
  }

  if (reverse) {
    return new Decimal(1).div(reverse.rate);
  }

  return null;
}

export function convertBaseEurToDisplayAmount(
  dataset: DomainDataset,
  amountBaseEur: string | null | undefined,
  displayCurrency: string,
  effectiveDate: string,
) {
  if (amountBaseEur === null || amountBaseEur === undefined) {
    return null;
  }

  if (displayCurrency === "EUR") {
    return new Decimal(amountBaseEur).toFixed(2);
  }

  const rate = resolveStoredFxRate(
    dataset,
    "EUR",
    displayCurrency,
    effectiveDate,
  );
  if (rate === null) {
    return null;
  }

  return new Decimal(amountBaseEur)
    .mul(rate)
    .toFixed(2);
}

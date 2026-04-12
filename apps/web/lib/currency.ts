import { Decimal } from "decimal.js";

import { resolveFxRate, type DomainDataset } from "@myfinance/domain";
import { formatCurrency } from "./formatters";

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

export function endOfMonthIso(value: string) {
  const [yearText, monthText] = value.slice(0, 7).split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const nextMonth = new Date(Date.UTC(year, month, 1));
  nextMonth.setUTCDate(0);
  return nextMonth.toISOString().slice(0, 10);
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

export function convertBaseEurToDisplayAmountWithFallback(
  dataset: DomainDataset,
  amountBaseEur: string | null | undefined,
  displayCurrency: string,
  effectiveDate: string,
  options: {
    fallbackDate?: string;
  } = {},
) {
  if (amountBaseEur === null || amountBaseEur === undefined) {
    return {
      amount: null,
      usedFallbackFx: false,
    };
  }

  if (displayCurrency === "EUR") {
    return {
      amount: new Decimal(amountBaseEur).toFixed(2),
      usedFallbackFx: false,
    };
  }

  const primaryRate = resolveStoredFxRate(
    dataset,
    "EUR",
    displayCurrency,
    effectiveDate,
  );
  if (primaryRate !== null) {
    return {
      amount: new Decimal(amountBaseEur).mul(primaryRate).toFixed(2),
      usedFallbackFx: false,
    };
  }

  const fallbackDate = options.fallbackDate ?? effectiveDate;
  return {
    amount: new Decimal(amountBaseEur)
      .mul(resolveFxRate(dataset, "EUR", displayCurrency, fallbackDate))
      .toFixed(2),
    usedFallbackFx: true,
  };
}

export function formatBaseEurAmountForDisplay(
  dataset: DomainDataset,
  amountBaseEur: string | null | undefined,
  displayCurrency: string,
  effectiveDate: string,
) {
  if (amountBaseEur === null || amountBaseEur === undefined) {
    return formatCurrency(null, displayCurrency);
  }

  const converted =
    displayCurrency === "EUR"
      ? new Decimal(amountBaseEur).toFixed(2)
      : new Decimal(amountBaseEur)
          .mul(resolveFxRate(dataset, "EUR", displayCurrency, effectiveDate))
          .toFixed(2);

  return formatCurrency(
    converted,
    displayCurrency,
  );
}

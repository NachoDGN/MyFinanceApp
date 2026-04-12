import assert from "node:assert/strict";
import test from "node:test";

import { buildIncomeReadModel } from "../packages/analytics/src/index.ts";
import { convertBaseEurToDisplayAmountWithFallback, endOfMonthIso } from "../apps/web/lib/currency.ts";
import { createDataset, createFxRate, createTransaction } from "./support/create-dataset";

test("income source breakdown merges obvious legal-name aliases", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "whitebox-company",
        transactionDate: "2026-02-06",
        postedDate: "2026-02-06",
        amountOriginal: "4000.00",
        amountBaseEur: "4000.00",
        transactionClass: "income",
        categoryCode: "business_income",
        counterpartyName: "Thewhitebox Company",
        descriptionRaw: "Invoice payment",
        descriptionClean: "INVOICE PAYMENT",
      }),
      createTransaction({
        id: "whitebox-srl",
        transactionDate: "2026-03-02",
        postedDate: "2026-03-02",
        amountOriginal: "4200.00",
        amountBaseEur: "4200.00",
        transactionClass: "income",
        categoryCode: "business_income",
        counterpartyName: "Thewhiteboxcompany S.r.l.",
        descriptionRaw: "Invoice payment 2",
        descriptionClean: "INVOICE PAYMENT 2",
      }),
      createTransaction({
        id: "other-source",
        transactionDate: "2026-03-12",
        postedDate: "2026-03-12",
        amountOriginal: "500.00",
        amountBaseEur: "500.00",
        transactionClass: "income",
        categoryCode: "salary",
        counterpartyName: "Another Client SL",
        descriptionRaw: "Small payment",
        descriptionClean: "SMALL PAYMENT",
      }),
    ],
  });

  const model = buildIncomeReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: { preset: "ytd", start: "2026-01-01", end: "2026-04-09" },
    referenceDate: "2026-04-09",
  });

  const whiteboxRow = model.sourceRows.find((row) =>
    row.aliases.includes("Thewhitebox Company"),
  );

  assert.ok(whiteboxRow);
  assert.equal(whiteboxRow.amountEur, "8200.00");
  assert.deepEqual(whiteboxRow.aliases, [
    "Thewhiteboxcompany S.r.l.",
    "Thewhitebox Company",
  ]);
  assert.equal(model.sourceRows.length, 2);
});

test("income chart conversion keeps full history visible in display currency", () => {
  const dataset = createDataset({
    fxRates: [
      createFxRate({
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-02-06",
        asOfTimestamp: "2026-02-06T16:00:00Z",
        rate: "1.17940000",
      }),
      createFxRate({
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T08:20:00Z",
        rate: "1.08695700",
      }),
    ],
  });

  assert.equal(endOfMonthIso("2026-02-01"), "2026-02-28");

  const februaryAmount = convertBaseEurToDisplayAmountWithFallback(
    dataset,
    "100.00",
    "USD",
    endOfMonthIso("2026-02-01"),
    { fallbackDate: "2026-04-09" },
  );
  const januaryAmount = convertBaseEurToDisplayAmountWithFallback(
    dataset,
    "100.00",
    "USD",
    endOfMonthIso("2026-01-01"),
    { fallbackDate: "2026-04-09" },
  );

  assert.equal(februaryAmount.amount, "117.94");
  assert.equal(februaryAmount.usedFallbackFx, false);
  assert.equal(januaryAmount.amount, "108.70");
  assert.equal(januaryAmount.usedFallbackFx, true);
});

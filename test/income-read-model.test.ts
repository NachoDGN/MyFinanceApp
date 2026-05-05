import assert from "node:assert/strict";
import test from "node:test";

import {
  buildIncomeCategoryReadModel,
  buildIncomeReadModel,
} from "../packages/analytics/src/index.ts";
import {
  getPreviousComparablePeriod,
  resolvePeriodSelection,
} from "../packages/domain/src/index.ts";
import {
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
} from "../apps/web/lib/currency.ts";
import {
  createDataset,
  createFxRate,
  createTransaction,
} from "./support/create-dataset";

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

test("income overview exposes category rows and monthly category composition", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "jan-salary",
        transactionDate: "2026-01-31",
        postedDate: "2026-01-31",
        amountOriginal: "1000.00",
        amountBaseEur: "1000.00",
        transactionClass: "income",
        categoryCode: "salary",
        counterpartyName: "Employer",
      }),
      createTransaction({
        id: "apr-dividend",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "50.00",
        amountBaseEur: "50.00",
        transactionClass: "dividend",
        categoryCode: null,
        counterpartyName: "Broker",
      }),
      createTransaction({
        id: "apr-expense",
        transactionDate: "2026-04-04",
        postedDate: "2026-04-04",
        amountOriginal: "-25.00",
        amountBaseEur: "-25.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        merchantNormalized: "MARKET",
      }),
    ],
  });

  const model = buildIncomeReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "ytd",
      referenceDate: "2026-04-15",
    }),
    referenceDate: "2026-04-15",
  });

  const april = model.incomeCategoryMonthlySeries.find(
    (row) => row.month === "2026-04-01",
  );

  assert.deepEqual(model.incomeCategoryRows, [
    { categoryCode: "salary", label: "Salary", amountEur: "1000.00" },
    { categoryCode: "__dividend", label: "Dividend", amountEur: "50.00" },
  ]);
  assert.equal(april?.totalIncomeEur, "50.00");
  assert.deepEqual(april?.categories, [
    { categoryCode: "__dividend", label: "Dividend", amountEur: "50.00" },
  ]);
});

test("income category read model returns matching transactions and selected-period trend", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "jan-salary",
        transactionDate: "2026-01-31",
        postedDate: "2026-01-31",
        amountOriginal: "1000.00",
        amountBaseEur: "1000.00",
        transactionClass: "income",
        categoryCode: "salary",
        counterpartyName: "Employer",
      }),
      createTransaction({
        id: "mar-salary",
        transactionDate: "2026-03-31",
        postedDate: "2026-03-31",
        amountOriginal: "500.00",
        amountBaseEur: "500.00",
        transactionClass: "income",
        categoryCode: "salary",
        counterpartyName: "Employer SL",
      }),
      createTransaction({
        id: "apr-interest",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "500.00",
        amountBaseEur: "500.00",
        transactionClass: "interest",
        categoryCode: null,
        counterpartyName: "Bank",
      }),
    ],
  });

  const model = buildIncomeCategoryReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    categoryCode: "salary",
    period: resolvePeriodSelection({
      preset: "ytd",
      referenceDate: "2026-04-15",
    }),
    referenceDate: "2026-04-15",
  });

  assert.equal(model.category?.label, "Salary");
  assert.equal(model.amountEur, "1500.00");
  assert.equal(model.periodSharePercent, "75.00");
  assert.deepEqual(
    model.transactions.map((transaction) => transaction.id),
    ["mar-salary", "jan-salary"],
  );
  assert.deepEqual(
    model.monthlySeries.map((row) => [row.month, row.amountEur]),
    [
      ["2026-01-01", "1000.00"],
      ["2026-02-01", "0.00"],
      ["2026-03-01", "500.00"],
      ["2026-04-01", "0.00"],
    ],
  );
  assert.deepEqual(model.sourceRows, [
    {
      label: "Employer",
      aliases: ["Employer", "Employer SL"],
      amountEur: "1500.00",
    },
  ]);
});

test("last month period resolves to the prior full calendar month", () => {
  const period = resolvePeriodSelection({
    preset: "last_month",
    referenceDate: "2026-05-04",
  });

  assert.deepEqual(period, {
    preset: "last_month",
    start: "2026-04-01",
    end: "2026-04-30",
  });
  assert.deepEqual(getPreviousComparablePeriod(period), {
    preset: "last_month",
    start: "2026-03-01",
    end: "2026-03-31",
  });
});

test("income totals include pending positive cash-flow rows as unresolved income", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "resolved-salary",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "2500.00",
        amountBaseEur: "2500.00",
        transactionClass: "income",
        categoryCode: "salary",
        counterpartyName: "Employer",
      }),
      createTransaction({
        id: "pending-stripe",
        transactionDate: "2026-04-13",
        postedDate: "2026-04-13",
        amountOriginal: "3350.65",
        amountBaseEur: "3350.65",
        transactionClass: "unknown",
        categoryCode: null,
        counterpartyName: "Stripe Technology Europe Ltd",
        classificationStatus: "unknown",
        classificationSource: "system_fallback",
        classificationConfidence: "0.00",
        needsReview: true,
        reviewReason: "Queued for automatic transaction analysis.",
        llmPayload: { analysisStatus: "pending" },
      }),
      createTransaction({
        id: "pending-outflow",
        transactionDate: "2026-04-14",
        postedDate: "2026-04-14",
        amountOriginal: "-184.00",
        amountBaseEur: "-184.00",
        transactionClass: "unknown",
        categoryCode: null,
        merchantNormalized: "OpenAI",
        classificationStatus: "unknown",
        classificationSource: "system_fallback",
        classificationConfidence: "0.00",
        needsReview: true,
        reviewReason: "Queued for automatic transaction analysis.",
        llmPayload: { analysisStatus: "pending" },
      }),
    ],
  });

  const model = buildIncomeReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "last_month",
      referenceDate: "2026-05-04",
    }),
    referenceDate: "2026-05-04",
  });

  assert.equal(model.incomeMetric?.valueBaseEur, "5850.65");
  assert.deepEqual(model.incomeCategoryRows, [
    {
      categoryCode: "__unresolved_income",
      label: "Unresolved Income",
      amountEur: "3350.65",
    },
    { categoryCode: "salary", label: "Salary", amountEur: "2500.00" },
  ]);
  assert.equal(model.incomeCompletenessPercent, "50.00");
});

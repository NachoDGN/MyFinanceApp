import assert from "node:assert/strict";
import test from "node:test";

import {
  buildSpendingCategoryReadModel,
  buildSpendingReadModel,
} from "../packages/analytics/src/index.ts";
import { resolvePeriodSelection } from "../packages/domain/src/index.ts";
import { createDataset, createTransaction } from "./support/create-dataset";

test("spending overview exposes monthly category composition for stacked bars", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "jan-groceries",
        transactionDate: "2026-01-12",
        postedDate: "2026-01-12",
        amountOriginal: "-50.00",
        amountBaseEur: "-50.00",
        categoryCode: "groceries",
        merchantNormalized: "MARKET",
      }),
      createTransaction({
        id: "mar-software",
        transactionDate: "2026-03-08",
        postedDate: "2026-03-08",
        amountOriginal: "-40.00",
        amountBaseEur: "-40.00",
        categoryCode: "software",
        merchantNormalized: "CLOUD APP",
      }),
      createTransaction({
        id: "apr-groceries",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        categoryCode: "groceries",
        merchantNormalized: "MARKET",
      }),
    ],
  });

  const model = buildSpendingReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "ytd",
      referenceDate: "2026-04-15",
    }),
    referenceDate: "2026-04-15",
  });

  const april = model.spendingCategoryMonthlySeries.find(
    (row) => row.month === "2026-04-01",
  );

  assert.equal(model.summary.spendingByCategory.length, 2);
  assert.equal(april?.totalSpendingEur, "100.00");
  assert.deepEqual(april?.categories, [
    {
      categoryCode: "groceries",
      label: "Groceries",
      amountEur: "100.00",
    },
  ]);
});

test("spending category read model returns matching transactions and selected-period trend", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "jan-groceries",
        transactionDate: "2026-01-12",
        postedDate: "2026-01-12",
        amountOriginal: "-50.00",
        amountBaseEur: "-50.00",
        categoryCode: "groceries",
        merchantNormalized: "MARKET",
      }),
      createTransaction({
        id: "mar-groceries",
        transactionDate: "2026-03-08",
        postedDate: "2026-03-08",
        amountOriginal: "-75.00",
        amountBaseEur: "-75.00",
        categoryCode: "groceries",
        merchantNormalized: "MARKET",
      }),
      createTransaction({
        id: "apr-software",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        categoryCode: "software",
        merchantNormalized: "CLOUD APP",
      }),
    ],
  });

  const model = buildSpendingCategoryReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    categoryCode: "groceries",
    period: resolvePeriodSelection({
      preset: "ytd",
      referenceDate: "2026-04-15",
    }),
    referenceDate: "2026-04-15",
  });

  assert.equal(model.category?.label, "Groceries");
  assert.equal(model.amountEur, "125.00");
  assert.equal(model.periodSharePercent, "55.56");
  assert.deepEqual(
    model.transactions.map((transaction) => transaction.id),
    ["mar-groceries", "jan-groceries"],
  );
  assert.deepEqual(
    model.monthlySeries.map((row) => [row.month, row.amountEur]),
    [
      ["2026-01-01", "50.00"],
      ["2026-02-01", "0.00"],
      ["2026-03-01", "75.00"],
      ["2026-04-01", "0.00"],
    ],
  );
  assert.deepEqual(model.merchantRows, [
    { label: "MARKET", amountEur: "125.00" },
  ]);
});

test("spending totals include pending negative cash-flow rows but skip card settlements", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "resolved-software",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        categoryCode: "software",
        merchantNormalized: "CLOUD APP",
      }),
      createTransaction({
        id: "pending-openai",
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
      createTransaction({
        id: "card-settlement",
        transactionDate: "2026-04-20",
        postedDate: "2026-04-20",
        amountOriginal: "-9902.66",
        amountBaseEur: "-9902.66",
        transactionClass: "unknown",
        categoryCode: null,
        descriptionRaw:
          "Liquidacion De Las Tarjetas De Credito Del Contrato 0049",
        descriptionClean:
          "LIQUIDACION DE LAS TARJETAS DE CREDITO DEL CONTRATO 0049",
        creditCardStatementStatus: "uploaded",
        classificationStatus: "unknown",
        classificationSource: "system_fallback",
        classificationConfidence: "0.00",
        needsReview: true,
        llmPayload: { analysisStatus: "pending" },
      }),
    ],
  });

  const model = buildSpendingReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "last_month",
      referenceDate: "2026-05-04",
    }),
    referenceDate: "2026-05-04",
  });

  assert.equal(model.spendMetric?.valueBaseEur, "284.00");
  assert.deepEqual(model.summary.spendingByCategory, [
    {
      categoryCode: "__unresolved_spending",
      label: "Unresolved Spending",
      amountEur: "184.00",
    },
    { categoryCode: "software", label: "Software", amountEur: "100.00" },
  ]);
});

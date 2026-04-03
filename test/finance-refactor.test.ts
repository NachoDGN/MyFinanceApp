import assert from "node:assert/strict";
import test from "node:test";

import { buildMetricResult } from "../packages/analytics/src/index.ts";
import { enrichImportedTransaction } from "../packages/classification/src/index.ts";
import {
  buildHoldingRows,
  buildImportedTransactions,
  getPreviousComparablePeriod,
  resolvePeriodSelection,
} from "../packages/domain/src/index.ts";

import {
  createAccount,
  createDataset,
  createRule,
  createTransaction,
} from "./support/create-dataset";

test("import building deduplicates by fingerprint and keeps the dataset user id", () => {
  const input = {
    accountId: "account-1",
    templateId: "template-1",
    originalFilename: "upload.csv",
    filePath: "/tmp/upload.csv",
  } as const;
  const duplicateRow = {
    transaction_date: "2026-04-01",
    posted_date: "2026-04-01",
    description_raw: "Groceries",
    amount_original_signed: "-25.00",
    currency_original: "EUR",
  } as const;
  const seed = buildImportedTransactions(createDataset(), input, "seed-batch", [duplicateRow]);
  const dataset = createDataset({ transactions: seed.inserted });

  const result = buildImportedTransactions(
    dataset,
    input,
    "batch-1",
    [
      duplicateRow,
      {
        transaction_date: "2026-04-02",
        posted_date: "2026-04-02",
        description_raw: "Coffee",
        amount_original_signed: "-3.50",
        currency_original: "EUR",
      },
    ],
  );

  assert.equal(result.duplicateCount, 1);
  assert.equal(result.inserted.length, 1);
  assert.equal(result.inserted[0]?.userId, dataset.profile.id);
  assert.equal(result.inserted[0]?.descriptionClean, "COFFEE");
});

test("month-to-date metrics use a dynamic comparison window and ignore internal transfers", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "income-apr",
        transactionDate: "2026-04-01",
        postedDate: "2026-04-01",
        amountOriginal: "1000.00",
        amountBaseEur: "1000.00",
        transactionClass: "income",
        categoryCode: "salary",
        descriptionRaw: "Salary",
        descriptionClean: "SALARY",
      }),
      createTransaction({
        id: "spend-apr",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Groceries",
        descriptionClean: "GROCERIES",
      }),
      createTransaction({
        id: "transfer-apr",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "-500.00",
        amountBaseEur: "-500.00",
        transactionClass: "transfer_internal",
        categoryCode: null,
        descriptionRaw: "Broker transfer",
        descriptionClean: "BROKER TRANSFER",
      }),
      createTransaction({
        id: "spend-mar",
        transactionDate: "2026-03-02",
        postedDate: "2026-03-02",
        amountOriginal: "-80.00",
        amountBaseEur: "-80.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Groceries",
        descriptionClean: "GROCERIES",
      }),
    ],
  });

  const currentPeriod = resolvePeriodSelection({
    preset: "mtd",
    referenceDate: "2026-04-03",
  });
  const previousPeriod = getPreviousComparablePeriod(currentPeriod);
  const spending = buildMetricResult(
    dataset,
    { kind: "consolidated" },
    "EUR",
    "spending_mtd_total",
    { referenceDate: "2026-04-03" },
  );
  const operatingNet = buildMetricResult(
    dataset,
    { kind: "consolidated" },
    "EUR",
    "operating_net_cash_flow_mtd",
    { referenceDate: "2026-04-03" },
  );

  assert.deepEqual(currentPeriod, {
    start: "2026-04-01",
    end: "2026-04-03",
    preset: "mtd",
  });
  assert.deepEqual(previousPeriod, {
    start: "2026-03-01",
    end: "2026-03-03",
    preset: "mtd",
  });
  assert.equal(spending.valueBaseEur, "100.00");
  assert.equal(spending.comparisonValueBaseEur, "80.00");
  assert.equal(operatingNet.valueBaseEur, "900.00");
});

test("saved classification rules win before fallback logic or LLM classification", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  try {
    const account = createAccount();
    const transaction = createTransaction({
      id: "notion-row",
      descriptionRaw: "Notion subscription",
      descriptionClean: "NOTION SUBSCRIPTION",
      categoryCode: null,
      transactionClass: "unknown",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      rules: [createRule()],
    });

    const decision = await enrichImportedTransaction(dataset, account, transaction);

    assert.equal(decision.classificationSource, "user_rule");
    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "software");
    assert.equal(decision.merchantNormalized, "NOTION");
    assert.equal(decision.needsReview, false);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
  }
});

test("holding valuation is computed from positions, quotes, and FX instead of hardcoded values", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "ABC",
        canonicalSymbol: "ABC",
        displaySymbol: "ABC",
        name: "ABC Corp",
        exchangeName: "NYSE",
        exchangeMic: "XNYS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T15:00:00Z",
        price: "10.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T15:00:00Z",
        rate: "0.500000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-1",
        securityId: "security-1",
        openQuantity: "4.00",
        openCostBasisEur: "15.00",
        avgCostEur: "3.75",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-04-01",
        lastRebuiltAt: "2026-04-03T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(dataset, { kind: "consolidated" }, "2026-04-03");

  assert.equal(holding?.currentValueEur, "20.00");
  assert.equal(holding?.unrealizedPnlEur, "5.00");
  assert.equal(holding?.unrealizedPnlPercent, "33.33");
});

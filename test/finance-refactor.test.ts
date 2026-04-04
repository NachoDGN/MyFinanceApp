import assert from "node:assert/strict";
import test from "node:test";

import {
  buildInvestmentsReadModel,
  buildMetricResult,
  buildSpendingReadModel,
} from "../packages/analytics/src/index.ts";
import {
  applyRuleMatch,
  enrichImportedTransaction,
  getInvestmentTransactionClassifierConfig,
  parseInvestmentEvent,
} from "../packages/classification/src/index.ts";
import { prepareInvestmentRebuild } from "../packages/db/src/investment-rebuild.ts";
import {
  buildHoldingRows,
  buildImportedTransactions,
  createTemplateConfig,
  getDatasetLatestDate,
  getLatestInvestmentCashBalances,
  getPreviousComparablePeriod,
  getScopeLatestDate,
  rebuildInvestmentState,
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
  const seed = buildImportedTransactions(createDataset(), input, "seed-batch", [
    duplicateRow,
  ]);
  const dataset = createDataset({ transactions: seed.inserted });

  const result = buildImportedTransactions(dataset, input, "batch-1", [
    duplicateRow,
    {
      transaction_date: "2026-04-02",
      posted_date: "2026-04-02",
      description_raw: "Coffee",
      amount_original_signed: "-3.50",
      currency_original: "EUR",
    },
  ]);

  assert.equal(result.duplicateCount, 1);
  assert.equal(result.inserted.length, 1);
  assert.equal(result.inserted[0]?.userId, dataset.profile.id);
  assert.equal(result.inserted[0]?.descriptionClean, "COFFEE");
});

test("import building deduplicates rounded investment rows against precise existing trades", () => {
  const input = {
    accountId: "broker-1",
    templateId: "template-1",
    originalFilename: "upload.csv",
    filePath: "/tmp/upload.csv",
  } as const;
  const investmentAccount = createAccount({
    id: "broker-1",
    displayName: "Broker",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-amd",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        assetType: "stock",
        quoteCurrency: "USD",
        active: true,
        metadataJson: {},
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
    transactions: [
      createTransaction({
        id: "existing-investment-trade",
        accountId: investmentAccount.id,
        transactionDate: "2026-03-04",
        postedDate: "2026-03-05",
        amountOriginal: "-215.46000000",
        amountBaseEur: "-215.46000000",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "uncategorized_investment",
        rawPayload: {
          _import: {
            transaction_type_raw: "buy",
          },
        },
        securityId: "security-amd",
        quantity: "1.00000000",
        unitPriceOriginal: "215.46000000",
      }),
    ],
  });

  const result = buildImportedTransactions(dataset, input, "batch-1", [
    {
      transaction_date: "2026-03-04",
      posted_date: "2026-03-05",
      description_raw: "ADVANCED MICRO DEVICES @ 1",
      amount_original_signed: "-215.00",
      currency_original: "EUR",
      transaction_type_raw: "buy",
      security_symbol: "AMD",
      quantity: "1",
      unit_price_original: "215",
    },
  ]);

  assert.equal(result.duplicateCount, 1);
  assert.equal(result.inserted.length, 0);
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
        id: "unresolved-spend-apr",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-40.00",
        amountBaseEur: "-40.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Unresolved groceries",
        descriptionClean: "UNRESOLVED GROCERIES",
        needsReview: true,
        reviewReason: "Needs confirmation.",
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

test("scope latest date prefers newer market and FX data over the latest transaction date", () => {
  const investmentAccount = createAccount({
    id: "brokerage-latest-date",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "older-trade",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-100.00",
        amountBaseEur: "-92.00",
        currencyOriginal: "USD",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        securityId: "security-amd-latest-date",
        quantity: "1.00000000",
      }),
    ],
    securities: [
      {
        id: "security-amd-latest-date",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-03-24T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-amd-latest-date",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T20:00:00Z",
        price: "110.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T20:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T20:00:00Z",
        rate: "0.92000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: investmentAccount.id,
        securityId: "security-amd-latest-date",
        openQuantity: "1.00000000",
        openCostBasisEur: "92.00000000",
        avgCostEur: "92.00000000",
        realizedPnlEur: "0.00000000",
        dividendsEur: "0.00000000",
        interestEur: "0.00000000",
        feesEur: "0.00000000",
        lastTradeDate: "2026-03-24",
        lastRebuiltAt: "2026-04-03T20:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  assert.equal(
    getScopeLatestDate(dataset, { kind: "consolidated" }),
    "2026-04-03",
  );
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

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
    );

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

test("latest date helpers cap future imports at the provided fallback date", () => {
  const account = createAccount({
    id: "broker-future-dates",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "future-import-row",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        securityId: "security-amd-latest-date",
        transactionDate: "2026-12-03",
        postedDate: "2026-12-03",
      }),
    ],
    securityPrices: [
      {
        securityId: "security-amd-latest-date",
        priceDate: "2026-04-02",
        quoteTimestamp: "2026-04-02T20:00:00Z",
        price: "110.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: { close: "110.00" },
        createdAt: "2026-04-02T20:00:00Z",
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: account.entityId,
        accountId: account.id,
        securityId: "security-amd-latest-date",
        openQuantity: "1.00000000",
        openCostBasisEur: "92.00000000",
        avgCostEur: "92.00000000",
        realizedPnlEur: "0.00000000",
        dividendsEur: "0.00000000",
        interestEur: "0.00000000",
        feesEur: "0.00000000",
        lastTradeDate: "2026-04-02",
        lastRebuiltAt: "2026-04-02T20:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  assert.equal(getDatasetLatestDate(dataset, "2026-04-04"), "2026-04-02");
  assert.equal(
    getScopeLatestDate(
      dataset,
      { kind: "account", accountId: account.id },
      "2026-04-04",
    ),
    "2026-04-02",
  );
});

test("investment review uses the dedicated model override when configured", () => {
  const previous = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";

  try {
    assert.equal(
      getInvestmentTransactionClassifierConfig().model,
      "gpt-5.4-mini",
    );
  } finally {
    if (previous === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previous;
    }
  }
});

test("investment parser recognizes named fund purchases even without explicit quantity", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "investment_trade_buy");
  assert.equal(parsed.securityHint, "VANGUARD US 500 STOCK INDEX EU");
});

test("investment parser stores sell quantities as negative values", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "240.00",
      amountBaseEur: "240.00",
      descriptionRaw: "ADVANCED MICRO DEVICES @ 8",
      descriptionClean: "ADVANCED MICRO DEVICES @ 8",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "investment_trade_sell");
  assert.equal(parsed.quantity, "-8.00000000");
});

test("investment parser recognizes periodic brokerage credits as interest", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "0.14",
      amountBaseEur: "0.14",
      descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
      descriptionClean: "PERIODO 19/02/2026 19/03/2026",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "interest");
});

test("rule matching respects account scope", () => {
  const transaction = createTransaction({
    accountId: "account-1",
    descriptionRaw: "Notion subscription",
    descriptionClean: "NOTION SUBSCRIPTION",
  });
  const outOfScopeRule = createRule({
    scopeJson: { account_id: "account-2" },
  });

  assert.equal(applyRuleMatch(transaction, [outOfScopeRule]), null);
});

test("investment rebuild upgrades stale unknown investment rows into parsed trade buys", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  process.env.TWELVE_DATA_API_KEY = "";

  try {
    const account = createAccount({
      id: "broker-1",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "vanguard-row",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason:
        "The description suggests an investment but lacks details for precise classification.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.transactionClass, "investment_trade_buy");
    assert.equal(patch?.categoryCode, "stock_buy");
    assert.match(patch?.reviewReason ?? "", /Security mapping unresolved/i);
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild explains when a mapped trade still lacks quantity", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  process.env.TWELVE_DATA_API_KEY = "";

  try {
    const account = createAccount({
      id: "broker-2",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "mapped-without-quantity",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-vusa",
      needsReview: true,
      reviewReason:
        "The description suggests an investment but lacks details for precise classification.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-vusa",
          providerName: "manual",
          providerSymbol: "VUSA",
          canonicalSymbol: "VUSA",
          displaySymbol: "VUSA",
          name: "Vanguard S&P 500 UCITS ETF",
          exchangeName: "LSE",
          micCode: "XLON",
          assetType: "etf",
          quoteCurrency: "EUR",
          country: "IE",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(
      patch?.reviewReason ?? "",
      /Mapped to VUSA, but market-data enrichment is unavailable/i,
    );
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild flags trades whose implied unit price is implausible", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2026-03-16",
              close: "294.45",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-4",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "goog-mismatch",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.89",
      amountBaseEur: "1.89",
      descriptionRaw: "ALPHABET INC CL C @ 15",
      descriptionClean: "ALPHABET INC CL C @ 15",
      transactionClass: "investment_trade_sell",
      categoryCode: "uncategorized_investment",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-goog",
      quantity: "15.00000000",
      unitPriceOriginal: "0.13000000",
      needsReview: false,
      reviewReason: null,
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-goog",
          providerName: "manual",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc.",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(patch?.reviewReason ?? "", /Mapped to GOOG/i);
    assert.match(
      patch?.reviewReason ?? "",
      /diverges from available market data/i,
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild rejects historical quotes that are far older than the requested trade date", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";

  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2025-09-17",
              close: "24.90",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-historical-drift",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "intc-missing-quantity",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-12",
      postedDate: "2026-03-12",
      amountOriginal: "-41.00",
      amountBaseEur: "-37.72",
      currencyOriginal: "USD",
      descriptionRaw: "INTEL CORP",
      descriptionClean: "INTEL CORP",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-intc-drift",
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Needs quantity derivation.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-intc-drift",
          providerName: "twelve_data",
          providerSymbol: "INTC",
          canonicalSymbol: "INTC",
          displaySymbol: "INTC",
          name: "Intel Corporation",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "United States",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      fxRates: [
        {
          baseCurrency: "USD",
          quoteCurrency: "EUR",
          asOfDate: "2026-03-12",
          asOfTimestamp: "2026-03-12T16:00:00Z",
          rate: "0.92000000",
          sourceName: "twelve_data",
          rawJson: {},
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-12");
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "intc-missing-quantity",
    );

    assert.equal(rebuilt.upsertedPrices.length, 0);
    assert.match(
      patch?.reviewReason ?? "",
      /did not return a usable historical price/i,
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild requests end-of-day quotes on weekends", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  const requestedUrls: string[] = [];

  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    requestedUrls.push(requestUrl);
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2026-04-01",
              close: "100.00",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    if (url.pathname.endsWith("/quote")) {
      return new Response(
        JSON.stringify({
          close: "110.00",
          currency: "USD",
          datetime: "2026-04-03",
          is_market_open: "false",
          last_quote_at: 1775232000,
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const investmentAccount = createAccount({
      id: "brokerage-weekend",
      accountType: "brokerage_account",
      assetDomain: "investment",
      defaultCurrency: "USD",
    });
    const dataset = createDataset({
      accounts: [investmentAccount],
      transactions: [
        createTransaction({
          id: "weekend-buy",
          accountId: investmentAccount.id,
          accountEntityId: investmentAccount.entityId,
          economicEntityId: investmentAccount.entityId,
          transactionDate: "2026-04-01",
          postedDate: "2026-04-01",
          amountOriginal: "-100.00",
          amountBaseEur: "-92.00",
          currencyOriginal: "USD",
          descriptionRaw: "AMD @ 1",
          descriptionClean: "AMD @ 1",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          securityId: "security-amd-weekend",
          quantity: "1.00000000",
          unitPriceOriginal: "100.00",
        }),
      ],
      securities: [
        {
          id: "security-amd-weekend",
          providerName: "twelve_data",
          providerSymbol: "AMD",
          canonicalSymbol: "AMD",
          displaySymbol: "AMD",
          name: "Advanced Micro Devices Inc",
          exchangeName: "NASDAQ",
          micCode: "XNAS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-04-01T08:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-amd-weekend",
          priceDate: "2026-04-03",
          quoteTimestamp: "2026-04-03T08:20:00Z",
          price: "152.40",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: {},
          createdAt: "2026-04-03T12:37:43Z",
        },
      ],
      fxRates: [
        {
          baseCurrency: "USD",
          quoteCurrency: "EUR",
          asOfDate: "2026-04-03",
          asOfTimestamp: "2026-04-03T20:00:00Z",
          rate: "0.92000000",
          sourceName: "twelve_data",
          rawJson: {},
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-04-04");
    const latestPrice = rebuilt.upsertedPrices.find(
      (price) =>
        price.securityId === "security-amd-weekend" &&
        price.priceDate === "2026-04-03",
    );
    const quoteRequest = requestedUrls.find((url) => url.includes("/quote"));

    assert.ok(quoteRequest);
    assert.equal(new URL(quoteRequest).searchParams.get("eod"), "true");
    assert.equal(latestPrice?.price, "110.00");
    assert.equal(latestPrice?.isDelayed, true);
    assert.equal(latestPrice?.isRealtime, false);
    assert.notDeepEqual(latestPrice?.rawJson, {});
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild uses stored historical prices for price sanity checks", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  delete process.env.TWELVE_DATA_API_KEY;

  try {
    const account = createAccount({
      id: "broker-4b",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "goog-stored-mismatch",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.89",
      amountBaseEur: "1.89",
      descriptionRaw: "ALPHABET INC CL C @ 15",
      descriptionClean: "ALPHABET INC CL C @ 15",
      transactionClass: "investment_trade_sell",
      categoryCode: "uncategorized_investment",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-goog-stored",
      quantity: "15.00000000",
      unitPriceOriginal: "0.13000000",
      needsReview: false,
      reviewReason: null,
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-goog-stored",
          providerName: "manual",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc.",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-goog-stored",
          priceDate: "2026-03-13",
          quoteTimestamp: "2026-03-13T16:00:00Z",
          price: "301.45999",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: {},
          createdAt: "2026-03-13T16:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(patch?.reviewReason ?? "", /Mapped to GOOG/i);
    assert.match(
      patch?.reviewReason ?? "",
      /diverges from available market data/i,
    );
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild clears quantity and unit price for non-trade rows", async () => {
  const account = createAccount({
    id: "broker-fee-cleanup",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "commission-row",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        transactionDate: "2026-03-16",
        postedDate: "2026-03-16",
        amountOriginal: "1.89",
        amountBaseEur: "1.89",
        descriptionRaw: "ALPHABET INC CL C @ 15 COMMISSION",
        descriptionClean: "ALPHABET INC CL C @ 15 COMMISSION",
        transactionClass: "fee",
        categoryCode: "broker_fee",
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        securityId: "security-goog-fee",
        quantity: "15.00000000",
        unitPriceOriginal: "0.13000000",
        needsReview: false,
        reviewReason: null,
      }),
    ],
    securities: [
      {
        id: "security-goog-fee",
        providerName: "manual",
        providerSymbol: "GOOG",
        canonicalSymbol: "GOOG",
        displaySymbol: "GOOG",
        name: "Alphabet Inc.",
        exchangeName: "NASDAQ",
        micCode: "XNGS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
  const patch = rebuilt.transactionPatches[0];

  assert.equal(patch?.quantity, null);
  assert.equal(patch?.unitPriceOriginal, null);
});

test("investment rebuild clears stale review flags for deterministic interest rows", async () => {
  const account = createAccount({
    id: "broker-interest",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const transaction = createTransaction({
    id: "period-interest",
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionDate: "2026-03-20",
    postedDate: "2026-03-20",
    amountOriginal: "0.14",
    amountBaseEur: "0.14",
    descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
    descriptionClean: "PERIODO 19/02/2026 19/03/2026",
    transactionClass: "interest",
    categoryCode: "interest",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.61",
    needsReview: true,
    reviewReason:
      "The transaction description and data do not clearly indicate a known transaction type or category.",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [transaction],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-20");
  const patch = rebuilt.transactionPatches[0];

  assert.equal(patch?.needsReview, false);
  assert.equal(patch?.reviewReason, null);
});

test("investment rebuild remaps stale EU fund aliases away from USD OTC securities", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/symbol_search")) {
      return new Response(
        JSON.stringify({
          data: [
            {
              symbol: "0P00000MNK",
              instrument_name: "Vanguard U.S. 500 Stock Index F",
              exchange: "OTC",
              mic_code: "OTCM",
              instrument_type: "Mutual Fund",
              country: "United States",
              currency: "USD",
            },
            {
              symbol: "0P00000G12",
              instrument_name:
                "Vanguard U.S. 500 Stock Index Fund Investor EUR Accumulation",
              exchange: "XHAM",
              mic_code: "XHAM",
              instrument_type: "Mutual Fund",
              country: "Germany",
              currency: "EUR",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-3",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "stale-eu-alias",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-wrong-vanguard",
      needsReview: true,
      reviewReason:
        'Security mapping unresolved for "VANGUARD US 500 STOCK INDEX EU".',
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-wrong-vanguard",
          providerName: "twelve_data",
          providerSymbol: "0P00000MNK",
          canonicalSymbol: "0P00000MNK",
          displaySymbol: "0P00000MNK",
          name: "Vanguard U.S. 500 Stock Index F",
          exchangeName: "OTC",
          micCode: "OTCM",
          assetType: "etf",
          quoteCurrency: "USD",
          country: "United States",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityAliases: [
        {
          id: "alias-wrong-vanguard",
          securityId: "security-wrong-vanguard",
          aliasTextNormalized: "VANGUARD US 500 STOCK INDEX EU",
          aliasSource: "provider",
          templateId: null,
          confidence: "0.9000",
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");

    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "0P00000G12");
    assert.equal(
      rebuilt.transactionPatches[0]?.securityId,
      rebuilt.insertedSecurities[0]?.id,
    );
    assert.equal(
      rebuilt.insertedAliases[0]?.aliasTextNormalized,
      "VANGUARD US 500 STOCK INDEX EU",
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("successful confident LLM classifications clear fallback review state", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "transfer_internal",
          category_code: "uncategorized_investment",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: null,
          confidence: 0.9,
          explanation: "Looks like a transfer between owned accounts.",
          reason: "Description indicates an internal transfer.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const account = createAccount({
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "broker-transfer",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      descriptionRaw: "Transferencia My Investor",
      descriptionClean: "TRANSFERENCIA MY INVESTOR",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          previousReviewReason: transaction.reviewReason ?? null,
          previousUserContext: "Previous manual note.",
          previousLlmPayload: {
            analysisStatus: "done",
            model: "gpt-4.1-mini",
          },
          userProvidedContext:
            "This is a broker commission for GOOG, not a stock sale.",
        },
      },
    );
    const llmPayload = decision.llmPayload as {
      reviewContext?: {
        userProvidedContext?: string;
        trigger?: string;
      };
      timing?: {
        requestedAt?: string;
        completedAt?: string;
        durationMs?: number;
      };
    };

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "transfer_internal");
    assert.equal(decision.needsReview, false);
    assert.equal(decision.reviewReason, null);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("invalid LLM economic entity overrides are ignored", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "Vanguard",
          counterparty_name: "Vanguard Japan Stock EUR INS",
          economic_entity_override: "Vanguard Japan Stock EUR INS",
          security_hint: "Vanguard Japan Stock EUR INS",
          confidence: 0.91,
          explanation: "This is a clearly named Vanguard investment purchase.",
          reason:
            "The description names a Vanguard fund, but it does not contain a valid entity override.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const account = createAccount({
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "MyInvestor",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "vanguard-invalid-entity-override",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      descriptionRaw: "VANGUARD JAPAN STOCK EUR INS @",
      descriptionClean: "VANGUARD JAPAN STOCK EUR INS @",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
    );

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "investment_trade_buy");
    assert.equal(decision.economicEntityId, account.entityId);
    assert.equal(
      decision.llmPayload.llm &&
        typeof decision.llmPayload.llm === "object" &&
        "economicEntityId" in decision.llmPayload.llm
        ? (decision.llmPayload.llm as { economicEntityId: string | null })
            .economicEntityId
        : null,
      null,
    );
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("investment review includes portfolio state and can override commission-like sells", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  let capturedUserPrompt = "";
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      input?: Array<{ role?: string; content?: Array<{ text?: string }> }>;
    };
    capturedUserPrompt =
      requestBody.input?.find((item) => item.role === "user")?.content?.[0]
        ?.text ?? "";

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "fee",
          category_code: "broker_fee",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: "ALPHABET INC CL C",
          confidence: 0.95,
          explanation: "The row looks like a broker commission, not a sale.",
          reason:
            "The implied per-share amount is far below the latest GOOG quote while the position remains open.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-goog",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "MyInvestor",
      displayName: "Brokerage",
      defaultCurrency: "EUR",
    });
    const otherAccount = createAccount({
      id: "broker-other",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Other Broker",
      displayName: "Other Brokerage",
      defaultCurrency: "EUR",
    });
    const transaction = createTransaction({
      id: "goog-commission-row",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.00",
      amountBaseEur: "1.00",
      currencyOriginal: "EUR",
      descriptionRaw: "ALPHABET INC CL C @ 8",
      descriptionClean: "ALPHABET INC CL C @ 8",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
      manualNotes: "Previous manual note.",
      securityId: "security-goog",
      quantity: null,
      unitPriceOriginal: null,
    });
    const baseCategories = createDataset().categories;
    const dataset = createDataset({
      accounts: [account, otherAccount],
      auditEvents: [
        {
          id: "audit-interest-example",
          actorType: "user",
          actorId: "user-1",
          actorName: "web-review-editor",
          sourceChannel: "web",
          commandName: "transactions.review_reanalyze",
          objectType: "transaction",
          objectId: "interest-example-tx",
          beforeJson: {
            accountId: account.id,
            transactionDate: "2026-03-20",
            postedDate: "2026-03-20",
            amountOriginal: "0.14",
            currencyOriginal: "EUR",
            descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
            merchantNormalized: null,
            counterpartyName: null,
            securityId: null,
            quantity: null,
            unitPriceOriginal: null,
            transactionClass: "unknown",
            categoryCode: "uncategorized_investment",
            classificationSource: "llm",
            classificationStatus: "llm",
            classificationConfidence: "0.51",
            needsReview: true,
            reviewReason: "Needs user confirmation.",
            llmPayload: {
              model: "gpt-4.1-mini",
              explanation: "No deterministic classifier matched the imported row.",
              reason: "The row might be interest, but the context is thin.",
            },
          },
          afterJson: {
            accountId: account.id,
            transactionClass: "interest",
            categoryCode: "uncategorized_investment",
            merchantNormalized: "MyInvestor",
            counterpartyName: "MyInvestor",
            quantity: null,
            unitPriceOriginal: null,
            reviewReason: null,
            manualNotes: "This is, in fact, earned interest.",
            llmPayload: {
              reviewContext: {
                userProvidedContext: "This is, in fact, earned interest.",
              },
            },
          },
          createdAt: "2026-03-21T09:00:00Z",
          notes:
            "Re-ran LLM classification for a single transaction with manual review context.",
        },
      ],
      categories: [
        ...baseCategories,
        {
          code: "broker_fee",
          displayName: "Broker Fee",
          parentCode: null,
          scopeKind: "investment",
          directionKind: "investment",
          sortOrder: 50,
          active: true,
          metadataJson: {},
        },
      ],
      transactions: [
        transaction,
        createTransaction({
          id: "goog-similar-history",
          accountId: account.id,
          accountEntityId: account.entityId,
          economicEntityId: account.entityId,
          transactionDate: "2026-03-05",
          postedDate: "2026-03-05",
          amountOriginal: "-1040.00",
          amountBaseEur: "-1040.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C @ 5",
          descriptionClean: "ALPHABET INC CL C @ 5",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          needsReview: false,
          reviewReason: null,
          securityId: "security-goog",
          quantity: "5.00000000",
          unitPriceOriginal: "208.00000000",
        }),
        createTransaction({
          id: "goog-pending-history",
          accountId: account.id,
          accountEntityId: account.entityId,
          economicEntityId: account.entityId,
          transactionDate: "2026-03-08",
          postedDate: "2026-03-08",
          amountOriginal: "-999.00",
          amountBaseEur: "-999.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C PENDING REVIEW",
          descriptionClean: "ALPHABET INC CL C PENDING REVIEW",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "llm",
          classificationSource: "llm",
          classificationConfidence: "0.52",
          needsReview: true,
          reviewReason: "Still ambiguous.",
          securityId: "security-goog",
          quantity: "5.00000000",
          unitPriceOriginal: "199.80000000",
        }),
        createTransaction({
          id: "other-account-goog",
          accountId: otherAccount.id,
          accountEntityId: otherAccount.entityId,
          economicEntityId: otherAccount.entityId,
          transactionDate: "2026-03-06",
          postedDate: "2026-03-06",
          amountOriginal: "-1200.00",
          amountBaseEur: "-1200.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C FROM OTHER ACCOUNT",
          descriptionClean: "ALPHABET INC CL C FROM OTHER ACCOUNT",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          needsReview: false,
          reviewReason: null,
          securityId: "security-goog",
          quantity: "6.00000000",
          unitPriceOriginal: "200.00000000",
        }),
      ],
      securities: [
        {
          id: "security-goog",
          providerName: "twelve_data",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc Class C",
          exchangeName: "NASDAQ",
          micCode: "XNAS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-goog",
          priceDate: "2026-03-16",
          quoteTimestamp: "2026-03-16T20:00:00Z",
          price: "215.40",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: { close: "215.40" },
          createdAt: "2026-03-16T20:00:00Z",
        },
      ],
      investmentPositions: [
        {
          userId: "user-1",
          entityId: account.entityId,
          accountId: account.id,
          securityId: "security-goog",
          openQuantity: "45.00000000",
          openCostBasisEur: "7200.00000000",
          avgCostEur: "160.00000000",
          realizedPnlEur: "0.00000000",
          dividendsEur: "0.00000000",
          interestEur: "0.00000000",
          feesEur: "0.00000000",
          lastTradeDate: "2026-03-01",
          lastRebuiltAt: "2026-03-16T20:00:00Z",
          provenanceJson: { source: "transactions" },
          unrealizedComplete: true,
        },
      ],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          userProvidedContext:
            "This is a broker commission for GOOG, not a stock sale.",
        },
      },
    );

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "fee");
    assert.equal(decision.categoryCode, "broker_fee");
    assert.equal(decision.needsReview, false);
    assert.equal(decision.quantity, null);
    assert.equal(decision.unitPriceOriginal, null);
    assert.match(capturedUserPrompt, /Portfolio state:/);
    assert.match(capturedUserPrompt, /Similar same-account resolved history:/);
    assert.match(
      capturedUserPrompt,
      /"descriptionRaw":"ALPHABET INC CL C @ 5"/,
    );
    assert.doesNotMatch(
      capturedUserPrompt,
      /ALPHABET INC CL C PENDING REVIEW/,
    );
    assert.doesNotMatch(
      capturedUserPrompt,
      /ALPHABET INC CL C FROM OTHER ACCOUNT/,
    );
    assert.match(capturedUserPrompt, /"symbol":"GOOG"/);
    assert.match(capturedUserPrompt, /"quantity":"45\.00000000"/);
    assert.match(capturedUserPrompt, /"impliedUnitPrice":"0\.13"/);
    assert.match(capturedUserPrompt, /"latestHoldingPrice":"215\.40"/);
    assert.match(
      capturedUserPrompt,
      /Examples from prior user corrections:/,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 transaction metadata: .*"descriptionRaw":"PERIODO 19\/02\/2026 19\/03\/2026"/,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 initial inference: .*"transactionClass":"unknown".*"model":"gpt-4\.1-mini"/,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 user feedback: This is, in fact, earned interest\./,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 corrected outcome: .*"transactionClass":"interest"/,
    );
    assert.match(capturedUserPrompt, /Review trigger: manual_review_update/);
    assert.match(
      capturedUserPrompt,
      /Previous user review context: Previous manual note\./,
    );
    assert.match(
      capturedUserPrompt,
      /New user review context: This is a broker commission for GOOG, not a stock sale\./,
    );
    const llmPayload = decision.llmPayload as {
      reviewContext?: {
        userProvidedContext?: string | null;
        trigger?: string | null;
      };
      timing?: {
        requestedAt?: string | null;
        completedAt?: string | null;
        durationMs?: number | null;
      };
      reviewExamplesUsed?: Array<{
        auditEventId?: string | null;
      }>;
    };
    assert.equal(
      llmPayload.reviewContext?.userProvidedContext,
      "This is a broker commission for GOOG, not a stock sale.",
    );
    assert.equal(llmPayload.reviewContext?.trigger, "manual_review_update");
    assert.equal(llmPayload.reviewExamplesUsed?.length, 1);
    assert.equal(
      llmPayload.reviewExamplesUsed?.[0]?.auditEventId,
      "audit-interest-example",
    );
    assert.equal(typeof llmPayload.timing?.requestedAt, "string");
    assert.equal(typeof llmPayload.timing?.completedAt, "string");
    assert.equal(typeof llmPayload.timing?.durationMs, "number");
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("spending read model respects the selected period when building merchant totals", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "apr-merchant",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-45.00",
        amountBaseEur: "-45.00",
        merchantNormalized: "COFFEE BAR",
        descriptionClean: "COFFEE BAR",
      }),
      createTransaction({
        id: "mar-merchant",
        transactionDate: "2026-03-20",
        postedDate: "2026-03-20",
        amountOriginal: "-90.00",
        amountBaseEur: "-90.00",
        merchantNormalized: "COFFEE BAR",
        descriptionClean: "COFFEE BAR",
      }),
    ],
    monthlyCashFlowRollups: [
      {
        entityId: "entity-1",
        month: "2026-03-01",
        incomeEur: "0.00",
        spendingEur: "90.00",
        operatingNetEur: "-90.00",
      },
      {
        entityId: "entity-1",
        month: "2026-04-01",
        incomeEur: "0.00",
        spendingEur: "45.00",
        operatingNetEur: "-45.00",
      },
    ],
  });

  const model = buildSpendingReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.transactions.length, 1);
  assert.equal(model.transactions[0]?.id, "apr-merchant");
  assert.deepEqual(model.merchantRows, [
    { label: "COFFEE BAR", amountEur: "45.00" },
  ]);
});

test("template config builder converts typed inputs into stored JSON rules", () => {
  const config = createTemplateConfig({
    columnMappings: [
      { target: "transaction_date", source: "Fecha" },
      { target: "description_raw", source: "Concepto" },
      { target: "amount_original_signed", source: "Importe" },
    ],
    signMode: "amount_direction_column",
    directionColumn: "Tipo",
    debitValuesText: "cargo",
    creditValuesText: "abono",
    dateDayFirst: true,
  });

  assert.deepEqual(config.columnMapJson, {
    transaction_date: "Fecha",
    description_raw: "Concepto",
    amount_original_signed: "Importe",
  });
  assert.deepEqual(config.signLogicJson, {
    mode: "amount_direction_column",
    direction_column: "Tipo",
    debit_values: ["cargo"],
    credit_values: ["abono"],
  });
  assert.deepEqual(config.normalizationRulesJson, {
    date_day_first: true,
  });
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

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-03",
  );

  assert.equal(holding?.currentValueEur, "20.00");
  assert.equal(holding?.unrealizedPnlEur, "5.00");
  assert.equal(holding?.unrealizedPnlPercent, "33.33");
});

test("holding valuation uses as-of FX even when the latest quote is older than the FX series", () => {
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
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
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
        priceDate: "2026-04-01",
        quoteTimestamp: "2026-04-01T15:00:00Z",
        price: "100.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-01T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
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
        openQuantity: "10.00",
        openCostBasisEur: "900.00",
        avgCostEur: "90.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-04-01",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentValueEur, "920.00");
  assert.equal(holding?.unrealizedPnlEur, "20.00");
  assert.equal(holding?.quoteFreshness, "delayed");
});

test("holding rows ignore placeholder seed quotes when a real market-data row exists", () => {
  const investmentAccount = createAccount({
    id: "brokerage-placeholder",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-placeholder",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
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
        securityId: "security-placeholder",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T08:20:00Z",
        price: "152.40",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T12:37:43Z",
      },
      {
        securityId: "security-placeholder",
        priceDate: "2026-04-02",
        quoteTimestamp: "2026-04-02T19:59:00Z",
        price: "217.50",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {
          symbol: "AMD",
          close: "217.5",
          datetime: "2026-04-02",
        },
        createdAt: "2026-04-03T20:08:02Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-placeholder",
        securityId: "security-placeholder",
        openQuantity: "1.00",
        openCostBasisEur: "100.00",
        avgCostEur: "100.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-03-24",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentPrice, "217.50");
  assert.equal(holding?.quoteTimestamp, "2026-04-02T19:59:00Z");
  assert.equal(holding?.quoteFreshness, "delayed");
});

test("holding freshness is stale when the latest delayed quote is older than five days", () => {
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
        providerSymbol: "INTC",
        canonicalSymbol: "INTC",
        displaySymbol: "INTC",
        name: "Intel Corporation",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
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
        priceDate: "2026-03-20",
        quoteTimestamp: "2026-03-20T15:00:00Z",
        price: "50.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-03-20T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
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
        openQuantity: "15.00",
        openCostBasisEur: "450.00",
        avgCostEur: "30.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-03-20",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentValueEur, "690.00");
  assert.equal(holding?.quoteFreshness, "stale");
});

test("holding rows do not expose current pricing when the last quote is more than thirty days old", () => {
  const investmentAccount = createAccount({
    id: "brokerage-old-quote",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-old-quote",
        providerName: "twelve_data",
        providerSymbol: "INTC",
        canonicalSymbol: "INTC",
        displaySymbol: "INTC",
        name: "Intel Corporation",
        exchangeName: "NASDAQ",
        exchangeMic: "XNGS",
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
        securityId: "security-old-quote",
        priceDate: "2026-01-15",
        quoteTimestamp: "2026-01-15T15:00:00Z",
        price: "24.90",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-01-15T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-old-quote",
        securityId: "security-old-quote",
        openQuantity: "15.00",
        openCostBasisEur: "450.00",
        avgCostEur: "30.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-01-15",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentPrice, null);
  assert.equal(holding?.currentValueEur, null);
  assert.equal(holding?.quoteFreshness, "stale");
});

test("investment rebuild derives open positions and brokerage cash from imported investment rows", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "EUR",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "buy-1",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-01",
        amountOriginal: "-200.00",
        amountBaseEur: "-200.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 2",
        descriptionClean: "ADVANCED MICRO DEVICES @ 2",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        securityId: "security-1",
        quantity: "2.00000000",
        unitPriceOriginal: "100.00",
        rawPayload: {
          Import: {
            balanceOriginal: "450.00",
            balanceCurrency: "EUR",
          },
        },
      }),
      createTransaction({
        id: "sell-1",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-02",
        amountOriginal: "120.00",
        amountBaseEur: "120.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_sell",
        categoryCode: "uncategorized_investment",
        securityId: "security-1",
        quantity: "-1.00000000",
        unitPriceOriginal: "120.00",
        rawPayload: {
          Import: {
            balanceOriginal: "570.00",
            balanceCurrency: "EUR",
          },
        },
      }),
    ],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-03T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T08:00:00Z",
        price: "150.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T08:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T08:00:00Z",
        rate: "0.90000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const rebuilt = rebuildInvestmentState(dataset, "2026-04-03");
  const balances = getLatestInvestmentCashBalances(dataset, "2026-04-03");

  assert.equal(rebuilt.positions.length, 1);
  assert.equal(rebuilt.positions[0]?.openQuantity, "1.00000000");
  assert.equal(rebuilt.positions[0]?.openCostBasisEur, "100.00000000");
  assert.equal(rebuilt.positions[0]?.realizedPnlEur, "20.00000000");
  assert.equal(balances[0]?.balanceBaseEur, "570.00000000");
  assert.equal(rebuilt.snapshots[0]?.totalPortfolioValueEur, "705.00000000");
});

test("investments read model keeps resolved broker transfers visible in the investments ledger", () => {
  const investmentAccount = createAccount({
    id: "brokerage-2",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "broker-transfer",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "500.00",
        amountBaseEur: "500.00",
        descriptionRaw: "Transferencia entre cuentas",
        descriptionClean: "TRANSFERENCIA ENTRE CUENTAS",
        transactionClass: "transfer_internal",
        categoryCode: "uncategorized_investment",
        needsReview: false,
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 1);
  assert.equal(model.investmentRows[0]?.transactionClass, "transfer_internal");
});

test("investments read model exposes unresolved investment items outside the selected period", () => {
  const investmentAccount = createAccount({
    id: "brokerage-3",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "older-review",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-99.58",
        amountBaseEur: "-99.58",
        descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
        descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: true,
        reviewReason: "Mapped to VUSA, but quantity still needs to be derived.",
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 0);
  assert.equal(model.unresolved.length, 1);
  assert.equal(
    model.unresolved[0]?.descriptionRaw,
    "VANGUARD US 500 STOCK INDEX EU",
  );
});

test("investments read model keeps processed rows available outside the selected period", () => {
  const investmentAccount = createAccount({
    id: "brokerage-processed",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "processed-buy",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-99.58",
        amountBaseEur: "-99.58",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 2",
        descriptionClean: "ADVANCED MICRO DEVICES @ 2",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: false,
        quantity: "2.00000000",
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 0);
  assert.equal(model.processedRows.length, 1);
  assert.equal(
    model.processedRows[0]?.descriptionRaw,
    "ADVANCED MICRO DEVICES @ 2",
  );
});

test("unresolved investment rows do not contribute to rebuilt positions or YTD investment KPIs", () => {
  const investmentAccount = createAccount({
    id: "brokerage-unresolved-kpi",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    categories: [
      ...createDataset().categories,
      {
        code: "dividend",
        displayName: "Dividend",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "income",
        sortOrder: 41,
        active: true,
        metadataJson: {},
      },
      {
        code: "stock_buy",
        displayName: "Stock Buy",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "investment",
        sortOrder: 42,
        active: true,
        metadataJson: {},
      },
    ],
    securities: [
      {
        id: "security-amd",
        providerName: "manual",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
    transactions: [
      createTransaction({
        id: "unresolved-dividend",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "18.50",
        amountBaseEur: "18.50",
        descriptionRaw: "Dividend from Vanguard",
        descriptionClean: "DIVIDEND FROM VANGUARD",
        transactionClass: "dividend",
        categoryCode: "dividend",
        needsReview: true,
        reviewReason: "Needs user confirmation.",
      }),
      createTransaction({
        id: "unresolved-buy",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-25",
        postedDate: "2026-03-25",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: true,
        reviewReason: "Low-confidence investment classification.",
        securityId: "security-amd",
        quantity: "1.00000000",
        unitPriceOriginal: "100.00000000",
      }),
    ],
  });

  const rebuilt = rebuildInvestmentState(dataset, "2026-04-03");
  const model = buildInvestmentsReadModel(
    {
      ...dataset,
      investmentPositions: rebuilt.positions,
      dailyPortfolioSnapshots: rebuilt.snapshots,
    },
    {
      scope: { kind: "consolidated" },
      displayCurrency: "EUR",
      period: resolvePeriodSelection({
        preset: "mtd",
        referenceDate: "2026-04-03",
      }),
      referenceDate: "2026-04-03",
    },
  );

  assert.equal(rebuilt.positions.length, 0);
  assert.equal(model.dividendsYtd, "0.00");
  assert.equal(model.unresolved.length, 2);
});

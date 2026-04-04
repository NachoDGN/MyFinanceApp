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
    );

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
        quantity: "1.00000000",
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

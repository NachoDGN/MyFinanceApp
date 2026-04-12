import assert from "node:assert/strict";
import test from "node:test";

import { buildInvestmentsReadModel } from "../packages/analytics/src/index.ts";
import { resolvePeriodSelection } from "../packages/domain/src/index.ts";

import { buildInvestmentsPageModel } from "../apps/web/lib/investments-page.ts";
import {
  convertBaseEurToDisplayAmount,
  formatBaseEurAmountForDisplay,
} from "../apps/web/lib/currency.ts";
import { buildHoldingDisplayMetricsMap } from "../apps/web/lib/investment-display.ts";
import {
  createAccount,
  createDataset,
  createFxRate,
  createInvestmentAccount,
  createInvestmentTransaction,
  createSecurity,
  createSecurityPrice,
  createTransaction,
} from "./support/create-dataset";

test("historical transaction display conversion uses the transaction date instead of the page reference date", () => {
  const dataset = createDataset({
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-02-06",
        asOfTimestamp: "2026-02-06T16:00:00Z",
        rate: "1.17940000",
        sourceName: "banque_france",
        rawJson: {},
      },
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-03-04",
        asOfTimestamp: "2026-03-04T16:00:00Z",
        rate: "1.16490000",
        sourceName: "banque_france",
        rawJson: {},
      },
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  assert.equal(
    convertBaseEurToDisplayAmount(dataset, "-2598.00", "USD", "2026-03-04"),
    "-3026.41",
  );
  assert.equal(
    convertBaseEurToDisplayAmount(dataset, "-2332.34", "USD", "2026-02-06"),
    "-2750.76",
  );
  assert.equal(
    convertBaseEurToDisplayAmount(dataset, "-2598.00", "USD", "2026-04-03"),
    "-2823.91",
  );

  const sparseDataset = createDataset({
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  assert.equal(
    convertBaseEurToDisplayAmount(
      sparseDataset,
      "-2598.00",
      "USD",
      "2026-03-04",
    ),
    null,
  );
});

test("display conversion prefers the freshest reverse FX quote when the direct pair is stale", () => {
  const dataset = createDataset({
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-11",
        asOfTimestamp: "2026-04-11T16:00:00Z",
        rate: "0.85288000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  assert.equal(
    convertBaseEurToDisplayAmount(dataset, "19416.38", "USD", "2026-04-11"),
    "22765.66",
  );
});

test("formatted base-EUR aggregates are converted before rendering in the selected currency", () => {
  const dataset = createDataset({
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-11",
        asOfTimestamp: "2026-04-11T16:00:00Z",
        rate: "0.85288000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  assert.equal(
    formatBaseEurAmountForDisplay(
      dataset,
      "61489.84",
      "USD",
      "2026-04-11",
    ),
    "$72,096.71",
  );
});

test("investments page unrealized KPI uses the canonical metric display amount", () => {
  const account = createInvestmentAccount({
    id: "brokerage-fx-kpi",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createInvestmentTransaction(account, {
        id: "buy-1",
        transactionDate: "2026-04-01",
        postedDate: "2026-04-01",
        amountOriginal: "-100.00",
        currencyOriginal: "USD",
        amountBaseEur: "-100.00",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        securityId: "security-fx",
        quantity: "1.00000000",
        needsReview: false,
        reviewReason: null,
        classificationStatus: "rule",
        classificationSource: "user_rule",
        classificationConfidence: "1.00",
        descriptionRaw: "Buy FX",
        descriptionClean: "BUY FX",
      }),
    ],
    securities: [
      createSecurity({
        id: "security-fx",
        displaySymbol: "FX",
        providerSymbol: "FX",
        canonicalSymbol: "FX",
        quoteCurrency: "USD",
      }),
    ],
    securityPrices: [
      createSecurityPrice({
        securityId: "security-fx",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T15:00:00Z",
        price: "300.00",
        currency: "USD",
      }),
    ],
    fxRates: [
      createFxRate({
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T12:00:00Z",
        rate: "0.500000",
        sourceName: "ecb",
      }),
    ],
  });

  const period = resolvePeriodSelection({
    preset: "mtd",
    referenceDate: "2026-04-03",
  });
  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "USD",
    period,
    referenceDate: "2026-04-03",
  });
  const pageModel = buildInvestmentsPageModel(
    {
      ...model,
      dataset,
      scopeParam: "consolidated",
      currency: "USD",
      referenceDate: "2026-04-03",
      period,
      navigationState: {
        scopeParam: "consolidated",
        currency: "USD",
        period: "mtd",
        referenceDate: "2026-04-03",
      },
      scopeOptions: [{ value: "consolidated", label: "Consolidated" }],
    },
    {},
  );
  const unrealizedCard = pageModel.metricCards.find(
    (card) => card.label === "Unrealized Gain",
  );

  assert.equal(model.metrics.unrealized.valueDisplay, "100.00");
  assert.equal(unrealizedCard?.value, "$100.00");
});

test("holding display metrics preserve canonical unrealized returns across display currencies", () => {
  const investmentAccount = createAccount({
    id: "broker-samsung-summary",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "MyInvestor",
    displayName: "MyInvestor",
    defaultCurrency: "EUR",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "samsung-buy-feb",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-02-06",
        postedDate: "2026-02-10",
        amountOriginal: "-2332.34",
        currencyOriginal: "EUR",
        amountBaseEur: "-2332.34",
        fxRateToEur: "1.00000000",
        descriptionRaw: "SAMSUNG ELECTR-GDR 144-A @ 1",
        descriptionClean: "SAMSUNG ELECTR-GDR 144-A @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        needsReview: false,
        securityId: "security-smsn",
        quantity: "1.00000000",
        unitPriceOriginal: "2332.34000000",
      }),
      createTransaction({
        id: "samsung-buy-mar",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-04",
        postedDate: "2026-03-06",
        amountOriginal: "-2598.00",
        currencyOriginal: "EUR",
        amountBaseEur: "-2598.00",
        fxRateToEur: "1.00000000",
        descriptionRaw: "SAMSUNG ELECTR-GDR 144-A @ 1",
        descriptionClean: "SAMSUNG ELECTR-GDR 144-A @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        needsReview: false,
        securityId: "security-smsn",
        quantity: "1.00000000",
        unitPriceOriginal: "2598.00000000",
      }),
    ],
    securities: [
      {
        id: "security-smsn",
        providerName: "twelve_data",
        providerSymbol: "SMSN",
        canonicalSymbol: "SMSN",
        displaySymbol: "SMSN",
        name: "Samsung Electronics Co., Ltd.",
        exchangeName: "LSE",
        micCode: "XLON",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "United Kingdom",
        isin: "US7960508882",
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-05T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-smsn",
        priceDate: "2026-04-05",
        quoteTimestamp: "2026-04-05T16:00:00Z",
        price: "3022.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-05T16:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-02-06",
        asOfTimestamp: "2026-02-06T16:00:00Z",
        rate: "1.17940000",
        sourceName: "banque_france",
        rawJson: {},
      },
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-03-04",
        asOfTimestamp: "2026-03-04T16:00:00Z",
        rate: "1.16490000",
        sourceName: "banque_france",
        rawJson: {},
      },
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "0.92000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "USD",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-05",
    }),
    referenceDate: "2026-04-05",
  });
  const metrics = buildHoldingDisplayMetricsMap(
    dataset,
    model.holdings.holdings,
    "USD",
    "2026-04-05",
  );
  const samsung = metrics.values().next().value;

  assert.equal(samsung?.avgCostDisplay, "2679.54");
  assert.equal(samsung?.currentValueDisplay, "6044.00");
  assert.equal(samsung?.unrealizedDisplay, "684.94");
  assert.equal(samsung?.unrealizedDisplayPercent, "12.78");
});

test("holding display metrics fall back to reference-date FX when historical trade FX is missing", () => {
  const investmentAccount = createAccount({
    id: "broker-amd-fallback",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "MyInvestor",
    displayName: "MyInvestor",
    defaultCurrency: "EUR",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "amd-buy-mar",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2025-03-11",
        postedDate: "2025-03-13",
        amountOriginal: "-90.76",
        currencyOriginal: "EUR",
        amountBaseEur: "-90.76",
        fxRateToEur: "1.00000000",
        descriptionRaw: "AMD @ 1",
        descriptionClean: "AMD @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        needsReview: false,
        securityId: "security-amd",
        quantity: "1.00000000",
        unitPriceOriginal: "90.76000000",
      }),
    ],
    securities: [
      {
        id: "security-amd",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NDQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "United States",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-06T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-amd",
        priceDate: "2026-04-06",
        quoteTimestamp: "2026-04-06T16:00:00Z",
        price: "217.50",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: true,
        isDelayed: false,
        marketState: "open",
        rawJson: {},
        createdAt: "2026-04-06T16:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "0.92000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "USD",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-06",
    }),
    referenceDate: "2026-04-06",
  });
  const metrics = buildHoldingDisplayMetricsMap(
    dataset,
    model.holdings.holdings,
    "USD",
    "2026-04-06",
  );
  const amd = metrics.values().next().value;

  assert.equal(amd?.avgCostDisplay, "98.65");
  assert.equal(amd?.openCostBasisDisplay, "98.65");
  assert.equal(amd?.currentValueDisplay, "217.50");
  assert.equal(amd?.unrealizedDisplay, "118.85");
  assert.equal(amd?.unrealizedDisplayPercent, "120.47");
});

test("manual fund display metrics preserve canonical unrealized returns across display currencies", () => {
  const cashAccount = createAccount({
    id: "revolut-company-usd",
    accountType: "company_bank",
    assetDomain: "cash",
    institutionName: "Revolut Business",
    displayName: "Revolut USD Main",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [cashAccount],
    transactions: [
      createTransaction({
        id: "fund-october",
        accountId: cashAccount.id,
        accountEntityId: cashAccount.entityId,
        economicEntityId: cashAccount.entityId,
        transactionDate: "2025-10-14",
        postedDate: "2025-10-14",
        amountOriginal: "-2500.00",
        currencyOriginal: "USD",
        amountBaseEur: "-2426.88",
        fxRateToEur: "0.97075200",
        descriptionRaw: "To USD SP:b9611010-6e70-49bd-b353-0f959610715d",
        descriptionClean: "TO USD SP:B9611010-6E70-49BD-B353-0F959610715D",
        transactionClass: "transfer_internal",
      }),
      createTransaction({
        id: "fund-march",
        accountId: cashAccount.id,
        accountEntityId: cashAccount.entityId,
        economicEntityId: cashAccount.entityId,
        transactionDate: "2026-03-31",
        postedDate: "2026-03-31",
        amountOriginal: "-20000.00",
        currencyOriginal: "USD",
        amountBaseEur: "-19415.00",
        fxRateToEur: "0.97075000",
        descriptionRaw: "To Inversión riesgo bajo",
        descriptionClean: "TO INVERSION RIESGO BAJO",
        transactionClass: "transfer_internal",
      }),
    ],
    manualInvestments: [
      {
        id: "manual-bond-fund",
        userId: cashAccount.userId,
        entityId: cashAccount.entityId,
        fundingAccountId: cashAccount.id,
        label: "Inversión en bonos",
        matcherText:
          "Inversión riesgo bajo, SP:b9611010-6e70-49bd-b353-0f959610715d",
        note: null,
        createdAt: "2026-04-11T08:00:00Z",
        updatedAt: "2026-04-11T08:00:00Z",
      },
    ],
    manualInvestmentValuations: [
      {
        id: "manual-bond-fund-valuation",
        userId: cashAccount.userId,
        manualInvestmentId: "manual-bond-fund",
        snapshotDate: "2026-04-11",
        currentValueOriginal: "22765.66",
        currentValueCurrency: "USD",
        note: "Manual mark-to-market",
        createdAt: "2026-04-11T09:00:00Z",
        updatedAt: "2026-04-11T09:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-11",
        asOfTimestamp: "2026-04-11T16:00:00Z",
        rate: "0.85288000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "USD",
    referenceDate: "2026-04-11",
  });
  const metrics = buildHoldingDisplayMetricsMap(
    dataset,
    model.holdings.holdings,
    "USD",
    "2026-04-11",
  );
  const fund = metrics.values().next().value;

  assert.equal(fund?.openCostBasisDisplay, "25609.56");
  assert.equal(fund?.currentValueDisplay, "22765.66");
  assert.equal(fund?.unrealizedDisplay, "-2843.89");
  assert.equal(fund?.unrealizedDisplayPercent, "-11.10");
});

test("manual fund unrealized return direction stays the same across EUR and USD display modes", () => {
  const cashAccount = createAccount({
    id: "revolut-company-usd-parity",
    accountType: "company_bank",
    assetDomain: "cash",
    institutionName: "Revolut Business",
    displayName: "Revolut USD Main",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [cashAccount],
    transactions: [
      createTransaction({
        id: "fund-october-parity",
        accountId: cashAccount.id,
        accountEntityId: cashAccount.entityId,
        economicEntityId: cashAccount.entityId,
        transactionDate: "2025-10-14",
        postedDate: "2025-10-14",
        amountOriginal: "-2500.00",
        currencyOriginal: "USD",
        amountBaseEur: "-2426.88",
        fxRateToEur: "0.97075200",
        descriptionRaw: "To USD SP:b9611010-6e70-49bd-b353-0f959610715d",
        descriptionClean: "TO USD SP:B9611010-6E70-49BD-B353-0F959610715D",
        transactionClass: "transfer_internal",
      }),
      createTransaction({
        id: "fund-march-parity",
        accountId: cashAccount.id,
        accountEntityId: cashAccount.entityId,
        economicEntityId: cashAccount.entityId,
        transactionDate: "2026-03-31",
        postedDate: "2026-03-31",
        amountOriginal: "-20000.00",
        currencyOriginal: "USD",
        amountBaseEur: "-19415.00",
        fxRateToEur: "0.97075000",
        descriptionRaw: "To Inversión riesgo bajo",
        descriptionClean: "TO INVERSION RIESGO BAJO",
        transactionClass: "transfer_internal",
      }),
    ],
    manualInvestments: [
      {
        id: "manual-bond-fund-parity",
        userId: cashAccount.userId,
        entityId: cashAccount.entityId,
        fundingAccountId: cashAccount.id,
        label: "Inversión en bonos",
        matcherText:
          "Inversión riesgo bajo, SP:b9611010-6e70-49bd-b353-0f959610715d",
        note: null,
        createdAt: "2026-04-11T08:00:00Z",
        updatedAt: "2026-04-11T08:00:00Z",
      },
    ],
    manualInvestmentValuations: [
      {
        id: "manual-bond-fund-valuation-parity",
        userId: cashAccount.userId,
        manualInvestmentId: "manual-bond-fund-parity",
        snapshotDate: "2026-04-11",
        currentValueOriginal: "22765.66",
        currentValueCurrency: "USD",
        note: "Manual mark-to-market",
        createdAt: "2026-04-11T09:00:00Z",
        updatedAt: "2026-04-11T09:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "EUR",
        quoteCurrency: "USD",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T16:00:00Z",
        rate: "1.08695700",
        sourceName: "twelve_data",
        rawJson: {},
      },
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-11",
        asOfTimestamp: "2026-04-11T16:00:00Z",
        rate: "0.85288000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const eurModel = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    referenceDate: "2026-04-11",
  });
  const usdModel = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "USD",
    referenceDate: "2026-04-11",
  });
  const eurFund = buildHoldingDisplayMetricsMap(
    dataset,
    eurModel.holdings.holdings,
    "EUR",
    "2026-04-11",
  ).values().next().value;
  const usdFund = buildHoldingDisplayMetricsMap(
    dataset,
    usdModel.holdings.holdings,
    "USD",
    "2026-04-11",
  ).values().next().value;

  assert.equal(eurFund?.unrealizedDisplay, "-2425.50");
  assert.equal(eurFund?.unrealizedDisplayPercent, "-11.10");
  assert.equal(usdFund?.unrealizedDisplayPercent, "-11.10");
  assert.equal(Number(usdFund?.unrealizedDisplay ?? "0") < 0, true);
});

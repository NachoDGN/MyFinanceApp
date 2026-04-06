import assert from "node:assert/strict";
import test from "node:test";

import { buildInvestmentsReadModel } from "../packages/analytics/src/index.ts";
import { resolvePeriodSelection } from "../packages/domain/src/index.ts";

import { convertBaseEurToDisplayAmount } from "../apps/web/lib/currency.ts";
import { buildHoldingDisplayMetricsMap } from "../apps/web/lib/investment-display.ts";
import {
  createAccount,
  createDataset,
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

test("holding display metrics replay open trade cost basis in the selected currency", () => {
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

  assert.equal(samsung?.avgCostDisplay, "2888.59");
  assert.equal(samsung?.currentValueDisplay, "6044.00");
  assert.equal(samsung?.unrealizedDisplay, "266.83");
  assert.equal(samsung?.unrealizedDisplayPercent, "4.62");
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
  assert.equal(amd?.unrealizedDisplayPercent, "120.48");
});

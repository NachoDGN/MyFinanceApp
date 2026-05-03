import assert from "node:assert/strict";
import test from "node:test";

import {
  selectOwnedFundNavRefreshSecurities,
  selectOwnedStockPriceRefreshSecurities,
  selectTrackedEurFxPairs,
} from "../packages/db/src/index.ts";

import {
  createAccount,
  createDataset,
  createTransaction,
} from "./support/create-dataset";

test("owned stock price refresh selection includes only open stock and ETF positions", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    displayName: "Brokerage",
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
      {
        id: "security-googl",
        providerName: "twelve_data",
        providerSymbol: "GOOGL",
        canonicalSymbol: "GOOGL",
        displaySymbol: "GOOGL",
        name: "Alphabet Inc Class A",
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
      {
        id: "security-vwce",
        providerName: "twelve_data",
        providerSymbol: "VWCE",
        canonicalSymbol: "VWCE",
        displaySymbol: "VWCE",
        name: "Vanguard FTSE All-World UCITS ETF",
        exchangeName: "XETRA",
        micCode: "XETR",
        assetType: "etf",
        quoteCurrency: "EUR",
        country: "DE",
        isin: "IE00BK5BQT80",
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-01T08:00:00Z",
      },
      {
        id: "security-fund",
        providerName: "manual_fund_nav",
        providerSymbol: "IE0032126645",
        canonicalSymbol: "IE0032126645",
        displaySymbol: "VUSAFUND",
        name: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
        exchangeName: "Manual NAV",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: "IE",
        isin: "IE0032126645",
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-01T08:00:00Z",
      },
    ],
    transactions: [
      createTransaction({
        id: "buy-amd",
        accountId: investmentAccount.id,
        transactionClass: "investment_trade_buy",
        securityId: "security-amd",
        quantity: "3",
        amountOriginal: "-300.00",
        amountBaseEur: "-300.00",
      }),
      createTransaction({
        id: "buy-googl",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        transactionClass: "investment_trade_buy",
        securityId: "security-googl",
        quantity: "2",
        amountOriginal: "-240.00",
        amountBaseEur: "-240.00",
      }),
      createTransaction({
        id: "sell-googl",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        transactionClass: "investment_trade_sell",
        securityId: "security-googl",
        quantity: "2",
        amountOriginal: "260.00",
        amountBaseEur: "260.00",
      }),
      createTransaction({
        id: "buy-vwce",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-04",
        postedDate: "2026-04-04",
        transactionClass: "investment_trade_buy",
        securityId: "security-vwce",
        quantity: "5",
        amountOriginal: "-500.00",
        amountBaseEur: "-500.00",
      }),
      createTransaction({
        id: "buy-fund",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-05",
        postedDate: "2026-04-05",
        transactionClass: "investment_trade_buy",
        securityId: "security-fund",
        quantity: "4",
        amountOriginal: "-400.00",
        amountBaseEur: "-400.00",
      }),
    ],
  });

  const selected = selectOwnedStockPriceRefreshSecurities(
    dataset,
    "2026-04-10",
  );

  assert.deepEqual(selected.map((security) => security.displaySymbol).sort(), [
    "AMD",
    "VWCE",
  ]);
});

test("owned stock price refresh selection includes crypto securities backed by cash balances", () => {
  const btcAccount = createAccount({
    id: "btc-company-account",
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: "BTC",
  });
  const dataset = createDataset({
    accounts: [btcAccount],
    accountBalanceSnapshots: [
      {
        accountId: btcAccount.id,
        asOfDate: "2026-04-10",
        balanceOriginal: "0.01500000",
        balanceCurrency: "BTC",
        balanceBaseEur: "0.01500000",
        sourceKind: "statement",
        importBatchId: null,
      },
    ],
    securities: [
      {
        id: "security-btc",
        providerName: "twelve_data",
        providerSymbol: "BTC/EUR",
        canonicalSymbol: "BTC",
        displaySymbol: "BTC",
        name: "Bitcoin",
        exchangeName: "Coinbase Pro",
        micCode: null,
        assetType: "crypto",
        quoteCurrency: "EUR",
        country: null,
        isin: null,
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "crypto",
          baseCurrency: "BTC",
          quoteCurrency: "EUR",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-04-01T08:00:00Z",
      },
    ],
  });

  const selected = selectOwnedStockPriceRefreshSecurities(
    dataset,
    "2026-04-10",
  );

  assert.deepEqual(
    selected.map((security) => security.displaySymbol),
    ["BTC"],
  );
});

test("owned fund NAV refresh selection includes only open fund positions with ISINs", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    displayName: "Brokerage",
  });

  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-fund",
        providerName: "manual_fund_nav",
        providerSymbol: "IE0032126645",
        canonicalSymbol: "IE0032126645",
        displaySymbol: "VUSAFUND",
        name: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
        exchangeName: "Manual NAV",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: "IE",
        isin: "IE0032126645",
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "mutual_fund",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-04-01T08:00:00Z",
      },
      {
        id: "security-fund-no-isin",
        providerName: "manual_fund_nav",
        providerSymbol: "NOISIN",
        canonicalSymbol: "NOISIN",
        displaySymbol: "NOISIN",
        name: "Missing ISIN Fund",
        exchangeName: "Manual NAV",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: "IE",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "mutual_fund",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-04-01T08:00:00Z",
      },
      {
        id: "security-amd",
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
    transactions: [
      createTransaction({
        id: "buy-fund",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-05",
        postedDate: "2026-04-05",
        transactionClass: "investment_trade_buy",
        securityId: "security-fund",
        quantity: "4",
        amountOriginal: "-400.00",
        amountBaseEur: "-400.00",
      }),
      createTransaction({
        id: "buy-fund-no-isin",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-05",
        postedDate: "2026-04-05",
        transactionClass: "investment_trade_buy",
        securityId: "security-fund-no-isin",
        quantity: "2",
        amountOriginal: "-200.00",
        amountBaseEur: "-200.00",
      }),
      createTransaction({
        id: "buy-amd",
        accountId: investmentAccount.id,
        transactionDate: "2026-04-05",
        postedDate: "2026-04-05",
        transactionClass: "investment_trade_buy",
        securityId: "security-amd",
        quantity: "2",
        amountOriginal: "-200.00",
        amountBaseEur: "-200.00",
      }),
    ],
  });

  const selected = selectOwnedFundNavRefreshSecurities(dataset, "2026-04-10");

  assert.deepEqual(
    selected.map((security) => security.displaySymbol),
    ["VUSAFUND"],
  );
});

test("tracked EUR FX pairs include cash-account and held-security currencies but exclude crypto", () => {
  const eurAccount = createAccount({
    id: "eur-company-account",
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: "EUR",
  });
  const usdAccount = createAccount({
    id: "usd-company-account",
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: "USD",
  });
  const btcAccount = createAccount({
    id: "btc-company-account",
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: "BTC",
  });
  const investmentAccount = createAccount({
    id: "brokerage-fx-account",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "EUR",
  });
  const dataset = createDataset({
    accounts: [eurAccount, usdAccount, btcAccount, investmentAccount],
    accountBalanceSnapshots: [
      {
        accountId: usdAccount.id,
        asOfDate: "2026-04-10",
        balanceOriginal: "1200.00",
        balanceCurrency: "USD",
        balanceBaseEur: "1104.00",
        sourceKind: "statement",
        importBatchId: null,
      },
      {
        accountId: btcAccount.id,
        asOfDate: "2026-04-10",
        balanceOriginal: "0.01500000",
        balanceCurrency: "BTC",
        balanceBaseEur: "0.01500000",
        sourceKind: "statement",
        importBatchId: null,
      },
    ],
    securities: [
      {
        id: "security-amd",
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
    transactions: [
      createTransaction({
        id: "buy-amd",
        accountId: investmentAccount.id,
        transactionClass: "investment_trade_buy",
        securityId: "security-amd",
        quantity: "3",
        amountOriginal: "-300.00",
        amountBaseEur: "-300.00",
      }),
    ],
  });

  assert.deepEqual(selectTrackedEurFxPairs(dataset, "2026-04-10"), ["USD"]);
});

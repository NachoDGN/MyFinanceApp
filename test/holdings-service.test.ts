import assert from "node:assert/strict";
import test from "node:test";

import type { FinanceRepository } from "../packages/domain/src/repository.ts";
import { FinanceDomainService } from "../packages/domain/src/service.ts";
import {
  createDataset,
  createInvestmentAccount,
  createSecurity,
  createSecurityPrice,
  createTransaction,
} from "./support/create-dataset.ts";

test("listHoldings reports stale freshness when stale fund NAVs coexist with fresh stock quotes", async () => {
  const account = createInvestmentAccount({
    id: "brokerage-1",
  });
  const dataset = createDataset({
    accounts: [account],
    securities: [
      createSecurity({
        id: "security-fund",
        providerName: "manual_fund_nav",
        providerSymbol: "IE0032126645",
        canonicalSymbol: "IE0032126645",
        displaySymbol: "VANUIEI",
        name: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
        assetType: "other",
        quoteCurrency: "EUR",
        country: "IE",
        isin: "IE0032126645",
        metadataJson: {
          instrumentType: "mutual_fund",
        },
      }),
      createSecurity({
        id: "security-stock",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
      }),
    ],
    securityPrices: [
      createSecurityPrice({
        securityId: "security-fund",
        priceDate: "2026-04-02",
        quoteTimestamp: "2026-04-02T16:00:00Z",
        price: "69.39",
        currency: "EUR",
        sourceName: "ft_markets_nav",
        isRealtime: false,
        isDelayed: true,
        marketState: "reference_nav",
        rawJson: {
          priceType: "nav",
        },
      }),
      createSecurityPrice({
        securityId: "security-stock",
        priceDate: "2026-04-17",
        quoteTimestamp: "2026-04-17T16:47:00Z",
        price: "279.235",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: true,
        isDelayed: false,
        marketState: "open",
        rawJson: {
          symbol: "AMD",
        },
      }),
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-17",
        asOfTimestamp: "2026-04-17T16:47:00Z",
        rate: "0.84750000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    transactions: [
      createTransaction({
        id: "buy-fund",
        accountId: account.id,
        transactionDate: "2026-03-01",
        postedDate: "2026-03-01",
        transactionClass: "investment_trade_buy",
        securityId: "security-fund",
        quantity: "2",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
      }),
      createTransaction({
        id: "buy-stock",
        accountId: account.id,
        transactionDate: "2026-04-01",
        postedDate: "2026-04-01",
        transactionClass: "investment_trade_buy",
        securityId: "security-stock",
        quantity: "2",
        amountOriginal: "-300.00",
        amountBaseEur: "-300.00",
      }),
    ],
  });

  const service = new FinanceDomainService({
    getDataset: async () => dataset,
  } as FinanceRepository);

  const holdings = await service.listHoldings(
    { kind: "account", accountId: account.id },
    "2026-04-17",
  );

  assert.equal(holdings.quoteFreshness, "stale");
});

import assert from "node:assert/strict";
import test from "node:test";

import { buildDeterministicClassification } from "../packages/classification/src/deterministic-classification.ts";
import { buildImportedTransactions } from "../packages/ingestion/src/index.ts";
import {
  createDataset,
  createInvestmentAccount,
  createSecurity,
} from "./support/create-dataset.ts";

test("buildImportedTransactions resolves securities from security_isin and derives unit prices", () => {
  const account = createInvestmentAccount({
    id: "brokerage-1",
    accountType: "brokerage_cash",
  });
  const security = createSecurity({
    id: "security-us500",
    providerSymbol: "VANUIEI",
    canonicalSymbol: "VANUIEI",
    displaySymbol: "VANUIEI",
    name: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
    exchangeName: "VANGUARD",
    micCode: null,
    assetType: "other",
    quoteCurrency: "EUR",
    country: "IE",
    isin: "IE0032126645",
  });
  const dataset = createDataset({
    accounts: [account],
    securities: [security],
  });

  const result = buildImportedTransactions(
    dataset,
    {
      accountId: account.id,
      templateId: "template-1",
      originalFilename: "fund-orders.csv",
      filePath: null,
    },
    "batch-1",
    [
      {
        transaction_date: "2026-04-04",
        description_raw: "Finalizada IE0032126645",
        amount_original_signed: "-100",
        currency_original: "EUR",
        transaction_type_raw: "Finalizada",
        security_isin: "IE0032126645",
        quantity: "1.44",
      },
    ],
  );
  const transaction = result.inserted[0];
  const imported = (
    transaction?.rawPayload as
      | { _import?: { security_isin?: string | null } }
      | undefined
  )?._import;

  assert.equal(transaction?.securityId, security.id);
  assert.equal(transaction?.unitPriceOriginal, "69.44444444");
  assert.equal(imported?.security_isin, "IE0032126645");

  const deterministic = buildDeterministicClassification(
    dataset,
    account,
    transaction!,
  );
  assert.equal(deterministic.transactionClass, "investment_trade_buy");
  assert.equal(deterministic.quantity, "1.44000000");
});

test("buildImportedTransactions keeps resolving legacy external_reference ISIN mappings", () => {
  const account = createInvestmentAccount({
    id: "brokerage-1",
    accountType: "brokerage_cash",
  });
  const security = createSecurity({
    id: "security-eurozone",
    providerSymbol: "VANEIEX",
    canonicalSymbol: "VANEIEX",
    displaySymbol: "VANEIEX",
    name: "Vanguard Eurozone Stock Index Fund EUR Acc",
    exchangeName: "VANGUARD",
    micCode: null,
    assetType: "other",
    quoteCurrency: "EUR",
    country: "IE",
    isin: "IE0008248803",
  });
  const dataset = createDataset({
    accounts: [account],
    securities: [security],
  });

  const result = buildImportedTransactions(
    dataset,
    {
      accountId: account.id,
      templateId: "template-1",
      originalFilename: "legacy-fund-orders.csv",
      filePath: null,
    },
    "batch-1",
    [
      {
        transaction_date: "2026-04-04",
        description_raw: "Finalizada IE0008248803",
        amount_original_signed: "-250",
        currency_original: "EUR",
        external_reference: "IE0008248803",
        quantity: "5",
      },
    ],
  );
  const transaction = result.inserted[0];
  const imported = (
    transaction?.rawPayload as
      | { _import?: { security_isin?: string | null } }
      | undefined
  )?._import;

  assert.equal(transaction?.securityId, security.id);
  assert.equal(imported?.security_isin, "IE0008248803");
});

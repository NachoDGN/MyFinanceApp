import assert from "node:assert/strict";
import test from "node:test";

import {
  buildFundOrderHistoryImportPlan,
  parseMyInvestorFundOrderHistoryText,
  reconcileFundOrderHistoryImportPlan,
  type Security,
} from "../packages/domain/src/index.ts";
import { createDataset, createTransaction } from "./support/create-dataset.ts";

const us500Security: Security = {
  id: "security-us500",
  providerName: "manual_fund_nav",
  providerSymbol: "IE0032126645",
  canonicalSymbol: "VANUIEI",
  displaySymbol: "VANUIEI",
  name: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
  exchangeName: "VANGUARD",
  micCode: null,
  assetType: "other",
  quoteCurrency: "EUR",
  country: "IE",
  isin: "IE0032126645",
  figi: null,
  active: true,
  metadataJson: {},
  lastPriceRefreshAt: null,
  createdAt: "2026-01-01T00:00:00Z",
};

test("parseMyInvestorFundOrderHistoryText parses finalized and rejected rows", () => {
  const rows = parseMyInvestorFundOrderHistoryText(`
01/07/2024

Suscripción Fondos de Inversión
300,00 €
Vanguard U.S. 500 Stock Index Fund EUR Acc
Puntual
Finalizada
4,87 participaciones

01/04/2026

Suscripción Fondos de Inversión
200,00 €
Vanguard Emerging Markets Stock Index Fund EUR Acc
Puntual
Rechazada
-
`);

  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], {
    orderDate: "2024-07-01",
    orderKind: "Suscripción Fondos de Inversión",
    amountEur: "300.00000000",
    fundName: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
    cadence: "Puntual",
    status: "Finalizada",
    quantity: "4.87000000",
  });
  assert.deepEqual(rows[1], {
    orderDate: "2026-04-01",
    orderKind: "Suscripción Fondos de Inversión",
    amountEur: "200.00000000",
    fundName: "Vanguard Emerging Markets Stock Index Fund EUR Acc",
    cadence: "Puntual",
    status: "Rechazada",
    quantity: null,
  });
});

test("buildFundOrderHistoryImportPlan patches matched buys and imports older missing rows", () => {
  const dataset = createDataset({
    securities: [us500Security],
    transactions: [
      createTransaction({
        id: "tx-us500-2024-07",
        accountId: "investment-account-1",
        transactionDate: "2024-07-03",
        postedDate: "2024-07-08",
        amountOriginal: "-299.00",
        amountBaseEur: "-299.00",
        descriptionRaw: "VANGUARD US 500 STOCK EUR INS",
        transactionClass: "investment_trade_buy",
        securityId: us500Security.id,
        quantity: "4.00000000",
        unitPriceOriginal: "74.75000000",
      }),
    ],
  });

  const rows = parseMyInvestorFundOrderHistoryText(`
01/07/2024

Suscripción Fondos de Inversión
300,00 €
Vanguard U.S. 500 Stock Index Fund EUR Acc
Puntual
Finalizada
4,87 participaciones

19/10/2023

Suscripción por Traspaso Externo
3.822,41 €
Vanguard U.S. 500 Stock Index Fund EUR Acc
Puntual
Finalizada
79,19 participaciones

01/04/2026

Suscripción Fondos de Inversión
200,00 €
Vanguard Emerging Markets Stock Index Fund EUR Acc
Puntual
Rechazada
-
`);

  const plan = buildFundOrderHistoryImportPlan(
    dataset,
    "investment-account-1",
    rows,
  );

  assert.equal(plan.parsedRows.length, 3);
  assert.equal(plan.finalizedRows.length, 2);
  assert.equal(plan.rejectedRows.length, 1);
  assert.equal(plan.unresolvedRows.length, 0);
  assert.equal(plan.matchedTransactionPatches.length, 1);
  assert.equal(plan.openingPositions.length, 1);

  assert.deepEqual(plan.matchedTransactionPatches[0], {
    transactionId: "tx-us500-2024-07",
    securityId: "security-us500",
    fundName: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
    orderDate: "2024-07-01",
    transactionDate: "2024-07-03",
    postedDate: "2024-07-08",
    quantity: "4.87000000",
    unitPriceOriginal: "61.39630390",
    actualAmountEur: "299.00000000",
    orderAmountEur: "300.00000000",
    amountDiffEur: "1.00000000",
    dayDistance: 2,
  });

  assert.deepEqual(plan.openingPositions[0], {
    securityId: "security-us500",
    fundName: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
    orderDate: "2023-10-19",
    orderKind: "Suscripción por Traspaso Externo",
    quantity: "79.19000000",
    costBasisEur: "3822.41000000",
  });
});

test("reconcileFundOrderHistoryImportPlan deletes stale fallback adjustments and skips existing openings", () => {
  const dataset = createDataset({
    securities: [us500Security],
    transactions: [
      createTransaction({
        id: "tx-us500-2024-07",
        accountId: "investment-account-1",
        transactionDate: "2024-07-03",
        postedDate: "2024-07-08",
        amountOriginal: "-299.76",
        amountBaseEur: "-299.76",
        descriptionRaw: "VANGUARD US 500 STOCK EUR INS",
        transactionClass: "investment_trade_buy",
        securityId: us500Security.id,
        quantity: "4.00000000",
        unitPriceOriginal: "74.94000000",
      }),
    ],
    holdingAdjustments: [
      {
        id: "adj-stale-us500-2024-07",
        userId: "user-1",
        entityId: "entity-1",
        accountId: "investment-account-1",
        securityId: us500Security.id,
        effectiveDate: "2024-07-01",
        shareDelta: "4.87000000",
        costBasisDeltaEur: "300.00000000",
        reason: "opening_position",
        note: "Created from app/CLI.",
        createdAt: "2026-01-01T00:00:00Z",
      },
      {
        id: "adj-existing-us500-2023-10",
        userId: "user-1",
        entityId: "entity-1",
        accountId: "investment-account-1",
        securityId: us500Security.id,
        effectiveDate: "2023-10-19",
        shareDelta: "79.19000000",
        costBasisDeltaEur: "3822.41000000",
        reason: "opening_position",
        note: "Created from app/CLI.",
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
  });

  const rows = parseMyInvestorFundOrderHistoryText(`
01/07/2024

Suscripción Fondos de Inversión
300,00 €
Vanguard U.S. 500 Stock Index Fund EUR Acc
Puntual
Finalizada
4,87 participaciones

19/10/2023

Suscripción por Traspaso Externo
3.822,41 €
Vanguard U.S. 500 Stock Index Fund EUR Acc
Puntual
Finalizada
79,19 participaciones
`);

  const importPlan = buildFundOrderHistoryImportPlan(
    dataset,
    "investment-account-1",
    rows,
  );
  const reconciliation = reconcileFundOrderHistoryImportPlan(
    dataset,
    "investment-account-1",
    importPlan,
  );

  assert.deepEqual(reconciliation.staleOpeningAdjustments, [
    {
      adjustmentId: "adj-stale-us500-2024-07",
      securityId: "security-us500",
      fundName: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
      orderDate: "2024-07-01",
      quantity: "4.87000000",
      costBasisEur: "300.00000000",
    },
  ]);
  assert.deepEqual(reconciliation.existingOpeningPositions, [
    {
      adjustmentId: "adj-existing-us500-2023-10",
      securityId: "security-us500",
      fundName: "Vanguard U.S. 500 Stock Index Fund EUR Acc",
      orderDate: "2023-10-19",
      quantity: "79.19000000",
      costBasisEur: "3822.41000000",
    },
  ]);
  assert.equal(reconciliation.openingPositionsToCreate.length, 0);
});

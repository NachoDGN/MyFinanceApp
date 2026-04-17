import assert from "node:assert/strict";
import test from "node:test";

import { fetchFtFundPriceAtOrBeforeDate } from "../packages/db/src/ft-fund-history.ts";
import {
  buildFundNavBackfillRequest,
  mergeFundNavBackfillRequests,
} from "../packages/db/src/fund-nav-backfill.ts";
import {
  readRequestUrl,
  withRuntimeOverrides,
} from "./support/runtime-overrides";

test("fund NAV backfill only requests the missing suffix when coverage is stale", () => {
  const request = buildFundNavBackfillRequest({
    security: {
      id: "security-fund-1",
      isin: "IE0031786696",
      quoteCurrency: "EUR",
      metadataJson: {
        ftMarketsHistory: {
          source: "ft_markets",
          publicSymbol: "IE0031786696:EUR",
          internalSymbol: "72731963",
          pageUrl:
            "https://markets.ft.com/data/funds/tearsheet/historical?s=IE0031786696:EUR",
          coveredFrom: "2023-01-01",
          coveredTo: "2025-12-31",
          lastBackfillAt: "2025-12-31T18:00:00Z",
          status: "ready",
          lastError: null,
        },
      },
    },
    transactionDate: "2023-01-23",
    triggerTransactionId: "txn-1",
    today: "2026-04-17",
  });

  assert.deepEqual(request, {
    securityId: "security-fund-1",
    isin: "IE0031786696",
    quoteCurrency: "EUR",
    startDate: "2026-01-01",
    endDate: "2026-04-17",
    triggerTransactionId: "txn-1",
  });
});

test("fund NAV backfill request merging dedupes repeated security coverage work", () => {
  const merged = mergeFundNavBackfillRequests([
    {
      securityId: "security-fund-1",
      isin: "IE0031786696",
      quoteCurrency: "EUR",
      startDate: "2024-01-01",
      endDate: "2026-04-17",
      triggerTransactionId: "txn-1",
    },
    {
      securityId: "security-fund-1",
      isin: "IE0031786696",
      quoteCurrency: "EUR",
      startDate: "2023-01-01",
      endDate: "2025-12-31",
      triggerTransactionId: "txn-2",
    },
  ]);

  assert.deepEqual(merged, [
    {
      securityId: "security-fund-1",
      isin: "IE0031786696",
      quoteCurrency: "EUR",
      startDate: "2023-01-01",
      endDate: "2026-04-17",
      triggerTransactionId: "txn-1",
    },
  ]);
});

test("FT fallback returns the nearest prior fund NAV for the triggering transaction", async () => {
  await withRuntimeOverrides(
    {
      fetch: async (input) => {
        const url = readRequestUrl(input);
        if (
          url.hostname === "markets.ft.com" &&
          url.pathname.endsWith("/historical")
        ) {
          return new Response(
            `
              <section
                data-f2-app-id="mod-tearsheet-historical-prices"
                data-mod-config="{&quot;symbol&quot;:&quot;72731963&quot;,&quot;inception&quot;:&quot;2014-02-27T00:00:00Z&quot;}"
              ></section>
            `,
            { status: 200 },
          );
        }

        if (
          url.hostname === "markets.ft.com" &&
          url.pathname.includes("/ajax/get-historical-prices")
        ) {
          return new Response(
            JSON.stringify({
              html: `
                <table>
                  <tr>
                    <td><span class="mod-ui-hide-small-below">Friday, January 20, 2023</span></td>
                    <td>184.12</td>
                    <td>184.12</td>
                    <td>184.12</td>
                    <td>184.12</td>
                    <td>0</td>
                  </tr>
                </table>
              `,
            }),
            {
              status: 200,
              headers: { "Content-Type": "application/json" },
            },
          );
        }

        throw new Error(`Unexpected fetch URL: ${url.toString()}`);
      },
    },
    async () => {
      const result = await fetchFtFundPriceAtOrBeforeDate({
        securityId: "security-fund-1",
        isin: "IE0031786696",
        quoteCurrency: "EUR",
        transactionDate: "2023-01-22",
        maxDriftDays: 5,
      });

      assert.equal(result.security?.publicSymbol, "IE0031786696:EUR");
      assert.equal(result.security?.internalSymbol, "72731963");
      assert.equal(result.price?.priceDate, "2023-01-20");
      assert.equal(result.price?.price, "184.12");
      assert.equal(result.price?.sourceName, "ft_markets_nav");
      assert.equal(result.price?.marketState, "reference_nav");
      assert.deepEqual(result.price?.rawJson, {
        importSource: "ft_markets",
        priceType: "nav",
        pageUrl:
          "https://markets.ft.com/data/funds/tearsheet/historical?s=IE0031786696%3AEUR",
        publicSymbol: "IE0031786696:EUR",
        internalSymbol: "72731963",
        requestedStartDate: "2023-01-17",
        requestedEndDate: "2023-01-22",
      });
    },
  );
});

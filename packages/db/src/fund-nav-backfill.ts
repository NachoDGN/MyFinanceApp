import type { Security, SecurityPrice } from "@myfinance/domain";

import {
  mapFromSql,
  readOptionalRecord,
  readOptionalString,
  serializeJson,
} from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { queueJob, supportsJobType } from "./job-state";
import {
  buildFtFundPublicSymbol,
  fetchFtFundHistory,
  type FtFundSecurityInfo,
} from "./ft-fund-history";

export interface FundNavBackfillRequest {
  securityId: string;
  isin: string;
  quoteCurrency: string;
  startDate: string;
  endDate: string;
  triggerTransactionId: string | null;
}

type FtMarketsCoverage = {
  source: "ft_markets";
  publicSymbol: string | null;
  internalSymbol: string | null;
  pageUrl: string | null;
  coveredFrom: string | null;
  coveredTo: string | null;
  lastBackfillAt: string | null;
  status: "queued" | "ready" | "failed";
  lastError: string | null;
};

function todayIso(now = new Date()) {
  return now.toISOString().slice(0, 10);
}

function shiftIsoDate(value: string, days: number) {
  const next = new Date(`${value}T00:00:00Z`);
  next.setUTCDate(next.getUTCDate() + days);
  return next.toISOString().slice(0, 10);
}

function startOfYearIso(value: string) {
  return `${value.slice(0, 4)}-01-01`;
}

function compareIsoDates(left: string, right: string) {
  return left.localeCompare(right);
}

function minIsoDate(left: string | null, right: string | null) {
  if (!left) return right;
  if (!right) return left;
  return compareIsoDates(left, right) <= 0 ? left : right;
}

function maxIsoDate(left: string | null, right: string | null) {
  if (!left) return right;
  if (!right) return left;
  return compareIsoDates(left, right) >= 0 ? left : right;
}

function readFtMarketsCoverage(metadataJson: Record<string, unknown>) {
  const coverage = readOptionalRecord(
    readOptionalRecord(metadataJson.ftMarketsHistory),
  );
  return {
    source: "ft_markets" as const,
    publicSymbol: readOptionalString(coverage?.publicSymbol),
    internalSymbol: readOptionalString(coverage?.internalSymbol),
    pageUrl: readOptionalString(coverage?.pageUrl),
    coveredFrom: readOptionalString(coverage?.coveredFrom),
    coveredTo: readOptionalString(coverage?.coveredTo),
    lastBackfillAt: readOptionalString(coverage?.lastBackfillAt),
    status:
      coverage?.status === "queued" ||
      coverage?.status === "failed" ||
      coverage?.status === "ready"
        ? coverage.status
        : ("ready" as const),
    lastError: readOptionalString(coverage?.lastError),
  } satisfies FtMarketsCoverage;
}

function hasCoverageForRange(
  metadataJson: Record<string, unknown>,
  startDate: string,
  endDate: string,
) {
  const coverage = readFtMarketsCoverage(metadataJson);
  return Boolean(
    coverage.coveredFrom &&
    coverage.coveredTo &&
    coverage.status === "ready" &&
    coverage.coveredFrom <= startDate &&
    coverage.coveredTo >= endDate,
  );
}

function buildMissingCoverageRange(
  metadataJson: Record<string, unknown>,
  startDate: string,
  endDate: string,
) {
  if (hasCoverageForRange(metadataJson, startDate, endDate)) {
    return null;
  }

  const coverage = readFtMarketsCoverage(metadataJson);
  if (
    coverage.status === "ready" &&
    coverage.coveredFrom &&
    coverage.coveredTo &&
    coverage.coveredFrom <= startDate &&
    coverage.coveredTo < endDate
  ) {
    return {
      startDate: shiftIsoDate(coverage.coveredTo, 1),
      endDate,
    };
  }

  if (
    coverage.status === "ready" &&
    coverage.coveredFrom &&
    coverage.coveredTo &&
    coverage.coveredFrom > startDate &&
    coverage.coveredTo >= endDate
  ) {
    return {
      startDate,
      endDate: shiftIsoDate(coverage.coveredFrom, -1),
    };
  }

  return { startDate, endDate };
}

function mergeCoverage(input: {
  current: FtMarketsCoverage;
  security: FtFundSecurityInfo | null;
  startDate: string;
  endDate: string;
  status: FtMarketsCoverage["status"];
  lastError?: string | null;
}) {
  return {
    source: "ft_markets",
    publicSymbol: input.security?.publicSymbol ?? input.current.publicSymbol,
    internalSymbol:
      input.security?.internalSymbol ?? input.current.internalSymbol,
    pageUrl: input.security?.pageUrl ?? input.current.pageUrl,
    coveredFrom: minIsoDate(input.current.coveredFrom, input.startDate),
    coveredTo:
      input.status === "ready"
        ? maxIsoDate(input.current.coveredTo, input.endDate)
        : input.current.coveredTo,
    lastBackfillAt: new Date().toISOString(),
    status: input.status,
    lastError: input.lastError ?? null,
  } satisfies FtMarketsCoverage;
}

async function readSecurityById(sql: SqlClient, securityId: string) {
  const rows = await sql`
    select *
    from public.securities
    where id = ${securityId}
    limit 1
  `;
  return rows[0] ? mapFromSql<Security>(rows[0]) : null;
}

async function upsertSecurityPrices(sql: SqlClient, prices: SecurityPrice[]) {
  for (const price of prices) {
    await sql`
      insert into public.security_prices ${sql({
        security_id: price.securityId,
        price_date: price.priceDate,
        quote_timestamp: price.quoteTimestamp,
        price: price.price,
        currency: price.currency,
        source_name: price.sourceName,
        is_realtime: price.isRealtime,
        is_delayed: price.isDelayed,
        market_state: price.marketState,
        raw_json: serializeJson(sql, price.rawJson),
        created_at: price.createdAt,
      } as Record<string, unknown>)}
      on conflict (security_id, price_date, source_name)
      do update set
        quote_timestamp = excluded.quote_timestamp,
        price = excluded.price,
        currency = excluded.currency,
        is_realtime = excluded.is_realtime,
        is_delayed = excluded.is_delayed,
        market_state = excluded.market_state,
        raw_json = excluded.raw_json,
        created_at = excluded.created_at
    `;
  }
}

async function hasPendingFundNavBackfillJob(
  sql: SqlClient,
  request: FundNavBackfillRequest,
) {
  const rows = await sql`
    select exists (
      select 1
      from public.jobs
      where job_type = ${"fund_nav_backfill"}
        and (status = ${"queued"} or status = ${"running"})
        and payload_json->>'securityId' = ${request.securityId}
        and payload_json->>'startDate' <= ${request.startDate}
        and payload_json->>'endDate' >= ${request.endDate}
    ) as pending
  `;
  return rows[0]?.pending === true;
}

async function hasQueuedOrRunningJob(sql: SqlClient, jobType: string) {
  const rows = await sql`
    select exists (
      select 1
      from public.jobs
      where job_type = ${jobType}
        and (status = ${"queued"} or status = ${"running"})
    ) as present
  `;
  return rows[0]?.present === true;
}

export function buildFundNavBackfillRequest(input: {
  security: Pick<Security, "id" | "isin" | "quoteCurrency" | "metadataJson">;
  transactionDate: string;
  triggerTransactionId: string;
  today?: string;
  isin?: string | null;
}) {
  const normalizedIsin =
    input.isin?.trim().toUpperCase() ??
    input.security.isin?.trim().toUpperCase() ??
    null;
  const normalizedCurrency = input.security.quoteCurrency.trim().toUpperCase();
  if (!normalizedIsin || !normalizedCurrency) {
    return null;
  }
  const endDate = input.today ?? todayIso();
  const startDate = startOfYearIso(input.transactionDate);
  const missingRange = buildMissingCoverageRange(
    input.security.metadataJson,
    startDate,
    endDate,
  );
  if (!missingRange) {
    return null;
  }
  return {
    securityId: input.security.id,
    isin: normalizedIsin,
    quoteCurrency: normalizedCurrency,
    startDate: missingRange.startDate,
    endDate: missingRange.endDate,
    triggerTransactionId: input.triggerTransactionId,
  } satisfies FundNavBackfillRequest;
}

export function mergeFundNavBackfillRequests(
  requests: readonly FundNavBackfillRequest[],
) {
  const mergedBySecurity = new Map<string, FundNavBackfillRequest>();
  for (const request of requests) {
    const existing = mergedBySecurity.get(request.securityId);
    if (!existing) {
      mergedBySecurity.set(request.securityId, request);
      continue;
    }
    mergedBySecurity.set(request.securityId, {
      ...existing,
      startDate:
        compareIsoDates(request.startDate, existing.startDate) < 0
          ? request.startDate
          : existing.startDate,
      endDate:
        compareIsoDates(request.endDate, existing.endDate) > 0
          ? request.endDate
          : existing.endDate,
      triggerTransactionId:
        existing.triggerTransactionId ?? request.triggerTransactionId,
    });
  }
  return [...mergedBySecurity.values()];
}

export async function queueFundNavBackfillJobs(
  sql: SqlClient,
  requests: readonly FundNavBackfillRequest[],
) {
  if (!(await supportsJobType(sql, "fund_nav_backfill"))) {
    return [];
  }

  const queuedJobIds: string[] = [];
  for (const request of mergeFundNavBackfillRequests(requests)) {
    const security = await readSecurityById(sql, request.securityId);
    if (!security) {
      continue;
    }
    if (
      hasCoverageForRange(
        security.metadataJson,
        request.startDate,
        request.endDate,
      )
    ) {
      continue;
    }
    if (await hasPendingFundNavBackfillJob(sql, request)) {
      continue;
    }
    const coverage = mergeCoverage({
      current: readFtMarketsCoverage(security.metadataJson),
      security: null,
      startDate: request.startDate,
      endDate: request.endDate,
      status: "queued",
      lastError: null,
    });
    await sql`
      update public.securities
      set metadata_json = ${serializeJson(sql, {
        ...security.metadataJson,
        ftMarketsHistory: coverage,
      })}::jsonb
      where id = ${security.id}
    `;
    queuedJobIds.push(
      await queueJob(sql, "fund_nav_backfill", {
        securityId: request.securityId,
        isin: request.isin,
        quoteCurrency: request.quoteCurrency,
        startDate: request.startDate,
        endDate: request.endDate,
        triggerTransactionId: request.triggerTransactionId,
      }),
    );
  }
  return queuedJobIds;
}

export async function processFundNavBackfillJob(
  sql: SqlClient,
  payloadJson: Record<string, unknown>,
) {
  const securityId = readOptionalString(payloadJson.securityId);
  const isin = readOptionalString(payloadJson.isin);
  const quoteCurrency = readOptionalString(payloadJson.quoteCurrency);
  const startDate = readOptionalString(payloadJson.startDate);
  const endDate = readOptionalString(payloadJson.endDate);
  if (!securityId || !isin || !quoteCurrency || !startDate || !endDate) {
    throw new Error(
      "Fund NAV backfill job is missing required payload fields.",
    );
  }

  const security = await readSecurityById(sql, securityId);
  if (!security) {
    throw new Error(`Security ${securityId} no longer exists.`);
  }

  try {
    const publicSymbol = buildFtFundPublicSymbol(isin, quoteCurrency);
    const history = await fetchFtFundHistory({
      publicSymbol,
      startDate,
      endDate,
    });
    if (history.rows.length === 0) {
      throw new Error(
        `FT returned no historical rows for ${publicSymbol} between ${startDate} and ${endDate}.`,
      );
    }
    const prices = history.rows.map((row) => ({
      securityId,
      priceDate: row.priceDate,
      quoteTimestamp: `${row.priceDate}T16:00:00Z`,
      price: row.close,
      currency: quoteCurrency,
      sourceName: "ft_markets_nav",
      isRealtime: false,
      isDelayed: true,
      marketState: "reference_nav",
      rawJson: {
        importSource: "ft_markets",
        priceType: "nav",
        pageUrl: history.security.pageUrl,
        publicSymbol: history.security.publicSymbol,
        internalSymbol: history.security.internalSymbol,
        requestedStartDate: startDate,
        requestedEndDate: endDate,
      },
      createdAt: new Date().toISOString(),
    })) satisfies SecurityPrice[];

    await upsertSecurityPrices(sql, prices);

    const latestQuoteTimestamp =
      prices.sort((left, right) =>
        right.quoteTimestamp.localeCompare(left.quoteTimestamp),
      )[0]?.quoteTimestamp ?? null;
    const coverage = mergeCoverage({
      current: readFtMarketsCoverage(security.metadataJson),
      security: history.security,
      startDate,
      endDate,
      status: "ready",
      lastError: null,
    });
    await sql`
      update public.securities
      set metadata_json = ${serializeJson(sql, {
        ...security.metadataJson,
        ftMarketsHistory: coverage,
      })}::jsonb,
          last_price_refresh_at = ${latestQuoteTimestamp}
      where id = ${securityId}
    `;

    if (await supportsJobType(sql, "position_rebuild")) {
      if (!(await hasQueuedOrRunningJob(sql, "position_rebuild"))) {
        await queueJob(sql, "position_rebuild", {
          trigger: "fund_nav_backfill",
          securityId,
          startDate,
          endDate,
        });
      }
    }
    if (await supportsJobType(sql, "metric_refresh")) {
      if (!(await hasQueuedOrRunningJob(sql, "metric_refresh"))) {
        await queueJob(sql, "metric_refresh", {
          trigger: "fund_nav_backfill",
          securityId,
          startDate,
          endDate,
        });
      }
    }

    return {
      ...payloadJson,
      publicSymbol: history.security.publicSymbol,
      internalSymbol: history.security.internalSymbol,
      pageUrl: history.security.pageUrl,
      rowsFetched: prices.length,
      latestPriceDate: prices[prices.length - 1]?.priceDate ?? null,
      completedAt: new Date().toISOString(),
    };
  } catch (error) {
    const coverage = mergeCoverage({
      current: readFtMarketsCoverage(security.metadataJson),
      security: null,
      startDate,
      endDate,
      status: "failed",
      lastError: error instanceof Error ? error.message : String(error),
    });
    await sql`
      update public.securities
      set metadata_json = ${serializeJson(sql, {
        ...security.metadataJson,
        ftMarketsHistory: coverage,
      })}::jsonb
      where id = ${securityId}
    `;
    throw error;
  }
}

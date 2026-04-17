import {
  getDatasetLatestDate,
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";

import { loadDatasetForUser } from "./dataset-loader";
import { withInvestmentMutationLock } from "./investment-mutation-lock";
import {
  prepareInvestmentRebuild,
  type InvestmentRebuildArtifacts,
  type InvestmentRebuildProgress,
} from "./investment-rebuild";
import {
  readOptionalRecord,
  serializeJson,
} from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { updateTransactionRecord } from "./transaction-record";
import { queueFundNavBackfillJobs } from "./fund-nav-backfill";

export type ApplyInvestmentRebuildOptions = {
  onProgress?: (progress: InvestmentRebuildProgress) => Promise<void> | void;
  historicalLookupTransactionIds?: readonly string[];
};

type RebuildTransactionPatch = InvestmentRebuildArtifacts["transactionPatches"][number];

function buildTransactionPatchUpdatePayload(
  patch: RebuildTransactionPatch,
): Record<string, unknown> {
  const updatePayload: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
  };

  if (patch.transactionClass !== undefined) {
    updatePayload.transaction_class = patch.transactionClass;
  }
  if (patch.categoryCode !== undefined) {
    updatePayload.category_code = patch.categoryCode;
  }
  if (patch.classificationStatus !== undefined) {
    updatePayload.classification_status = patch.classificationStatus;
  }
  if (patch.classificationSource !== undefined) {
    updatePayload.classification_source = patch.classificationSource;
  }
  if (patch.classificationConfidence !== undefined) {
    updatePayload.classification_confidence = patch.classificationConfidence;
  }
  if (patch.securityId !== undefined) {
    updatePayload.security_id = patch.securityId;
  }
  if (patch.quantity !== undefined) {
    updatePayload.quantity = patch.quantity;
  }
  if (patch.unitPriceOriginal !== undefined) {
    updatePayload.unit_price_original = patch.unitPriceOriginal;
  }
  if (patch.needsReview !== undefined) {
    updatePayload.needs_review = patch.needsReview;
  }
  if (patch.reviewReason !== undefined) {
    updatePayload.review_reason = patch.reviewReason;
  }

  return updatePayload;
}

function buildNextTransactionLlmPayload(
  existingTransaction: Transaction | null,
  patch: RebuildTransactionPatch,
) {
  const existingLlmPayload =
    readOptionalRecord(existingTransaction?.llmPayload) ?? {};

  if (!patch.llmPayload && !patch.rebuildEvidence) {
    return null;
  }

  return {
    ...existingLlmPayload,
    ...(patch.llmPayload ?? {}),
    ...(patch.rebuildEvidence
      ? {
          rebuildEvidence: {
            ...(readOptionalRecord(existingLlmPayload.rebuildEvidence) ?? {}),
            ...patch.rebuildEvidence,
          },
        }
      : {}),
  };
}

async function insertSecurities(
  sql: SqlClient,
  securities: InvestmentRebuildArtifacts["insertedSecurities"],
) {
  for (const security of securities) {
    await sql`
      insert into public.securities ${sql({
        id: security.id,
        provider_name: security.providerName,
        provider_symbol: security.providerSymbol,
        canonical_symbol: security.canonicalSymbol,
        display_symbol: security.displaySymbol,
        name: security.name,
        exchange_name: security.exchangeName,
        mic_code: security.micCode,
        asset_type: security.assetType,
        quote_currency: security.quoteCurrency,
        country: security.country,
        isin: security.isin,
        figi: security.figi,
        active: security.active,
        metadata_json: serializeJson(sql, security.metadataJson),
        last_price_refresh_at: security.lastPriceRefreshAt,
        created_at: security.createdAt,
      } as Record<string, unknown>)}
      on conflict (provider_name, provider_symbol) do nothing
    `;
  }
}

async function insertSecurityAliases(
  sql: SqlClient,
  aliases: InvestmentRebuildArtifacts["insertedAliases"],
) {
  for (const alias of aliases) {
    await sql`
      insert into public.security_aliases ${sql({
        id: alias.id,
        security_id: alias.securityId,
        alias_text_normalized: alias.aliasTextNormalized,
        alias_source: alias.aliasSource,
        template_id: alias.templateId,
        confidence: alias.confidence,
        created_at: alias.createdAt,
      } as Record<string, unknown>)}
      on conflict (security_id, alias_text_normalized) do nothing
    `;
  }
}

async function upsertSecurityPrices(
  sql: SqlClient,
  prices: InvestmentRebuildArtifacts["upsertedPrices"],
) {
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

async function applyTransactionPatches(
  sql: SqlClient,
  input: {
    userId: string;
    latestTransactionsById: Map<string, Transaction>;
    transactionPatches: InvestmentRebuildArtifacts["transactionPatches"];
  },
) {
  for (const patch of input.transactionPatches) {
    const updatePayload = buildTransactionPatchUpdatePayload(patch);
    const nextLlmPayload = buildNextTransactionLlmPayload(
      input.latestTransactionsById.get(patch.id) ?? null,
      patch,
    );

    await updateTransactionRecord(sql, {
      userId: input.userId,
      transactionId: patch.id,
      updatePayload,
      llmPayload: nextLlmPayload ?? undefined,
      returning: false,
    });
  }
}

async function replaceInvestmentArtifacts(
  sql: SqlClient,
  input: {
    userId: string;
    positions: DomainDataset["investmentPositions"];
    snapshots: DomainDataset["dailyPortfolioSnapshots"];
  },
) {
  await sql`
    delete from public.daily_portfolio_snapshots
    where user_id = ${input.userId}
  `;
  await sql`
    delete from public.investment_positions
    where user_id = ${input.userId}
  `;

  for (const position of input.positions) {
    await sql`
      insert into public.investment_positions ${sql({
        user_id: position.userId,
        entity_id: position.entityId,
        account_id: position.accountId,
        security_id: position.securityId,
        open_quantity: position.openQuantity,
        open_cost_basis_eur: position.openCostBasisEur,
        avg_cost_eur: position.avgCostEur,
        realized_pnl_eur: position.realizedPnlEur,
        dividends_eur: position.dividendsEur,
        interest_eur: position.interestEur,
        fees_eur: position.feesEur,
        last_trade_date: position.lastTradeDate,
        last_rebuilt_at: position.lastRebuiltAt,
        provenance_json: serializeJson(sql, position.provenanceJson),
        unrealized_complete: position.unrealizedComplete,
      } as Record<string, unknown>)}
    `;
  }

  for (const snapshot of input.snapshots) {
    await sql`
      insert into public.daily_portfolio_snapshots ${sql({
        snapshot_date: snapshot.snapshotDate,
        user_id: snapshot.userId,
        entity_id: snapshot.entityId,
        account_id: snapshot.accountId,
        security_id: snapshot.securityId,
        market_value_eur: snapshot.marketValueEur,
        cost_basis_eur: snapshot.costBasisEur,
        unrealized_pnl_eur: snapshot.unrealizedPnlEur,
        cash_balance_eur: snapshot.cashBalanceEur,
        total_portfolio_value_eur: snapshot.totalPortfolioValueEur,
        generated_at: snapshot.generatedAt,
      } as Record<string, unknown>)}
    `;
  }
}

export async function applyInvestmentRebuild(
  sql: SqlClient,
  userId: string,
  options?: ApplyInvestmentRebuildOptions,
) {
  return withInvestmentMutationLock(sql, userId, async () => {
    const latestDataset = await loadDatasetForUser(sql, userId);
    const referenceDate = getDatasetLatestDate(latestDataset);
    const rebuilt = await prepareInvestmentRebuild(latestDataset, referenceDate, {
      onProgress: options?.onProgress,
      historicalLookupTransactionIds: options?.historicalLookupTransactionIds,
    });
    const latestTransactionsById = new Map(
      latestDataset.transactions.map((transaction) => [
        transaction.id,
        transaction,
      ]),
    );

    await insertSecurities(sql, rebuilt.insertedSecurities);
    await insertSecurityAliases(sql, rebuilt.insertedAliases);
    await upsertSecurityPrices(sql, rebuilt.upsertedPrices);
    await queueFundNavBackfillJobs(sql, rebuilt.fundNavBackfillRequests);
    await applyTransactionPatches(sql, {
      userId,
      latestTransactionsById,
      transactionPatches: rebuilt.transactionPatches,
    });
    await replaceInvestmentArtifacts(sql, {
      userId,
      positions: rebuilt.positions,
      snapshots: rebuilt.snapshots,
    });

    return {
      referenceDate,
      rebuiltPositions: rebuilt.positions.length,
      rebuiltSnapshots: rebuilt.snapshots.length,
      updatedTransactions: rebuilt.transactionPatches.length,
      insertedSecurities: rebuilt.insertedSecurities.length,
      upsertedPrices: rebuilt.upsertedPrices.length,
    };
  });
}

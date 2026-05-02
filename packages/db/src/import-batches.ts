import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import {
  type DomainDataset,
  type ImportCommitResult,
  type ImportPreviewResult,
  type Security,
  type Transaction,
} from "@myfinance/domain";
import {
  buildImportedTransactions,
  normalizeImportExecutionInput,
  type PortfolioStatementPosition,
  type PortfolioStatementSnapshot,
  runDeterministicImport,
  sanitizeImportResult,
} from "@myfinance/ingestion";

import { queueJob, supportsJobType } from "./job-state";
import { serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";

const DEFAULT_IMPORT_JOBS_QUEUED = [
  "classification",
  "transfer_rematch",
  "position_rebuild",
  "metric_refresh",
] as const satisfies ImportCommitResult["jobsQueued"];

const IBKR_STATEMENT_PRICE_SOURCE = "ibkr_statement";

type PortfolioStatementSnapshotCommitSummary = {
  statementDate: string | null;
  accountNumber: string | null;
  cashSnapshotUpserted: boolean;
  cashSnapshotMethod: string | null;
  cashSnapshotIncludesDividendAccruals: boolean;
  openPositionsDetected: number;
  securitiesUpserted: number;
  securityAliasesInserted: number;
  pricesUpserted: number;
  openingPositionsInserted: number;
  openingPositionsSkipped: number;
  skippedReasons: string[];
};

function mergeDefaultImportJobs(
  jobsQueued: readonly ImportCommitResult["jobsQueued"][number][] | undefined,
): ImportCommitResult["jobsQueued"] {
  return [
    ...new Set([
      ...DEFAULT_IMPORT_JOBS_QUEUED,
      ...((jobsQueued ?? []) as ImportCommitResult["jobsQueued"]),
    ]),
  ] as ImportCommitResult["jobsQueued"];
}

export type CommitPreparedImportBatchOptions = {
  importBatchId?: string;
  importedByActor?: string;
  jobsQueued?: ImportCommitResult["jobsQueued"];
  importBatchExtraValues?: Record<string, unknown>;
};

export type CommitPreparedImportBatchResult = {
  preview: ImportCommitResult;
  importBatchId: string;
  jobsQueued: ImportCommitResult["jobsQueued"];
  insertedTransactions: Transaction[];
};

export type SyntheticImportBatchCommitResult = {
  importBatchId: string;
  insertedTransactions: Transaction[];
};

function isUniqueViolation(error: unknown): error is { code: string } {
  return (
    error !== null &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}

async function insertTransactions(
  sql: SqlClient,
  transactions: readonly Transaction[],
) {
  const insertedTransactions: Transaction[] = [];

  for (const transaction of transactions) {
    try {
      const inserted = await sql`
        insert into public.transactions (
          id,
          user_id,
          account_id,
          account_entity_id,
          economic_entity_id,
          import_batch_id,
          provider_name,
          provider_record_id,
          source_fingerprint,
          duplicate_key,
          transaction_date,
          posted_date,
          amount_original,
          currency_original,
          amount_base_eur,
          fx_rate_to_eur,
          description_raw,
          description_clean,
          merchant_normalized,
          counterparty_name,
          transaction_class,
          category_code,
          subcategory_code,
          transfer_group_id,
          related_account_id,
          related_transaction_id,
          transfer_match_status,
          cross_entity_flag,
          reimbursement_status,
          classification_status,
          classification_source,
          classification_confidence,
          needs_review,
          review_reason,
          exclude_from_analytics,
          correction_of_transaction_id,
          voided_at,
          manual_notes,
          llm_payload,
          raw_payload,
          security_id,
          quantity,
          unit_price_original,
          credit_card_statement_status,
          linked_credit_card_account_id,
          created_at,
          updated_at
        ) values (
          ${transaction.id},
          ${transaction.userId},
          ${transaction.accountId},
          ${transaction.accountEntityId},
          ${transaction.economicEntityId},
          ${transaction.importBatchId ?? null},
          ${transaction.providerName ?? null},
          ${transaction.providerRecordId ?? null},
          ${transaction.sourceFingerprint},
          ${transaction.duplicateKey ?? null},
          ${transaction.transactionDate},
          ${transaction.postedDate ?? null},
          ${transaction.amountOriginal},
          ${transaction.currencyOriginal},
          ${transaction.amountBaseEur},
          ${transaction.fxRateToEur ?? null},
          ${transaction.descriptionRaw},
          ${transaction.descriptionClean},
          ${transaction.merchantNormalized ?? null},
          ${transaction.counterpartyName ?? null},
          ${transaction.transactionClass},
          ${transaction.categoryCode ?? null},
          ${transaction.subcategoryCode ?? null},
          ${transaction.transferGroupId ?? null},
          ${transaction.relatedAccountId ?? null},
          ${transaction.relatedTransactionId ?? null},
          ${transaction.transferMatchStatus},
          ${transaction.crossEntityFlag},
          ${transaction.reimbursementStatus},
          ${transaction.classificationStatus},
          ${transaction.classificationSource},
          ${transaction.classificationConfidence},
          ${transaction.needsReview},
          ${transaction.reviewReason ?? null},
          ${transaction.excludeFromAnalytics},
          ${transaction.correctionOfTransactionId ?? null},
          ${transaction.voidedAt ?? null},
          ${transaction.manualNotes ?? null},
          ${serializeJson(sql, transaction.llmPayload)}::jsonb,
          ${serializeJson(sql, transaction.rawPayload)}::jsonb,
          ${transaction.securityId ?? null},
          ${transaction.quantity ?? null},
          ${transaction.unitPriceOriginal ?? null},
          ${transaction.creditCardStatementStatus},
          ${transaction.linkedCreditCardAccountId ?? null},
          ${transaction.createdAt},
          ${transaction.updatedAt}
        )
        returning id
      `;
      if (inserted.length > 0) {
        insertedTransactions.push(transaction);
      }
    } catch (error) {
      if (isUniqueViolation(error)) {
        continue;
      }
      throw error;
    }
  }

  return insertedTransactions;
}

async function queueImportBatchJobs(
  sql: SqlClient,
  input: {
    jobsQueued: ImportCommitResult["jobsQueued"];
    importBatchId: string;
    accountId: string;
  },
) {
  const availableAt = new Date().toISOString();
  for (const jobType of input.jobsQueued) {
    if (!(await supportsJobType(sql, jobType))) {
      continue;
    }
    await queueJob(
      sql,
      jobType,
      {
        importBatchId: input.importBatchId,
        accountId: input.accountId,
      },
      {
        availableAt,
      },
    );
  }
}

async function touchAccountLastImportedAt(
  sql: SqlClient,
  input: {
    accountId: string;
    userId: string;
    importedAt: string;
  },
) {
  await sql`
    update public.accounts
    set last_imported_at = ${input.importedAt}
    where id = ${input.accountId}
      and user_id = ${input.userId}
  `;
}

async function insertImportBatchRecord(
  sql: SqlClient,
  input: {
    importBatchId: string;
    userId: string;
    accountId: string;
    templateId: string | null;
    sourceKind: "upload" | "bank_sync";
    providerName: string | null;
    bankConnectionId: string | null;
    storagePath: string;
    originalFilename: string;
    rowCountDetected: number;
    rowCountParsed: number;
    rowCountInserted: number;
    rowCountDuplicates: number;
    rowCountFailed: number;
    previewSummary: Record<string, unknown>;
    commitSummary: Record<string, unknown>;
    importedByActor: string;
    importedAt: string;
    extraValues?: Record<string, unknown>;
  },
) {
  await sql`
    insert into public.import_batches ${sql({
      id: input.importBatchId,
      user_id: input.userId,
      account_id: input.accountId,
      template_id: input.templateId,
      source_kind: input.sourceKind,
      provider_name: input.providerName,
      bank_connection_id: input.bankConnectionId,
      storage_path: input.storagePath,
      original_filename: input.originalFilename,
      file_sha256: randomUUID().replace(/-/g, ""),
      status: "committed",
      row_count_detected: input.rowCountDetected,
      row_count_parsed: input.rowCountParsed,
      row_count_inserted: input.rowCountInserted,
      row_count_duplicates: input.rowCountDuplicates,
      row_count_failed: input.rowCountFailed,
      preview_summary_json: serializeJson(sql, input.previewSummary),
      commit_summary_json: serializeJson(sql, input.commitSummary),
      imported_by_actor: input.importedByActor,
      imported_at: input.importedAt,
      ...(input.extraValues ?? {}),
    } as Record<string, unknown>)}
  `;
}

function readRecordText(
  record: Record<string, unknown> | null | undefined,
  keys: readonly string[],
) {
  if (!record) {
    return null;
  }
  for (const key of keys) {
    const value = record[key];
    if (value === null || value === undefined) {
      continue;
    }
    const text = String(value).trim();
    if (text) {
      return text;
    }
  }
  return null;
}

function readSnapshotText(
  snapshot: PortfolioStatementSnapshot,
  ...keys: string[]
) {
  return readRecordText(snapshot as Record<string, unknown>, keys);
}

function readPositionText(
  position: PortfolioStatementPosition,
  ...keys: string[]
) {
  return readRecordText(position as Record<string, unknown>, keys);
}

function normalizeCurrencyCode(value: string | null, fallback: string) {
  const normalized = (value ?? fallback).trim().toUpperCase();
  return normalized || fallback;
}

function normalizeAliasText(value: string | null) {
  return (value ?? "").trim().replace(/\s+/g, " ").toLowerCase();
}

function parseStatementDecimal(value: unknown) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).replace(/\s+/g, "").replace(/,/g, "").trim();
  if (!text) {
    return null;
  }
  try {
    const decimal = new Decimal(text);
    return decimal.isFinite() ? decimal : null;
  } catch {
    return null;
  }
}

function readStatementDecimal(
  record: Record<string, unknown> | null | undefined,
  keys: readonly string[],
) {
  if (!record) {
    return null;
  }
  for (const key of keys) {
    const parsed = parseStatementDecimal(record[key]);
    if (parsed !== null) {
      return parsed;
    }
  }
  return null;
}

function normalizeIsoDate(value: string | null) {
  if (!value) {
    return null;
  }
  const directMatch = value.match(/\d{4}-\d{2}-\d{2}/);
  if (directMatch) {
    return directMatch[0];
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return new Date(parsed).toISOString().slice(0, 10);
}

function normalizeStatementTimestamp(
  value: string | null,
  statementDate: string | null,
) {
  if (value) {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return new Date(parsed).toISOString();
    }
  }
  return statementDate
    ? `${statementDate}T23:59:59.000Z`
    : new Date().toISOString();
}

function normalizeStatementAssetType(
  value: string | null,
): Security["assetType"] {
  const normalized = (value ?? "").trim().toLowerCase();
  if (normalized.includes("etf")) {
    return "etf";
  }
  if (normalized.includes("cash")) {
    return "cash";
  }
  if (
    normalized.includes("stock") ||
    normalized.includes("share") ||
    normalized.includes("gdr") ||
    normalized.includes("adr")
  ) {
    return "stock";
  }
  return "other";
}

function buildSecurityLookup(dataset: DomainDataset) {
  const byIsin = new Map<string, Security>();
  const bySymbol = new Map<string, Security>();

  for (const security of dataset.securities) {
    if (security.isin) {
      byIsin.set(security.isin.trim().toUpperCase(), security);
    }
    for (const symbol of [
      security.providerSymbol,
      security.canonicalSymbol,
      security.displaySymbol,
    ]) {
      const normalized = symbol.trim().toUpperCase();
      if (normalized) {
        bySymbol.set(normalized, security);
      }
    }
  }

  for (const alias of dataset.securityAliases) {
    const security = dataset.securities.find(
      (candidate) => candidate.id === alias.securityId,
    );
    if (!security) {
      continue;
    }
    const normalized = alias.aliasTextNormalized.trim().toUpperCase();
    if (normalized) {
      bySymbol.set(normalized, security);
    }
  }

  return {
    find(position: PortfolioStatementPosition) {
      const isin = readPositionText(
        position,
        "isin",
        "security_isin",
      )?.toUpperCase();
      if (isin && byIsin.has(isin)) {
        return byIsin.get(isin) ?? null;
      }
      const symbol = readPositionText(
        position,
        "symbol",
        "securitySymbol",
        "security_symbol",
      )?.toUpperCase();
      if (symbol && bySymbol.has(symbol)) {
        return bySymbol.get(symbol) ?? null;
      }
      return null;
    },
    remember(security: Security) {
      if (security.isin) {
        byIsin.set(security.isin.trim().toUpperCase(), security);
      }
      for (const symbol of [
        security.providerSymbol,
        security.canonicalSymbol,
        security.displaySymbol,
      ]) {
        const normalized = symbol.trim().toUpperCase();
        if (normalized) {
          bySymbol.set(normalized, security);
        }
      }
    },
  };
}

async function ensureStatementSecurity(
  sql: SqlClient,
  input: {
    position: PortfolioStatementPosition;
    snapshot: PortfolioStatementSnapshot;
    statementDate: string | null;
    defaultCurrency: string;
    existingSecurity: Security | null;
  },
) {
  if (input.existingSecurity) {
    return { security: input.existingSecurity, upserted: false };
  }

  const symbol =
    readPositionText(
      input.position,
      "symbol",
      "securitySymbol",
      "security_symbol",
    ) ??
    readPositionText(input.position, "conid") ??
    readPositionText(input.position, "isin") ??
    "UNKNOWN";
  const securityName =
    readPositionText(input.position, "securityName", "security_name", "name") ??
    symbol;
  const providerSymbol = readPositionText(input.position, "conid") ?? symbol;
  const quoteCurrency = normalizeCurrencyCode(
    readPositionText(input.position, "currency"),
    input.defaultCurrency,
  );
  const now = new Date().toISOString();
  const security: Security = {
    id: randomUUID(),
    providerName: "interactive_brokers",
    providerSymbol,
    canonicalSymbol: symbol,
    displaySymbol: symbol,
    name: securityName,
    exchangeName: readPositionText(input.position, "exchange") ?? "IBKR",
    micCode: null,
    assetType: normalizeStatementAssetType(
      readPositionText(input.position, "assetType", "asset_type"),
    ),
    quoteCurrency,
    country: null,
    isin: readPositionText(input.position, "isin"),
    figi: null,
    active: true,
    metadataJson: {
      source: "ibkr_statement",
      statementDate: input.statementDate,
      accountNumber: readSnapshotText(
        input.snapshot,
        "accountNumber",
        "account_number",
      ),
      conid: readPositionText(input.position, "conid"),
      exchange: readPositionText(input.position, "exchange"),
      originalPosition: input.position,
    },
    lastPriceRefreshAt: null,
    createdAt: now,
  };

  const rows = await sql`
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
    on conflict (provider_name, provider_symbol)
    do update set
      canonical_symbol = excluded.canonical_symbol,
      display_symbol = excluded.display_symbol,
      name = excluded.name,
      exchange_name = excluded.exchange_name,
      mic_code = excluded.mic_code,
      asset_type = excluded.asset_type,
      quote_currency = excluded.quote_currency,
      country = excluded.country,
      isin = coalesce(public.securities.isin, excluded.isin),
      figi = coalesce(public.securities.figi, excluded.figi),
      active = true,
      metadata_json = public.securities.metadata_json || excluded.metadata_json
    returning id
  `;
  return {
    security: {
      ...security,
      id: String(rows[0]?.id ?? security.id),
    },
    upserted: true,
  };
}

async function insertStatementSecurityAliases(
  sql: SqlClient,
  input: {
    securityId: string;
    templateId: string | null;
    position: PortfolioStatementPosition;
  },
) {
  let inserted = 0;
  const aliases = [
    readPositionText(
      input.position,
      "symbol",
      "securitySymbol",
      "security_symbol",
    ),
    readPositionText(input.position, "isin"),
    readPositionText(input.position, "conid"),
  ]
    .map(normalizeAliasText)
    .filter(Boolean);

  for (const aliasTextNormalized of [...new Set(aliases)]) {
    const rows = await sql`
      insert into public.security_aliases ${sql({
        id: randomUUID(),
        security_id: input.securityId,
        alias_text_normalized: aliasTextNormalized,
        alias_source: "provider",
        template_id: input.templateId,
        confidence: "1",
        created_at: new Date().toISOString(),
      } as Record<string, unknown>)}
      on conflict (security_id, alias_text_normalized) do nothing
      returning id
    `;
    if (rows.length > 0) {
      inserted += 1;
    }
  }

  return inserted;
}

async function upsertStatementSecurityPrice(
  sql: SqlClient,
  input: {
    securityId: string;
    position: PortfolioStatementPosition;
    snapshot: PortfolioStatementSnapshot;
    statementDate: string;
    quoteTimestamp: string;
    defaultCurrency: string;
  },
) {
  const price = readStatementDecimal(
    input.position as Record<string, unknown>,
    ["closePrice", "close_price"],
  );
  if (price === null) {
    return false;
  }

  await sql`
    insert into public.security_prices ${sql({
      security_id: input.securityId,
      price_date: input.statementDate,
      quote_timestamp: input.quoteTimestamp,
      price: price.toFixed(),
      currency: normalizeCurrencyCode(
        readPositionText(input.position, "currency"),
        input.defaultCurrency,
      ),
      source_name: IBKR_STATEMENT_PRICE_SOURCE,
      is_realtime: false,
      is_delayed: true,
      market_state: "statement_close",
      raw_json: serializeJson(sql, {
        source: "ibkr_statement",
        snapshot: input.snapshot,
        position: input.position,
      }),
      created_at: new Date().toISOString(),
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
  return true;
}

async function maybeInsertOpeningPositionFromStatement(
  sql: SqlClient,
  input: {
    userId: string;
    entityId: string;
    accountId: string;
    securityId: string;
    statementDate: string;
    importBatchId: string;
    accountNumber: string | null;
    position: PortfolioStatementPosition;
  },
) {
  const quantity = readStatementDecimal(
    input.position as Record<string, unknown>,
    ["quantity"],
  );
  if (quantity === null || quantity.isZero()) {
    return { inserted: false, skippedReason: "missing_or_zero_quantity" };
  }

  const existingRows = await sql`
    select
      exists(
        select 1
        from public.holding_adjustments
        where user_id = ${input.userId}
          and account_id = ${input.accountId}
          and security_id = ${input.securityId}
          and effective_date <= ${input.statementDate}
      ) as has_adjustment,
      exists(
        select 1
        from public.transactions
        where user_id = ${input.userId}
          and account_id = ${input.accountId}
          and security_id = ${input.securityId}
          and transaction_date <= ${input.statementDate}
          and voided_at is null
      ) as has_transaction
  `;
  const hasPriorPositionEvidence = Boolean(
    existingRows[0]?.has_adjustment || existingRows[0]?.has_transaction,
  );
  if (hasPriorPositionEvidence) {
    return { inserted: false, skippedReason: "position_already_seeded" };
  }

  const costBasis = readStatementDecimal(
    input.position as Record<string, unknown>,
    ["costBasis", "cost_basis"],
  );
  await sql`
    insert into public.holding_adjustments ${sql({
      id: randomUUID(),
      user_id: input.userId,
      entity_id: input.entityId,
      account_id: input.accountId,
      security_id: input.securityId,
      effective_date: input.statementDate,
      share_delta: quantity.toFixed(),
      cost_basis_delta_eur: costBasis?.toFixed() ?? null,
      reason: "opening_position",
      note: `Seeded from IBKR statement${input.accountNumber ? ` ${input.accountNumber}` : ""} import ${input.importBatchId}.`,
      created_at: new Date().toISOString(),
    } as Record<string, unknown>)}
  `;

  return { inserted: true, skippedReason: null };
}

async function persistPortfolioStatementSnapshot(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    accountId: string;
    templateId: string | null;
    importBatchId: string;
    snapshot: PortfolioStatementSnapshot | null | undefined;
  },
): Promise<PortfolioStatementSnapshotCommitSummary | null> {
  if (!input.snapshot) {
    return null;
  }

  const account = input.dataset.accounts.find(
    (candidate) =>
      candidate.id === input.accountId && candidate.userId === input.userId,
  );
  if (!account) {
    throw new Error(`Account ${input.accountId} was not found.`);
  }

  const statementDate = normalizeIsoDate(
    readSnapshotText(
      input.snapshot,
      "statementDate",
      "statement_date",
      "periodEnd",
      "period_end",
    ),
  );
  const accountNumber = readSnapshotText(
    input.snapshot,
    "accountNumber",
    "account_number",
  );
  const baseCurrency = normalizeCurrencyCode(
    readSnapshotText(input.snapshot, "baseCurrency", "base_currency"),
    account.defaultCurrency,
  );
  const openPositions =
    input.snapshot.openPositions ?? input.snapshot.open_positions ?? [];
  const summary: PortfolioStatementSnapshotCommitSummary = {
    statementDate,
    accountNumber,
    cashSnapshotUpserted: false,
    cashSnapshotMethod: null,
    cashSnapshotIncludesDividendAccruals: false,
    openPositionsDetected: openPositions.length,
    securitiesUpserted: 0,
    securityAliasesInserted: 0,
    pricesUpserted: 0,
    openingPositionsInserted: 0,
    openingPositionsSkipped: 0,
    skippedReasons: [],
  };
  if (!statementDate) {
    summary.skippedReasons.push("missing_statement_date");
    return summary;
  }

  const cashIncludingAccruals = readStatementDecimal(
    input.snapshot as Record<string, unknown>,
    ["cashBalanceIncludingAccruals", "cash_balance_including_accruals"],
  );
  const cashBalance = readStatementDecimal(
    input.snapshot as Record<string, unknown>,
    ["cashBalance", "cash_balance"],
  );
  const dividendAccruals = readStatementDecimal(
    input.snapshot as Record<string, unknown>,
    ["dividendAccruals", "dividend_accruals"],
  );
  const netAssetValue = readStatementDecimal(
    input.snapshot as Record<string, unknown>,
    ["netAssetValue", "net_asset_value"],
  );
  const positionMarketValueTotal = openPositions.reduce<Decimal | null>(
    (sum, position) => {
      if (sum === null) {
        return null;
      }
      const positionCurrency = normalizeCurrencyCode(
        readPositionText(position, "currency"),
        baseCurrency,
      );
      if (positionCurrency !== baseCurrency) {
        return null;
      }
      const marketValue = readStatementDecimal(
        position as Record<string, unknown>,
        ["marketValue", "market_value"],
      );
      return marketValue === null ? null : sum.plus(marketValue);
    },
    new Decimal(0),
  );
  const cashFromNav =
    netAssetValue !== null && positionMarketValueTotal !== null
      ? netAssetValue.minus(positionMarketValueTotal)
      : null;
  const balanceOriginal =
    cashFromNav ??
    cashIncludingAccruals ??
    (cashBalance && dividendAccruals
      ? cashBalance.plus(dividendAccruals)
      : cashBalance);

  if (balanceOriginal !== null && baseCurrency === "EUR") {
    await sql`
      insert into public.account_balance_snapshots ${sql({
        account_id: input.accountId,
        as_of_date: statementDate,
        balance_original: balanceOriginal.toFixed(),
        balance_currency: baseCurrency,
        balance_base_eur: balanceOriginal.toFixed(),
        source_kind: "statement",
        import_batch_id: input.importBatchId,
      } as Record<string, unknown>)}
      on conflict (account_id, as_of_date)
      do update set
        balance_original = excluded.balance_original,
        balance_currency = excluded.balance_currency,
        balance_base_eur = excluded.balance_base_eur,
        source_kind = excluded.source_kind,
        import_batch_id = excluded.import_batch_id
    `;
    summary.cashSnapshotUpserted = true;
    summary.cashSnapshotMethod =
      cashFromNav !== null
        ? "net_asset_value_less_positions"
        : cashIncludingAccruals !== null
          ? "cash_including_accruals"
          : cashBalance !== null && dividendAccruals !== null
            ? "cash_plus_dividend_accruals"
            : "cash_balance";
    summary.cashSnapshotIncludesDividendAccruals =
      cashFromNav !== null ||
      cashIncludingAccruals !== null ||
      (cashBalance !== null &&
        dividendAccruals !== null &&
        !dividendAccruals.isZero());
  } else if (balanceOriginal !== null) {
    summary.skippedReasons.push("cash_snapshot_requires_eur_base_currency");
  }

  const lookup = buildSecurityLookup(input.dataset);
  const quoteTimestamp = normalizeStatementTimestamp(
    readSnapshotText(input.snapshot, "generatedAt", "generated_at"),
    statementDate,
  );

  for (const position of openPositions) {
    const ensuredSecurity = await ensureStatementSecurity(sql, {
      position,
      snapshot: input.snapshot,
      statementDate,
      defaultCurrency: baseCurrency,
      existingSecurity: lookup.find(position),
    });
    if (ensuredSecurity.upserted) {
      summary.securitiesUpserted += 1;
      lookup.remember(ensuredSecurity.security);
    }

    summary.securityAliasesInserted += await insertStatementSecurityAliases(
      sql,
      {
        securityId: ensuredSecurity.security.id,
        templateId: input.templateId,
        position,
      },
    );

    const priceUpserted = await upsertStatementSecurityPrice(sql, {
      securityId: ensuredSecurity.security.id,
      position,
      snapshot: input.snapshot,
      statementDate,
      quoteTimestamp,
      defaultCurrency: baseCurrency,
    });
    if (priceUpserted) {
      summary.pricesUpserted += 1;
    }

    const openingPosition = await maybeInsertOpeningPositionFromStatement(sql, {
      userId: input.userId,
      entityId: account.entityId,
      accountId: input.accountId,
      securityId: ensuredSecurity.security.id,
      statementDate,
      importBatchId: input.importBatchId,
      accountNumber,
      position,
    });
    if (openingPosition.inserted) {
      summary.openingPositionsInserted += 1;
    } else {
      summary.openingPositionsSkipped += 1;
      if (openingPosition.skippedReason) {
        summary.skippedReasons.push(openingPosition.skippedReason);
      }
    }
  }

  summary.skippedReasons = [...new Set(summary.skippedReasons)];
  return summary;
}

async function finalizeImportBatchRecord(
  sql: SqlClient,
  input: {
    importBatchId: string;
    userId: string;
    rowCountInserted: number;
    rowCountDuplicates: number;
    commitSummary: Record<string, unknown>;
  },
) {
  await sql`
    update public.import_batches
    set row_count_inserted = ${input.rowCountInserted},
        row_count_duplicates = ${input.rowCountDuplicates},
        commit_summary_json = ${serializeJson(sql, input.commitSummary)}::jsonb
    where id = ${input.importBatchId}
      and user_id = ${input.userId}
  `;
}

async function persistCommittedImportBatch(
  sql: SqlClient,
  input: {
    importBatchRecord: Parameters<typeof insertImportBatchRecord>[1];
    dataset?: DomainDataset;
    userId: string;
    accountId: string;
    templateId?: string | null;
    importedAt: string;
    jobsQueued: ImportCommitResult["jobsQueued"];
    preparedTransactions: Transaction[];
    portfolioStatementSnapshot?: PortfolioStatementSnapshot | null;
    queueBeforeInsert?: boolean;
  },
) {
  await insertImportBatchRecord(sql, input.importBatchRecord);

  let insertedTransactions: Transaction[] = [];
  let portfolioStatementSummary: PortfolioStatementSnapshotCommitSummary | null =
    null;
  if (input.queueBeforeInsert) {
    await queueImportBatchJobs(sql, {
      jobsQueued: input.jobsQueued,
      importBatchId: input.importBatchRecord.importBatchId,
      accountId: input.accountId,
    });
    insertedTransactions = await insertTransactions(
      sql,
      input.preparedTransactions,
    );
    portfolioStatementSummary =
      input.portfolioStatementSnapshot && input.dataset
        ? await persistPortfolioStatementSnapshot(sql, {
            userId: input.userId,
            dataset: input.dataset,
            accountId: input.accountId,
            templateId: input.templateId ?? null,
            importBatchId: input.importBatchRecord.importBatchId,
            snapshot: input.portfolioStatementSnapshot,
          })
        : null;
  } else {
    insertedTransactions = await insertTransactions(
      sql,
      input.preparedTransactions,
    );
    portfolioStatementSummary =
      input.portfolioStatementSnapshot && input.dataset
        ? await persistPortfolioStatementSnapshot(sql, {
            userId: input.userId,
            dataset: input.dataset,
            accountId: input.accountId,
            templateId: input.templateId ?? null,
            importBatchId: input.importBatchRecord.importBatchId,
            snapshot: input.portfolioStatementSnapshot,
          })
        : null;
    await queueImportBatchJobs(sql, {
      jobsQueued: input.jobsQueued,
      importBatchId: input.importBatchRecord.importBatchId,
      accountId: input.accountId,
    });
  }

  await touchAccountLastImportedAt(sql, {
    accountId: input.accountId,
    userId: input.userId,
    importedAt: input.importedAt,
  });

  return {
    insertedTransactions,
    transactionIds: insertedTransactions.map((transaction) => transaction.id),
    portfolioStatementSummary,
  };
}

export function sumPreparedTransactionAmountBaseEur(
  transactions: Transaction[],
) {
  return transactions
    .reduce(
      (sum, transaction) => sum.plus(transaction.amountBaseEur),
      new Decimal(0),
    )
    .toFixed(2);
}

export async function commitPreparedImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    normalizedInput: ReturnType<typeof normalizeImportExecutionInput>;
    previewFallback?: () => Promise<ImportPreviewResult>;
    options?: CommitPreparedImportBatchOptions;
  },
): Promise<CommitPreparedImportBatchResult> {
  const commitResult = input.normalizedInput.filePath
    ? await runDeterministicImport(
        "commit",
        input.normalizedInput,
        input.dataset,
      )
    : null;
  const importBatchId = input.options?.importBatchId ?? randomUUID();
  const preparedTransactions =
    commitResult && input.normalizedInput.filePath
      ? buildImportedTransactions(
          input.dataset,
          input.normalizedInput,
          importBatchId,
          commitResult.normalizedRows ?? [],
        )
      : null;
  const preview =
    commitResult && input.normalizedInput.filePath
      ? ({
          ...(sanitizeImportResult(commitResult) as ImportCommitResult),
          rowCountDuplicates: preparedTransactions?.duplicateCount ?? 0,
        } satisfies ImportCommitResult)
      : ({
          ...((input.previewFallback
            ? await input.previewFallback()
            : await (async () => {
                throw new Error(
                  "A file path is required to commit this import flow.",
                );
              })()) as ImportCommitResult),
          importBatchId,
          rowCountInserted: 0,
          transactionIds: [],
          jobsQueued: [...DEFAULT_IMPORT_JOBS_QUEUED],
        } satisfies ImportCommitResult);
  const jobsQueued =
    input.options?.jobsQueued ??
    mergeDefaultImportJobs(
      (commitResult as ImportCommitResult | null)?.jobsQueued as
        | ImportCommitResult["jobsQueued"]
        | undefined,
    );
  const importedAt = new Date().toISOString();

  const { insertedTransactions, transactionIds, portfolioStatementSummary } =
    await persistCommittedImportBatch(sql, {
      importBatchRecord: {
        importBatchId,
        userId: input.userId,
        accountId: input.normalizedInput.accountId,
        templateId: input.normalizedInput.templateId,
        sourceKind: "upload",
        providerName: null,
        bankConnectionId: null,
        storagePath: input.normalizedInput.filePath
          ? `private-imports/local/${input.normalizedInput.originalFilename}`
          : `private-imports/manual/${input.normalizedInput.originalFilename}`,
        originalFilename: input.normalizedInput.originalFilename,
        rowCountDetected: preview.rowCountDetected,
        rowCountParsed: preview.rowCountParsed,
        rowCountInserted:
          preparedTransactions?.inserted.length ?? preview.rowCountParsed,
        rowCountDuplicates:
          preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates,
        rowCountFailed: preview.rowCountFailed,
        previewSummary: {
          sampleRows: preview.sampleRows,
          parseErrors: preview.parseErrors,
          dateRange: preview.dateRange,
        },
        commitSummary: { jobsQueued },
        importedByActor: input.options?.importedByActor ?? "web-cli",
        importedAt,
        extraValues: input.options?.importBatchExtraValues,
      },
      dataset: input.dataset,
      userId: input.userId,
      accountId: input.normalizedInput.accountId,
      templateId: input.normalizedInput.templateId,
      importedAt,
      jobsQueued,
      preparedTransactions: preparedTransactions?.inserted ?? [],
      portfolioStatementSnapshot: commitResult?.portfolioStatementSnapshot,
      queueBeforeInsert: false,
    });

  const rowCountInserted =
    insertedTransactions.length ||
    (preparedTransactions ? 0 : preview.rowCountParsed);
  const rowCountDuplicates =
    preparedTransactions === null
      ? preview.rowCountDuplicates
      : (preparedTransactions.duplicateCount ?? 0) +
        (preparedTransactions.inserted.length - insertedTransactions.length);

  await finalizeImportBatchRecord(sql, {
    importBatchId,
    userId: input.userId,
    rowCountInserted,
    rowCountDuplicates,
    commitSummary: {
      jobsQueued,
      transactionIds,
      ...(portfolioStatementSummary
        ? { portfolioStatementSnapshot: portfolioStatementSummary }
        : {}),
    },
  });

  return {
    preview: {
      ...preview,
      importBatchId,
      rowCountInserted,
      rowCountDuplicates,
      transactionIds,
      jobsQueued: [...jobsQueued],
    },
    importBatchId,
    jobsQueued: [...jobsQueued],
    insertedTransactions,
  };
}

export async function commitSyntheticImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    accountId: string;
    originalFilename: string;
    sourceKind: "bank_sync";
    providerName: string;
    bankConnectionId: string;
    preparedTransactions: Transaction[];
    importedByActor: string;
    jobsQueued?: ImportCommitResult["jobsQueued"];
    dateRange?: { start: string; end: string } | null;
  },
): Promise<SyntheticImportBatchCommitResult> {
  const importBatchId = randomUUID();
  const jobsQueued =
    input.jobsQueued ??
    (input.preparedTransactions.length > 0
      ? [...DEFAULT_IMPORT_JOBS_QUEUED]
      : (["metric_refresh"] satisfies ImportCommitResult["jobsQueued"]));
  const importedAt = new Date().toISOString();

  const preparedTransactions = input.preparedTransactions.map(
    (transaction) => ({
      ...transaction,
      importBatchId,
    }),
  );
  const { insertedTransactions, transactionIds } =
    await persistCommittedImportBatch(sql, {
      importBatchRecord: {
        importBatchId,
        userId: input.userId,
        accountId: input.accountId,
        templateId: null,
        sourceKind: input.sourceKind,
        providerName: input.providerName,
        bankConnectionId: input.bankConnectionId,
        storagePath: `bank-sync/${input.providerName}/${input.bankConnectionId}/${input.originalFilename}`,
        originalFilename: input.originalFilename,
        rowCountDetected: input.preparedTransactions.length,
        rowCountParsed: input.preparedTransactions.length,
        rowCountInserted: input.preparedTransactions.length,
        rowCountDuplicates: 0,
        rowCountFailed: 0,
        previewSummary: {
          dateRange: input.dateRange ?? null,
          sampleRows: input.preparedTransactions
            .slice(0, 3)
            .map((transaction) => ({
              providerRecordId: transaction.providerRecordId,
              transactionDate: transaction.transactionDate,
              amountOriginal: transaction.amountOriginal,
              currencyOriginal: transaction.currencyOriginal,
              descriptionRaw: transaction.descriptionRaw,
            })),
        },
        commitSummary: {
          jobsQueued,
          sourceKind: input.sourceKind,
          providerName: input.providerName,
        },
        importedByActor: input.importedByActor,
        importedAt,
      },
      userId: input.userId,
      accountId: input.accountId,
      importedAt,
      jobsQueued,
      preparedTransactions,
    });

  await finalizeImportBatchRecord(sql, {
    importBatchId,
    userId: input.userId,
    rowCountInserted: insertedTransactions.length,
    rowCountDuplicates: Math.max(
      0,
      input.preparedTransactions.length - insertedTransactions.length,
    ),
    commitSummary: {
      jobsQueued,
      sourceKind: input.sourceKind,
      providerName: input.providerName,
      transactionIds,
    },
  });

  return {
    importBatchId,
    insertedTransactions,
  };
}

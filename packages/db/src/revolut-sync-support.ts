import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import {
  normalizeDescription,
  resolveFxRate,
  type Account,
  type AuditEvent,
  type BankAccountLink,
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";

import { createAuditEvent, insertAuditEventRecord } from "./audit-log";
import { queueJob } from "./job-state";
import {
  buildRevolutProviderContext,
  type RevolutAccount,
  type RevolutExpense,
  type RevolutTransaction,
} from "./revolut";
import type { SqlClient } from "./sql-runtime";

export const REVOLUT_PROVIDER_NAME = "revolut_business";
export const REVOLUT_CONNECTION_LABEL = "Revolut Business";

export type BankSyncTrigger =
  | "oauth_callback"
  | "manual_sync"
  | "webhook"
  | "scheduled";

function normalizeDescriptionForSourceImport(value: string) {
  return normalizeDescription(value).clean;
}

function humanizeRevolutType(value: string) {
  return value.replace(/_/g, " ").replace(/\s+/g, " ").trim().toUpperCase();
}

export function getRevolutAccountDisplayName(account: RevolutAccount) {
  const normalizedName =
    typeof account.name === "string" && account.name.trim()
      ? account.name.trim()
      : null;
  if (normalizedName) {
    return normalizedName;
  }

  return `Revolut ${account.currency} ${account.id.slice(0, 8)}`;
}

export function buildRevolutProviderRecordId(
  transaction: RevolutTransaction,
  legId: string,
) {
  return `${transaction.id}:${legId}`;
}

export function buildRevolutSourceFingerprint(
  accountId: string,
  providerRecordId: string,
) {
  return `${REVOLUT_PROVIDER_NAME}:${accountId}:${providerRecordId}`;
}

function sliceIsoDate(value: string | null | undefined) {
  return typeof value === "string" && value.length >= 10
    ? value.slice(0, 10)
    : null;
}

function buildRevolutTransactionDescription(
  transaction: RevolutTransaction,
  merchantName: string | null,
  legDescription: string | null,
) {
  const pieces = [
    merchantName,
    legDescription,
    transaction.reference ?? null,
  ].filter((value): value is string => Boolean(value));
  return (
    [...new Set(pieces)].join(" | ") || humanizeRevolutType(transaction.type)
  );
}

export async function upsertAccountBalanceSnapshot(
  sql: SqlClient,
  input: {
    accountId: string;
    asOfDate: string;
    balanceOriginal: string;
    balanceCurrency: string;
    balanceBaseEur: string;
  },
) {
  await sql`
    insert into public.account_balance_snapshots ${sql({
      account_id: input.accountId,
      as_of_date: input.asOfDate,
      balance_original: input.balanceOriginal,
      balance_currency: input.balanceCurrency,
      balance_base_eur: input.balanceBaseEur,
      source_kind: "statement",
      import_batch_id: null,
    } as Record<string, unknown>)}
    on conflict (account_id, as_of_date)
    do update set
      balance_original = excluded.balance_original,
      balance_currency = excluded.balance_currency,
      balance_base_eur = excluded.balance_base_eur,
      source_kind = excluded.source_kind,
      import_batch_id = excluded.import_batch_id
  `;
}

async function createRevolutManagedAccount(
  sql: SqlClient,
  input: {
    userId: string;
    entityId: string;
    revolutAccount: RevolutAccount;
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
): Promise<Account> {
  const accountId = randomUUID();
  const now = new Date().toISOString();
  const displayName = getRevolutAccountDisplayName(input.revolutAccount);
  const matchingAliases = [input.revolutAccount.currency, displayName].filter(
    (value, index, values) => values.indexOf(value) === index,
  );
  const account = {
    id: accountId,
    userId: input.userId,
    entityId: input.entityId,
    institutionName: REVOLUT_CONNECTION_LABEL,
    displayName,
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: input.revolutAccount.currency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: null,
    matchingAliases,
    accountSuffix: null,
    balanceMode: "statement",
    staleAfterDays: null,
    lastImportedAt: null,
    createdAt: now,
  } satisfies Account;

  await sql`
    insert into public.accounts ${sql({
      id: account.id,
      user_id: account.userId,
      entity_id: account.entityId,
      institution_name: account.institutionName,
      display_name: account.displayName,
      account_type: account.accountType,
      asset_domain: account.assetDomain,
      default_currency: account.defaultCurrency,
      opening_balance_original: null,
      opening_balance_currency: null,
      opening_balance_date: null,
      include_in_consolidation: account.includeInConsolidation,
      is_active: account.isActive,
      import_template_default_id: null,
      matching_aliases: account.matchingAliases,
      account_suffix: null,
      balance_mode: account.balanceMode,
      stale_after_days: null,
      last_imported_at: null,
      created_at: account.createdAt,
    } as Record<string, unknown>)}
  `;

  await insertAuditEventRecord(
    sql,
    createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "accounts.create",
      "account",
      account.id,
      null,
      account as unknown as Record<string, unknown>,
    ),
    "Auto-created a company bank account from a Revolut Business connection.",
  );

  return account;
}

export async function resolveOrCreateRevolutAccountLinks(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    connectionId: string;
    entityId: string;
    revolutAccounts: RevolutAccount[];
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
) {
  const now = new Date().toISOString();
  const nextAccounts = [...input.dataset.accounts];
  const nextLinks = [...input.dataset.bankAccountLinks];

  for (const revolutAccount of input.revolutAccounts.filter(
    (account) => account.state === "active",
  )) {
    const revolutAccountDisplayName =
      getRevolutAccountDisplayName(revolutAccount);
    const isAccountReservedForDifferentExternalAccount = (accountId: string) =>
      nextLinks.some(
        (link) =>
          link.provider === REVOLUT_PROVIDER_NAME &&
          link.accountId === accountId &&
          link.externalAccountId !== revolutAccount.id,
      );
    let linkedAccount =
      nextLinks
        .filter(
          (link) =>
            link.connectionId === input.connectionId &&
            link.externalAccountId === revolutAccount.id,
        )
        .map(
          (link) =>
            nextAccounts.find((account) => account.id === link.accountId) ??
            null,
        )
        .find((account): account is Account => Boolean(account)) ?? null;

    if (!linkedAccount) {
      const candidates = nextAccounts.filter(
        (account) =>
          account.entityId === input.entityId &&
          account.accountType === "company_bank" &&
          account.assetDomain === "cash" &&
          account.isActive &&
          account.institutionName === REVOLUT_CONNECTION_LABEL &&
          account.defaultCurrency === revolutAccount.currency &&
          !isAccountReservedForDifferentExternalAccount(account.id),
      );
      linkedAccount =
        candidates.find(
          (account) => account.displayName === revolutAccountDisplayName,
        ) ?? (candidates.length === 1 ? candidates[0] : null);
    }

    if (!linkedAccount) {
      linkedAccount = await createRevolutManagedAccount(sql, {
        userId: input.userId,
        entityId: input.entityId,
        revolutAccount,
        actorName: input.actorName,
        sourceChannel: input.sourceChannel,
      });
      nextAccounts.push(linkedAccount);
    }

    const linkId =
      nextLinks.find(
        (link) =>
          link.connectionId === input.connectionId &&
          link.externalAccountId === revolutAccount.id,
      )?.id ?? randomUUID();

    await sql`
      insert into public.bank_account_links ${sql({
        id: linkId,
        user_id: input.userId,
        connection_id: input.connectionId,
        account_id: linkedAccount.id,
        provider: REVOLUT_PROVIDER_NAME,
        external_account_id: revolutAccount.id,
        external_account_name: revolutAccountDisplayName,
        external_currency: revolutAccount.currency,
        last_seen_at: now,
        created_at: now,
        updated_at: now,
      } as Record<string, unknown>)}
      on conflict (connection_id, external_account_id)
      do update set
        account_id = excluded.account_id,
        external_account_name = excluded.external_account_name,
        external_currency = excluded.external_currency,
        last_seen_at = excluded.last_seen_at,
        updated_at = excluded.updated_at
    `;

    const nextLink = {
      id: linkId,
      userId: input.userId,
      connectionId: input.connectionId,
      accountId: linkedAccount.id,
      provider: REVOLUT_PROVIDER_NAME,
      externalAccountId: revolutAccount.id,
      externalAccountName: revolutAccountDisplayName,
      externalCurrency: revolutAccount.currency,
      lastSeenAt: now,
      createdAt: now,
      updatedAt: now,
    } satisfies BankAccountLink;
    const existingIndex = nextLinks.findIndex((link) => link.id === linkId);
    if (existingIndex === -1) {
      nextLinks.push(nextLink);
    } else {
      nextLinks[existingIndex] = nextLink;
    }
  }

  return {
    dataset: {
      ...input.dataset,
      accounts: nextAccounts,
      bankAccountLinks: nextLinks,
    } satisfies DomainDataset,
    bankAccountLinks: nextLinks.filter(
      (link) => link.connectionId === input.connectionId,
    ),
  };
}

export function buildRevolutSyntheticTransaction(input: {
  dataset: DomainDataset;
  account: Account;
  transaction: RevolutTransaction;
  expense: RevolutExpense | null;
  leg: RevolutTransaction["legs"][number];
  importBatchId: string | null;
}) {
  const providerRecordId = buildRevolutProviderRecordId(
    input.transaction,
    input.leg.leg_id,
  );
  const transactionDate =
    sliceIsoDate(input.transaction.completed_at) ??
    sliceIsoDate(input.transaction.created_at) ??
    new Date().toISOString().slice(0, 10);
  const postedDate =
    sliceIsoDate(input.transaction.completed_at) ??
    sliceIsoDate(input.transaction.updated_at) ??
    transactionDate;
  const amountOriginal = new Decimal(input.leg.amount).toFixed(8);
  const currencyOriginal = input.leg.currency.toUpperCase();
  const fxRateToEur = resolveFxRate(
    input.dataset,
    currencyOriginal,
    "EUR",
    transactionDate,
  ).toFixed(8);
  const amountBaseEur = new Decimal(amountOriginal)
    .times(new Decimal(fxRateToEur))
    .toFixed(8);
  const merchantName = input.transaction.merchant?.name?.trim() || null;
  const descriptionRaw = buildRevolutTransactionDescription(
    input.transaction,
    merchantName,
    input.leg.description?.trim() || null,
  );
  const createdAt = new Date().toISOString();
  const providerContext = buildRevolutProviderContext({
    transaction: input.transaction,
    leg: input.leg,
    expense: input.expense,
  });

  return {
    id: randomUUID(),
    userId: input.dataset.profile.id,
    accountId: input.account.id,
    accountEntityId: input.account.entityId,
    economicEntityId: input.account.entityId,
    importBatchId: input.importBatchId,
    providerName: REVOLUT_PROVIDER_NAME,
    providerRecordId,
    sourceFingerprint: buildRevolutSourceFingerprint(
      input.account.id,
      providerRecordId,
    ),
    duplicateKey: providerRecordId,
    transactionDate,
    postedDate,
    amountOriginal,
    currencyOriginal,
    amountBaseEur,
    fxRateToEur,
    descriptionRaw,
    descriptionClean: normalizeDescriptionForSourceImport(descriptionRaw),
    merchantNormalized: merchantName,
    counterpartyName: null,
    transactionClass: "unknown",
    categoryCode: null,
    subcategoryCode: null,
    transferGroupId: null,
    relatedAccountId: null,
    relatedTransactionId: null,
    transferMatchStatus: "not_transfer",
    crossEntityFlag: false,
    reimbursementStatus: "none",
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Queued for automatic transaction analysis.",
    excludeFromAnalytics: false,
    correctionOfTransactionId: null,
    voidedAt: null,
    manualNotes: null,
    llmPayload: {
      analysisStatus: "pending",
      explanation: null,
      model: null,
      error: null,
      queuedAt: createdAt,
      providerContext,
    },
    rawPayload: {
      provider: REVOLUT_PROVIDER_NAME,
      providerContext,
      providerRaw: {
        transaction: input.transaction,
        expense: input.expense,
      },
    },
    securityId: null,
    quantity: null,
    unitPriceOriginal: null,
    creditCardStatementStatus: "not_applicable",
    linkedCreditCardAccountId: null,
    createdAt,
    updatedAt: createdAt,
  } satisfies Transaction;
}

export async function queueUniqueRevolutSyncJob(
  sql: SqlClient,
  input: {
    userId: string;
    connectionId: string;
    trigger: BankSyncTrigger;
    availableAt?: string;
  },
) {
  const existing =
    input.trigger === "scheduled"
      ? await sql`
          select id
          from public.jobs
          where job_type = ${"bank_sync"}
            and (status = ${"queued"} or status = ${"running"})
            and payload_json->>'connectionId' = ${input.connectionId}
          limit 1
        `
      : await sql`
          select id
          from public.jobs
          where job_type = ${"bank_sync"}
            and (
              status = ${"running"}
              or (
                status = ${"queued"}
                and available_at <= ${new Date().toISOString()}
              )
            )
            and payload_json->>'connectionId' = ${input.connectionId}
          limit 1
        `;
  if (existing[0]?.id) {
    return {
      queued: false,
      jobId: String(existing[0].id),
    };
  }

  const queuedAt = new Date().toISOString();
  const jobId = await queueJob(
    sql,
    "bank_sync",
    {
      connectionId: input.connectionId,
      trigger: input.trigger,
      queuedAt,
    },
    {
      availableAt: input.availableAt,
    },
  );
  await sql`
    update public.bank_connections
    set last_sync_queued_at = ${queuedAt},
        updated_at = ${queuedAt}
    where id = ${input.connectionId}
      and user_id = ${input.userId}
  `;
  return {
    queued: true,
    jobId,
  };
}

export async function runRevolutSyncWithLock<T>(
  sql: SqlClient,
  connectionId: string,
  runner: () => Promise<T>,
) {
  const lockRows = await sql`
    select pg_try_advisory_lock(hashtext(${connectionId}), 814) as locked
  `;
  if (lockRows[0]?.locked !== true) {
    throw new Error(
      `Revolut connection ${connectionId} is already syncing in another worker.`,
    );
  }

  try {
    return await runner();
  } finally {
    await sql`
      select pg_advisory_unlock(hashtext(${connectionId}), 814)
    `;
  }
}

import { Decimal } from "decimal.js";

import { resolveFxRate, type Transaction } from "@myfinance/domain";

import { loadDatasetForUser } from "./dataset-loader";
import { commitSyntheticImportBatch } from "./import-batches";
import { queueJob } from "./job-state";
import {
  decryptBankSecret,
  encryptBankSecret,
  fetchRevolutAccounts,
  fetchRevolutExpenses,
  fetchRevolutTransactions,
  getRevolutRuntimeConfig,
  refreshRevolutAccessToken,
  type RevolutExpense,
  type RevolutTransaction,
} from "./revolut";
import {
  buildRevolutProviderRecordId,
  buildRevolutSyntheticTransaction,
  queueUniqueRevolutSyncJob,
  resolveOrCreateRevolutAccountLinks,
  REVOLUT_PROVIDER_NAME,
  runRevolutSyncWithLock,
  upsertAccountBalanceSnapshot,
} from "./revolut-sync-support";
import { mapFromSql } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { transactionColumnsSql } from "./transaction-columns";
import { updateTransactionRecord } from "./transaction-record";

function formatUnknownError(error: unknown) {
  return error instanceof Error ? error.message : "unknown error";
}

function warnRevolutSyncExpensesSkipped(connectionId: string, error: unknown) {
  console.warn(
    `[revolut-sync] Expenses enrichment skipped for connection ${connectionId}: ${formatUnknownError(error)}`,
  );
}

export async function processRevolutSyncJob(
  sql: SqlClient,
  userId: string,
  payloadJson: Record<string, unknown>,
) {
  const connectionId =
    typeof payloadJson.connectionId === "string"
      ? payloadJson.connectionId
      : "";
  if (!connectionId) {
    throw new Error("Bank sync job is missing connectionId.");
  }

  return runRevolutSyncWithLock(sql, connectionId, async () => {
    const connectionRows = await sql`
      select *
      from public.bank_connections
      where id = ${connectionId}
        and user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
      limit 1
    `;
    const connectionRow = connectionRows[0];
    if (!connectionRow) {
      throw new Error(`Bank connection ${connectionId} was not found.`);
    }

    const encryptedRefreshToken =
      typeof connectionRow.encrypted_refresh_token === "string"
        ? connectionRow.encrypted_refresh_token
        : "";
    if (!encryptedRefreshToken) {
      throw new Error(
        `Bank connection ${connectionId} is missing an encrypted refresh token.`,
      );
    }

    const config = getRevolutRuntimeConfig();
    const nowIso = new Date().toISOString();
    try {
      const refreshToken = decryptBankSecret(
        config.masterKey,
        encryptedRefreshToken,
      );
      const tokenResponse = await refreshRevolutAccessToken(
        config,
        refreshToken,
      );
      const nextEncryptedRefreshToken = tokenResponse.refresh_token
        ? encryptBankSecret(config.masterKey, tokenResponse.refresh_token)
        : encryptedRefreshToken;
      const accessToken = tokenResponse.access_token;
      const revolutAccounts = await fetchRevolutAccounts(config, accessToken);

      let dataset = await loadDatasetForUser(sql, userId);
      const linked = await resolveOrCreateRevolutAccountLinks(sql, {
        userId,
        dataset,
        connectionId,
        entityId: String(connectionRow.entity_id),
        revolutAccounts,
        actorName: "worker-revolut-sync",
        sourceChannel: "worker",
      });
      dataset = linked.dataset;

      const linksByExternalAccountId = new Map(
        linked.bankAccountLinks.map((link) => [link.externalAccountId, link]),
      );
      const accountsById = new Map(
        dataset.accounts.map((account) => [account.id, account]),
      );
      const revolutAccountsById = new Map(
        revolutAccounts.map((account) => [account.id, account]),
      );

      const snapshotAsOfDate = nowIso.slice(0, 10);
      for (const link of linked.bankAccountLinks) {
        const revolutAccount = revolutAccountsById.get(link.externalAccountId);
        if (!revolutAccount) {
          continue;
        }
        const balanceOriginal = new Decimal(revolutAccount.balance).toFixed(8);
        const balanceBaseEur = new Decimal(balanceOriginal)
          .times(
            resolveFxRate(
              dataset,
              revolutAccount.currency,
              "EUR",
              snapshotAsOfDate,
            ),
          )
          .toFixed(8);
        await upsertAccountBalanceSnapshot(sql, {
          accountId: link.accountId,
          asOfDate: snapshotAsOfDate,
          balanceOriginal,
          balanceCurrency: revolutAccount.currency,
          balanceBaseEur,
        });
        await sql`
          update public.accounts
          set last_imported_at = ${nowIso}
          where id = ${link.accountId}
            and user_id = ${userId}
        `;
      }

      const lastCursorCreatedAt =
        typeof connectionRow.last_cursor_created_at === "string"
          ? connectionRow.last_cursor_created_at
          : null;
      const fromDate = lastCursorCreatedAt
        ? new Date(
            Date.parse(lastCursorCreatedAt) -
              config.syncLookbackMinutes * 60_000,
          ).toISOString()
        : new Date(
            Date.now() - config.initialBackfillDays * 24 * 60 * 60_000,
          ).toISOString();

      const fetchedTransactions: RevolutTransaction[] = [];
      let nextToCursor: string | null = null;
      while (true) {
        const page = await fetchRevolutTransactions(config, accessToken, {
          from: fromDate,
          to: nextToCursor,
          count: 1000,
        });
        if (page.length === 0) {
          break;
        }
        fetchedTransactions.push(...page);
        if (page.length < 1000) {
          break;
        }
        const nextCursor = page.at(-1)?.created_at ?? null;
        if (!nextCursor || nextCursor === nextToCursor) {
          break;
        }
        nextToCursor = nextCursor;
      }

      const expenseByTransactionId = new Map<string, RevolutExpense>();
      try {
        let nextExpenseToCursor: string | null = null;
        while (true) {
          const page = await fetchRevolutExpenses(config, accessToken, {
            from: fromDate,
            to: nextExpenseToCursor,
            count: 500,
          });
          if (page.length === 0) {
            break;
          }
          for (const expense of page) {
            if (expense.transaction_id) {
              expenseByTransactionId.set(expense.transaction_id, expense);
            }
          }
          if (page.length < 500) {
            break;
          }
          const nextCursor = page.at(-1)?.expense_date ?? null;
          if (!nextCursor || nextCursor === nextExpenseToCursor) {
            break;
          }
          nextExpenseToCursor = nextCursor;
        }
      } catch (error) {
        warnRevolutSyncExpensesSkipped(connectionId, error);
      }

      const linkedAccountIds = linked.bankAccountLinks.map(
        (link) => link.accountId,
      );
      const existingRows =
        linkedAccountIds.length > 0
          ? await sql`
              select ${transactionColumnsSql(sql)}
              from public.transactions
              where user_id = ${userId}
                and provider_name = ${REVOLUT_PROVIDER_NAME}
                and account_id in ${sql(linkedAccountIds)}
            `
          : [];
      const existingTransactionsByProviderRecordId = new Map(
        existingRows
          .map((row) => mapFromSql<Transaction>(row))
          .filter((transaction) => transaction.providerRecordId)
          .map((transaction) => [
            transaction.providerRecordId as string,
            transaction,
          ]),
      );

      const newTransactionsByAccount = new Map<string, Transaction[]>();
      let latestSeenCursor = lastCursorCreatedAt;
      let mutatedExistingRows = 0;

      const chronologicalTransactions = [...fetchedTransactions].sort(
        (left, right) => left.created_at.localeCompare(right.created_at),
      );
      for (const revolutTransaction of chronologicalTransactions) {
        if (
          !latestSeenCursor ||
          revolutTransaction.created_at > latestSeenCursor
        ) {
          latestSeenCursor = revolutTransaction.created_at;
        }
        const expense =
          expenseByTransactionId.get(revolutTransaction.id) ?? null;
        for (const leg of revolutTransaction.legs) {
          const link = linksByExternalAccountId.get(leg.account_id);
          if (!link) {
            continue;
          }
          const account = accountsById.get(link.accountId);
          if (!account) {
            continue;
          }
          const providerRecordId = buildRevolutProviderRecordId(
            revolutTransaction,
            leg.leg_id,
          );
          const nextTransaction = buildRevolutSyntheticTransaction({
            dataset,
            account,
            transaction: revolutTransaction,
            expense,
            leg,
            importBatchId: null,
          });
          const existingTransaction =
            existingTransactionsByProviderRecordId.get(providerRecordId) ??
            null;

          if (revolutTransaction.state === "completed") {
            if (existingTransaction) {
              await updateTransactionRecord(sql, {
                userId,
                transactionId: existingTransaction.id,
                updatePayload: {
                  transaction_date: nextTransaction.transactionDate,
                  posted_date: nextTransaction.postedDate,
                  amount_original: nextTransaction.amountOriginal,
                  currency_original: nextTransaction.currencyOriginal,
                  amount_base_eur: nextTransaction.amountBaseEur,
                  fx_rate_to_eur: nextTransaction.fxRateToEur,
                  description_raw: nextTransaction.descriptionRaw,
                  description_clean: nextTransaction.descriptionClean,
                  source_fingerprint: nextTransaction.sourceFingerprint,
                  duplicate_key: nextTransaction.duplicateKey,
                  provider_name: nextTransaction.providerName,
                  provider_record_id: nextTransaction.providerRecordId,
                  raw_payload: nextTransaction.rawPayload,
                  voided_at: null,
                  exclude_from_analytics: false,
                  updated_at: nowIso,
                },
                returning: false,
              });
              mutatedExistingRows += 1;
            } else {
              const accountTransactions =
                newTransactionsByAccount.get(account.id) ?? [];
              accountTransactions.push(nextTransaction);
              newTransactionsByAccount.set(account.id, accountTransactions);
            }
            continue;
          }

          if (revolutTransaction.state === "reverted" && existingTransaction) {
            await updateTransactionRecord(sql, {
              userId,
              transactionId: existingTransaction.id,
              updatePayload: {
                raw_payload: nextTransaction.rawPayload,
                voided_at: nowIso,
                updated_at: nowIso,
              },
              returning: false,
            });
            mutatedExistingRows += 1;
          }
        }
      }

      let insertedTransactions = 0;
      for (const [
        accountId,
        preparedTransactions,
      ] of newTransactionsByAccount) {
        if (preparedTransactions.length === 0) {
          continue;
        }
        const account = accountsById.get(accountId);
        if (!account) {
          continue;
        }
        const dates = preparedTransactions.map(
          (transaction) => transaction.transactionDate,
        );
        const committed = await commitSyntheticImportBatch(sql, {
          userId,
          accountId,
          originalFilename: `revolut-sync-${account.defaultCurrency}-${snapshotAsOfDate}.json`,
          sourceKind: "bank_sync",
          providerName: REVOLUT_PROVIDER_NAME,
          bankConnectionId: connectionId,
          preparedTransactions,
          importedByActor: "worker-revolut-sync",
          dateRange: {
            start: [...dates].sort()[0] ?? snapshotAsOfDate,
            end: [...dates].sort().at(-1) ?? snapshotAsOfDate,
          },
        });
        insertedTransactions += committed.insertedTransactions.length;
      }

      if (mutatedExistingRows > 0 && insertedTransactions === 0) {
        await queueJob(sql, "metric_refresh", {
          connectionId,
          trigger: "bank_sync_update",
        });
      }

      const nextScheduledSyncAt = new Date(
        Date.now() + config.syncIntervalMinutes * 60_000,
      ).toISOString();
      await sql`
        update public.bank_connections
        set encrypted_refresh_token = ${nextEncryptedRefreshToken},
            status = ${"active"},
            last_cursor_created_at = ${latestSeenCursor ?? lastCursorCreatedAt},
            last_successful_sync_at = ${nowIso},
            auth_expires_at = ${new Date(
              Date.now() + tokenResponse.expires_in * 1000,
            ).toISOString()},
            last_error = null,
            updated_at = ${nowIso}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
      await queueUniqueRevolutSyncJob(sql, {
        userId,
        connectionId,
        trigger: "scheduled",
        availableAt: nextScheduledSyncAt,
      });

      return {
        connectionId,
        fetchedTransactions: fetchedTransactions.length,
        insertedTransactions,
        updatedTransactions: mutatedExistingRows,
        linkedAccountCount: linked.bankAccountLinks.length,
        latestCursorCreatedAt: latestSeenCursor,
        syncedAt: nowIso,
      };
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown Revolut sync failure.";
      const nextStatus = /invalid_grant|unauthorized|401/i.test(errorMessage)
        ? "reauthorization_required"
        : "error";
      await sql`
        update public.bank_connections
        set status = ${nextStatus},
            last_error = ${errorMessage},
            updated_at = ${nowIso}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
      throw error;
    }
  });
}

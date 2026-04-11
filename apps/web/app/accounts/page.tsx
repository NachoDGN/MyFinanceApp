import { AppShell } from "../../components/app-shell";
import { AccountsWorkbench } from "../../components/accounts-workbench";
import { RevolutConnectionsCard } from "../../components/revolut-connections-card";
import { formatCurrency, getAccountsModel } from "../../lib/queries";

export default async function AccountsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getAccountsModel(searchParams);
  const snapshots = new Map(
    model.accounts.balances.map((row) => [row.accountId, row]),
  );
  const bankLinksByAccountId = new Map(
    model.dataset.bankAccountLinks.map((link) => [link.accountId, link]),
  );
  const blockingUsageCounts = new Map<string, number>();

  const incrementUsage = (accountId: string | null | undefined) => {
    if (!accountId) return;
    blockingUsageCounts.set(
      accountId,
      (blockingUsageCounts.get(accountId) ?? 0) + 1,
    );
  };

  model.dataset.importBatches.forEach((batch) =>
    incrementUsage(batch.accountId),
  );
  model.dataset.transactions.forEach((transaction) => {
    incrementUsage(transaction.accountId);
    incrementUsage(transaction.relatedAccountId);
  });
  model.dataset.accountBalanceSnapshots.forEach((snapshot) =>
    incrementUsage(snapshot.accountId),
  );
  model.dataset.holdingAdjustments.forEach((adjustment) =>
    incrementUsage(adjustment.accountId),
  );
  model.dataset.investmentPositions.forEach((position) =>
    incrementUsage(position.accountId),
  );
  model.dataset.dailyPortfolioSnapshots.forEach((snapshot) =>
    incrementUsage(snapshot.accountId),
  );

  return (
    <AppShell
      pathname="/accounts"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Accounts</h1>
            <p className="page-subtitle">
              Cash and investment accounts are first-class. Broker accounts
              surface both cash balance and security market value.
            </p>
          </div>
        </div>

        <RevolutConnectionsCard
          configured={model.revolutRuntime.configured}
          missingEnvKeys={model.revolutRuntime.missingEnvKeys}
          entities={model.dataset.entities
            .filter((entity) => entity.active && entity.entityKind === "company")
            .map((entity) => ({
              id: entity.id,
              displayName: entity.displayName,
            }))}
          connections={model.dataset.bankConnections
            .filter((connection) => connection.provider === "revolut_business")
            .map((connection) => ({
              id: connection.id,
              entityName:
                model.dataset.entities.find(
                  (entity) => entity.id === connection.entityId,
                )?.displayName ?? connection.entityId,
              status: connection.status,
              lastSuccessfulSyncAt: connection.lastSuccessfulSyncAt ?? null,
              lastSyncQueuedAt: connection.lastSyncQueuedAt ?? null,
              lastWebhookAt: connection.lastWebhookAt ?? null,
              lastError: connection.lastError ?? null,
              linkedAccounts: model.dataset.bankAccountLinks
                .filter((link) => link.connectionId === connection.id)
                .map(
                  (link) =>
                    model.dataset.accounts.find(
                      (account) => account.id === link.accountId,
                    )?.displayName ?? link.accountId,
                ),
            }))}
        />

        <AccountsWorkbench
          entities={model.dataset.entities.filter((entity) => entity.active)}
          templates={model.dataset.templates}
          defaultCurrency={model.dataset.profile.defaultBaseCurrency}
          defaultCashStaleAfterDays={
            model.workspaceSettings.defaultCashStaleAfterDays
          }
          defaultInvestmentStaleAfterDays={
            model.workspaceSettings.defaultInvestmentStaleAfterDays
          }
          accounts={model.accounts.accounts.map((account) => {
            const snapshot = snapshots.get(account.id);
            const usageCount = blockingUsageCounts.get(account.id) ?? 0;
            const workspaceDefaultThreshold =
              account.assetDomain === "investment"
                ? model.workspaceSettings.defaultInvestmentStaleAfterDays
                : model.workspaceSettings.defaultCashStaleAfterDays;
            return {
              id: account.id,
              displayName: account.displayName,
              institutionName: account.institutionName,
              entityName:
                model.dataset.entities.find(
                  (entity) => entity.id === account.entityId,
                )?.displayName ?? account.entityId,
              accountType: account.accountType,
              currentBalance: formatCurrency(
                snapshot?.balanceBaseEur,
                model.currency,
              ),
              currentBalanceCurrency:
                snapshot?.balanceCurrency ?? account.defaultCurrency,
              lastImport: account.lastImportedAt?.slice(0, 10) ?? "Never",
              staleThreshold: account.staleAfterDays
                ? `${account.staleAfterDays} days`
                : `Workspace default · ${workspaceDefaultThreshold} days`,
              setupStatus: bankLinksByAccountId.has(account.id)
                ? "Revolut linked"
                : account.importTemplateDefaultId
                  ? "Template assigned"
                  : "Setup incomplete",
              balanceMode: account.balanceMode,
              aliases: account.matchingAliases.join(", ") || "—",
              defaultCurrency: account.defaultCurrency,
              openingBalanceOriginal: account.openingBalanceOriginal ?? null,
              openingBalanceDate: account.openingBalanceDate ?? null,
              includeInConsolidation: account.includeInConsolidation,
              importTemplateDefaultId: account.importTemplateDefaultId ?? null,
              matchingAliasesText: account.matchingAliases.join(", "),
              accountSuffix: account.accountSuffix ?? null,
              staleAfterDays: account.staleAfterDays ?? null,
              workspaceDefaultStaleAfterDays: workspaceDefaultThreshold,
              canDelete: usageCount === 0,
              deleteBlockedReason:
                usageCount === 0
                  ? null
                  : "Removal is locked because this account already has imported or derived history.",
            };
          })}
        />
      </div>
    </AppShell>
  );
}

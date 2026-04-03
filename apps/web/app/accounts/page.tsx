import { AppShell } from "../../components/app-shell";
import { AccountsWorkbench } from "../../components/accounts-workbench";
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
      state={{
        scopeParam: model.scopeParam,
        currency: model.currency,
        period: model.period.preset,
      }}
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

        <AccountsWorkbench
          entities={model.dataset.entities}
          templates={model.dataset.templates}
          accounts={model.accounts.accounts.map((account) => {
            const snapshot = snapshots.get(account.id);
            const usageCount = blockingUsageCounts.get(account.id) ?? 0;
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
                : "—",
              setupStatus: account.importTemplateDefaultId
                ? "Template assigned"
                : "Setup incomplete",
              balanceMode: account.balanceMode,
              aliases: account.matchingAliases.join(", ") || "—",
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

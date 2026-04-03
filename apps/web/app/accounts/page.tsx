import { AppShell } from "../../components/app-shell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { formatCurrency, getAccountsModel } from "../../lib/queries";

export default async function AccountsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getAccountsModel(searchParams);
  const snapshots = new Map(model.accounts.balances.map((row) => [row.accountId, row]));

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
              Cash and investment accounts are first-class. Broker accounts surface both cash balance and security market value.
            </p>
          </div>
        </div>

        {model.accounts.accounts.map((account) => {
          const snapshot = snapshots.get(account.id);
          return (
            <SectionCard key={account.id} title={account.displayName} subtitle={account.institutionName} span="span-6">
              <div className="split-grid">
                <div>
                  <span className="label-sm">Entity</span>
                  <div className="timeline-label">
                    {model.dataset.entities.find((entity) => entity.id === account.entityId)?.displayName}
                  </div>
                </div>
                <div>
                  <span className="label-sm">Current Balance</span>
                  <div className="timeline-label">
                    {formatCurrency(snapshot?.balanceBaseEur, model.currency)}
                  </div>
                </div>
                <div>
                  <span className="label-sm">Balance Source</span>
                  <div className="metric-nominal">{snapshot?.sourceKind ?? "—"}</div>
                </div>
                <div>
                  <span className="label-sm">Last Import</span>
                  <div className="metric-nominal">{account.lastImportedAt?.slice(0, 10) ?? "Never"}</div>
                </div>
                <div>
                  <span className="label-sm">Stale Threshold</span>
                  <div className="metric-nominal">{account.staleAfterDays ?? "—"} days</div>
                </div>
                <div>
                  <span className="label-sm">Setup</span>
                  <div className="metric-nominal">
                    {account.importTemplateDefaultId ? "Template assigned" : "Setup incomplete"}
                  </div>
                </div>
              </div>
            </SectionCard>
          );
        })}

        <SimpleTable
          span="span-12"
          headers={["Account", "Entity", "Institution", "Current Balance", "Currency", "Last Import", "Mode", "Aliases"]}
          rows={model.accounts.accounts.map((account) => {
            const snapshot = snapshots.get(account.id);
            return [
              account.displayName,
              model.dataset.entities.find((entity) => entity.id === account.entityId)?.displayName ?? account.entityId,
              account.institutionName,
              formatCurrency(snapshot?.balanceBaseEur, model.currency),
              snapshot?.balanceCurrency ?? account.defaultCurrency,
              account.lastImportedAt?.slice(0, 10) ?? "Never",
              account.balanceMode,
              account.matchingAliases.join(", "),
            ];
          })}
        />
      </div>
    </AppShell>
  );
}

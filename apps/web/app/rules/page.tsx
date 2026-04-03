import { AppShell } from "../../components/app-shell";
import { RulesWorkbench } from "../../components/rules-workbench";
import { getRulesModel } from "../../lib/queries";

export default async function RulesPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getRulesModel(searchParams);

  return (
    <AppShell
      pathname="/rules"
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
            <h1 className="page-title">Rules</h1>
            <p className="page-subtitle">
              Deterministic rules, system heuristics, and natural-language draft parsing live here so the classification layer can evolve with your ledger.
            </p>
          </div>
        </div>
        <RulesWorkbench
          model={{
            rules: model.rules.rules,
            drafts: model.drafts,
            deterministicSummaries: model.deterministicSummaries,
          }}
        />
      </div>
    </AppShell>
  );
}

import { AppShell } from "../../components/app-shell";
import { InsightCards, SectionCard } from "../../components/primitives";
import { getInsightsModel } from "../../lib/queries";

export default async function InsightsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getInsightsModel(searchParams);

  return (
    <AppShell
      pathname="/insights"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Insights</h1>
            <p className="page-subtitle">
              Backend-generated structured insights with explicit evidence. This page is facts-first, not prose-first.
            </p>
          </div>
        </div>

        <SectionCard title="Structured Insight Cards" subtitle="Deterministic engine" span="span-8">
          <InsightCards insights={model.insights} />
        </SectionCard>

        <SectionCard title="Quality Context" subtitle="Trust the numbers appropriately" span="span-4">
          <div className="legend-list">
            <span className="pill">Pending review: {model.summary.quality.pendingReviewCount}</span>
            <span className="pill">Unclassified amount: {model.summary.quality.unclassifiedAmountMtdEur} EUR</span>
            <span className="pill">Stale accounts: {model.summary.quality.staleAccountsCount}</span>
            <span className="pill">Price freshness: {model.summary.quality.priceFreshness}</span>
          </div>
        </SectionCard>
      </div>
    </AppShell>
  );
}

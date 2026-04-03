import { AppShell } from "../../components/app-shell";
import { SectionCard } from "../../components/primitives";
import { getSettingsModel } from "../../lib/queries";

export default async function SettingsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getSettingsModel(searchParams);

  return (
    <AppShell
      pathname="/settings"
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
            <h1 className="page-title">Settings</h1>
            <p className="page-subtitle">
              Minimal v1 settings surface for entity management, account thresholds, base currency, and market data controls.
            </p>
          </div>
        </div>

        <SectionCard title="Entities" subtitle="Current scope owners" span="span-6">
          <div className="legend-list">
            {model.dataset.entities.map((entity) => (
              <span key={entity.id} className="pill">
                {entity.displayName} · {entity.entityKind}
              </span>
            ))}
          </div>
        </SectionCard>

        <SectionCard title="Runtime Defaults" subtitle="Thresholds" span="span-6">
          <div className="legend-list">
            <span className="pill">Base currency: EUR</span>
            <span className="pill">Timezone: Europe/Madrid</span>
            <span className="pill">Cash stale default: 7 days</span>
            <span className="pill">Broker stale default: 3 days</span>
            <span className="pill">LLM low-confidence cutoff: 0.70</span>
            <span className="pill">Quote provider: Twelve Data</span>
          </div>
        </SectionCard>
      </div>
    </AppShell>
  );
}

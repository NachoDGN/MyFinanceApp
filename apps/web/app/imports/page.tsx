import { AppShell } from "../../components/app-shell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { getImportsModel } from "../../lib/queries";

export default async function ImportsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getImportsModel(searchParams);

  return (
    <AppShell
      pathname="/imports"
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
            <h1 className="page-title">Imports</h1>
            <p className="page-subtitle">
              Upload is deterministic and template-driven: account selection first, template selection second, preview before commit every time.
            </p>
          </div>
        </div>

        <SectionCard title="New Upload" subtitle="Preview before commit" span="span-6">
          <form className="form-grid">
            <label className="input-label">
              Account
              <select className="input-select">
                {model.dataset.accounts.map((account) => (
                  <option key={account.id}>{account.displayName}</option>
                ))}
              </select>
            </label>
            <label className="input-label">
              Template
              <select className="input-select">
                {model.templates.templates.map((template) => (
                  <option key={template.id}>{template.name}</option>
                ))}
              </select>
            </label>
            <label className="input-label">
              Filename
              <input className="input-field" defaultValue="statement.csv" />
            </label>
            <label className="input-label">
              Action
              <button className="btn-pill" type="button">
                Preview Import
              </button>
            </label>
          </form>
        </SectionCard>

        <SectionCard title="Preview Contract" subtitle="What preview returns" span="span-6">
          <div className="legend-list">
            {[
              "Parse success and detected date range",
              "Duplicate count and sample normalized rows",
              "Failed rows if parsing breaks",
              "Commit summary with inserted row count and queued jobs",
            ].map((item) => (
              <span key={item} className="pill">
                {item}
              </span>
            ))}
          </div>
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={["Filename", "Account", "Template", "Imported At", "Date Range", "Inserted", "Duplicates", "Failures", "Status"]}
          rows={model.importBatches.map((batch) => [
            batch.originalFilename,
            model.dataset.accounts.find((account) => account.id === batch.accountId)?.displayName ?? batch.accountId,
            model.templates.templates.find((template) => template.id === batch.templateId)?.name ?? batch.templateId,
            batch.importedAt,
            batch.detectedDateRange ? `${batch.detectedDateRange.start} → ${batch.detectedDateRange.end}` : "—",
            String(batch.rowCountInserted),
            String(batch.rowCountDuplicates),
            String(batch.rowCountFailed),
            batch.status,
          ])}
        />
      </div>
    </AppShell>
  );
}

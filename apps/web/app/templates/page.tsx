import { AppShell } from "../../components/app-shell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { getTemplatesModel } from "../../lib/queries";

export default async function TemplatesPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getTemplatesModel(searchParams);

  return (
    <AppShell
      pathname="/templates"
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
            <h1 className="page-title">Templates</h1>
            <p className="page-subtitle">
              Templates define the canonical dataframe contract. Columns are mapped manually; AI is explicitly not used for spreadsheet inference.
            </p>
          </div>
        </div>

        <SectionCard title="Template Configurator" subtitle="Deterministic column mapping" span="span-6">
          <form className="form-grid">
            <label className="input-label">
              Worksheet
              <input className="input-field" defaultValue="Transactions" />
            </label>
            <label className="input-label">
              Header Row
              <input className="input-field" defaultValue="3" />
            </label>
            <label className="input-label">
              Default Currency
              <select className="input-select" defaultValue="EUR">
                <option>EUR</option>
                <option>USD</option>
              </select>
            </label>
            <label className="input-label">
              Sign Logic
              <select className="input-select" defaultValue="signed_amount">
                <option>signed_amount</option>
                <option>debit_credit_columns</option>
              </select>
            </label>
            <label className="input-label" style={{ gridColumn: "1 / -1" }}>
              Canonical Map Preview
              <textarea
                className="input-textarea"
                defaultValue={`transaction_date -> date\ndescription_raw -> description\namount_original_signed -> net_amount\ncurrency_original -> currency`}
              />
            </label>
          </form>
        </SectionCard>

        <SectionCard title="Versioning Rules" subtitle="Safe evolution" span="span-6">
          <div className="legend-list">
            {[
              "Clone templates instead of destructive edits",
              "Keep old versions active for historical reproducibility",
              "Map to canonical fields before pandas ingest runs",
            ].map((item) => (
              <span key={item} className="pill">
                {item}
              </span>
            ))}
          </div>
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={["Template", "Institution", "Account Type", "File Kind", "Date Format", "Default Currency", "Version", "Active"]}
          rows={model.templates.templates.map((template) => [
            template.name,
            template.institutionName,
            template.compatibleAccountType,
            template.fileKind,
            template.dateFormat,
            template.defaultCurrency,
            String(template.version),
            template.active ? "Yes" : "No",
          ])}
        />
      </div>
    </AppShell>
  );
}

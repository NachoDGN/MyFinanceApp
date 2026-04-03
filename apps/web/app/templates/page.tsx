import { AppShell } from "../../components/app-shell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { TemplateWorkbench } from "../../components/template-workbench";
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
              Templates define how uploaded cash, brokerage, and exchange statements map into the canonical import dataframe before the enrichment pipeline runs.
            </p>
          </div>
        </div>

        <SectionCard title="Template Configurator" subtitle="Persisted template mappings" span="span-8">
          <TemplateWorkbench templates={model.templates.templates} />
        </SectionCard>

        <SectionCard title="Canonical Fields" subtitle="What templates can map" span="span-4">
          <div className="legend-list">
            {[
              "transaction_date / posted_date",
              "description_raw / amount_original_signed / currency_original",
              "external_reference / balance_original / transaction_type_raw",
              "security_symbol / security_name / quantity / unit_price_original",
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

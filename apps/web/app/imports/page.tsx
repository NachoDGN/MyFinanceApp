import { AppShell } from "../../components/app-shell";
import { ImportWorkbench } from "../../components/import-workbench";
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
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Imports</h1>
            <p className="page-subtitle">
              Upload spreadsheets in the browser, pick an existing template or
              create one from a new spreadsheet with AI inference, preview the
              canonical rows, then commit them into Postgres with queued
              enrichment.
            </p>
          </div>
        </div>

        <SectionCard
          title="New Upload"
          subtitle="Preview and commit from the browser"
          span="span-8"
        >
          <ImportWorkbench
            accounts={model.dataset.accounts}
            templates={model.templates.templates}
            importBatches={model.importBatches}
          />
        </SectionCard>

        <SectionCard
          title="Pipeline"
          subtitle="What happens after commit"
          span="span-4"
        >
          <div className="legend-list">
            {[
              "New spreadsheet runs AI-assisted table detection before saving a template",
              "Template maps spreadsheet columns into canonical fields",
              "Rows are fingerprinted before insert and duplicate-safe in Postgres",
              "New rows enter with pending LLM enrichment metadata",
              "Worker jobs classify transactions and store explanation + structured outputs",
            ].map((item) => (
              <span key={item} className="pill">
                {item}
              </span>
            ))}
          </div>
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={[
            "Filename",
            "Account",
            "Template",
            "Imported At",
            "Date Range",
            "Inserted",
            "Duplicates",
            "Failures",
            "Status",
          ]}
          rows={model.importBatches.map((batch) => [
            batch.originalFilename,
            model.dataset.accounts.find(
              (account) => account.id === batch.accountId,
            )?.displayName ?? batch.accountId,
            model.templates.templates.find(
              (template) => template.id === batch.templateId,
            )?.name ?? batch.templateId,
            batch.importedAt,
            batch.detectedDateRange
              ? `${batch.detectedDateRange.start} -> ${batch.detectedDateRange.end}`
              : "-",
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

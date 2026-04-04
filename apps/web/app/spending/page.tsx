import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  MetricCard,
  MultiSeriesChart,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import { formatCurrency, getSpendingModel } from "../../lib/queries";

export default async function SpendingPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getSpendingModel(searchParams);

  return (
    <AppShell
      pathname="/spending"
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
            <h1 className="page-title">Spending</h1>
            <p className="page-subtitle">
              Outflows are grouped by economic attribution, so cross-entity expenses follow the entity that actually owns the spend.
            </p>
          </div>
        </div>

        <div className="metrics-row">
          <MetricCard
            label="Current-Period Spending"
            value={formatCurrency(model.spendMetric?.valueDisplay, model.currency)}
            delta={`${model.spendMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(model.spendMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={Number(model.spendMetric?.deltaDisplay ?? "0") <= 0 ? "down" : "up"}
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.spendingEur))}
          />
          <MetricCard
            label="Trailing 3-Month Avg"
            value={formatCurrency(model.trailingThreeMonthAverage, model.currency)}
            delta="Trend"
            subtitle="Average monthly spend"
            direction="down"
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.spendingEur))}
          />
          <MetricCard
            label="Top Category"
            value={model.topCategory?.label ?? "N/A"}
            delta={model.topCategory ? formatCurrency(model.topCategory.amountEur, model.currency) : "N/A"}
            subtitle="Largest current-period category"
            direction="down"
            chartValues={model.summary.spendingByCategory.slice(0, 5).map((row) => Number(row.amountEur))}
          />
          <MetricCard
            label="Coverage"
            value={`${model.coverage}%`}
            delta={`${formatCurrency(model.summary.quality.unclassifiedAmountMtdEur, model.currency)}`}
            subtitle="Categorized spend share"
            direction={Number(model.coverage) >= 90 ? "up" : "down"}
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.operatingNetEur))}
          />
        </div>

        <SectionCard title="Monthly Spend Trend" subtitle="Trend" span="span-12">
          <MultiSeriesChart rows={model.summary.monthlySeries} />
        </SectionCard>

        <SectionCard title="Category Breakdown" subtitle="Current period" span="span-6">
          <DistributionList
            rows={model.summary.spendingByCategory.map((row) => ({
              label: row.label,
              amountEur: row.amountEur,
            }))}
            currency={model.currency}
          />
        </SectionCard>

        <SectionCard title="Merchant Table" subtitle="Largest current-period merchants" span="span-6">
          <DistributionList rows={model.merchantRows.slice(0, 8)} currency={model.currency} />
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={["Date", "Account", "Economic Entity", "Description", "Merchant", "Amount", "Category", "Review"]}
          rows={model.transactions.slice(0, 20).map((row) => [
            row.transactionDate,
            model.dataset.accounts.find((account) => account.id === row.accountId)?.displayName ?? row.accountId,
            model.dataset.entities.find((entity) => entity.id === row.economicEntityId)?.displayName ?? row.economicEntityId,
            row.descriptionRaw,
            row.merchantNormalized ?? "—",
            formatCurrency(row.amountBaseEur, model.currency),
            row.categoryCode ?? "—",
            <ReviewEditorCell
              transactionId={row.id}
              needsReview={row.needsReview}
              reviewReason={row.reviewReason}
              manualNotes={row.manualNotes}
              transactionClass={row.transactionClass}
              classificationSource={row.classificationSource}
              quantity={row.quantity}
              llmPayload={row.llmPayload}
            />,
          ])}
        />
      </div>
    </AppShell>
  );
}

import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  MetricCard,
  MultiSeriesChart,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { formatCurrency, getSpendingModel } from "../../lib/queries";

export default async function SpendingPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getSpendingModel(searchParams);
  const spendMetric = model.summary.metrics.find((metric) => metric.metricId === "spending_mtd_total");
  const trailingThreeMonthAverage = (
    model.summary.monthlySeries
      .slice(-3)
      .reduce((sum, row) => sum + Number(row.spendingEur), 0) / 3
  ).toFixed(2);
  const coverage = spendMetric?.valueBaseEur
    ? (
        (1 -
          Number(model.summary.quality.unclassifiedAmountMtdEur) /
            Math.max(Number(spendMetric.valueBaseEur), 1)) *
        100
      ).toFixed(2)
    : "100.00";
  const topCategory = model.summary.spendingByCategory[0];
  const merchantRows = [...new Map(
    model.transactions.map((row) => [
      row.merchantNormalized ?? row.descriptionClean,
      {
        label: row.merchantNormalized ?? row.descriptionClean,
        amountEur: "0.00",
      },
    ]),
  ).values()]
    .map((row) => ({
      ...row,
      amountEur: model.transactions
        .filter((transaction) => (transaction.merchantNormalized ?? transaction.descriptionClean) === row.label)
        .reduce((sum, transaction) => {
          const signed = transaction.transactionClass === "refund"
            ? -Number(transaction.amountBaseEur)
            : Math.abs(Number(transaction.amountBaseEur));
          return sum + signed;
        }, 0)
        .toFixed(2),
    }))
    .sort((a, b) => Number(b.amountEur) - Number(a.amountEur));

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
            value={formatCurrency(spendMetric?.valueDisplay, model.currency)}
            delta={`${spendMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(spendMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={Number(spendMetric?.deltaDisplay ?? "0") <= 0 ? "down" : "up"}
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.spendingEur))}
          />
          <MetricCard
            label="Trailing 3-Month Avg"
            value={formatCurrency(trailingThreeMonthAverage, model.currency)}
            delta="Trend"
            subtitle="Average monthly spend"
            direction="down"
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.spendingEur))}
          />
          <MetricCard
            label="Top Category"
            value={topCategory?.label ?? "N/A"}
            delta={topCategory ? formatCurrency(topCategory.amountEur, model.currency) : "N/A"}
            subtitle="Largest current-period category"
            direction="down"
            chartValues={model.summary.spendingByCategory.slice(0, 5).map((row) => Number(row.amountEur))}
          />
          <MetricCard
            label="Coverage"
            value={`${coverage}%`}
            delta={`${formatCurrency(model.summary.quality.unclassifiedAmountMtdEur, model.currency)}`}
            subtitle="Categorized spend share"
            direction={Number(coverage) >= 90 ? "up" : "down"}
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
          <DistributionList rows={merchantRows.slice(0, 8)} currency={model.currency} />
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
            row.needsReview ? "Needs review" : "OK",
          ])}
        />
      </div>
    </AppShell>
  );
}

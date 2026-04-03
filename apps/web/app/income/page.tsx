import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  MetricCard,
  MultiSeriesChart,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { formatCurrency, getIncomeModel } from "../../lib/queries";

export default async function IncomePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getIncomeModel(searchParams);
  const incomeMetric = model.summary.metrics.find((metric) => metric.metricId === "income_mtd_total");
  const sourceRows = [...new Map(
    model.transactions.map((row) => [
      row.counterpartyName ?? row.merchantNormalized ?? row.descriptionClean,
      {
        label: row.counterpartyName ?? row.merchantNormalized ?? row.descriptionClean,
        amountEur: "0.00",
      },
    ]),
  ).values()]
    .map((row) => ({
      ...row,
      amountEur: model.transactions
        .filter(
          (transaction) =>
            (transaction.counterpartyName ?? transaction.merchantNormalized ?? transaction.descriptionClean) ===
            row.label,
        )
        .reduce((sum, transaction) => sum + Number(transaction.amountBaseEur), 0)
        .toFixed(2),
    }))
    .sort((a, b) => Number(b.amountEur) - Number(a.amountEur));
  const trailingThreeMonthAverage = (
    model.summary.monthlySeries
      .slice(-3)
      .reduce((sum, row) => sum + Number(row.incomeEur), 0) / 3
  ).toFixed(2);
  const topSourceShare = sourceRows[0] && incomeMetric?.valueBaseEur
    ? ((Number(sourceRows[0].amountEur) / Math.max(Number(incomeMetric.valueBaseEur), 1)) * 100).toFixed(2)
    : "0.00";
  const investmentIncome = model.transactions
    .filter((row) => ["dividend", "interest"].includes(row.transactionClass))
    .reduce((sum, row) => sum + Number(row.amountBaseEur), 0)
    .toFixed(2);

  return (
    <AppShell
      pathname="/income"
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
            <h1 className="page-title">Income</h1>
            <p className="page-subtitle">
              Primary income KPIs exclude reimbursements, refunds, owner contributions, loan proceeds, and internal transfers.
            </p>
          </div>
        </div>

        <div className="metrics-row">
          <MetricCard
            label="Current-Period Income"
            value={formatCurrency(incomeMetric?.valueDisplay, model.currency)}
            delta={`${incomeMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(incomeMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={Number(incomeMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"}
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.incomeEur))}
          />
          <MetricCard
            label="Trailing 3-Month Avg"
            value={formatCurrency(trailingThreeMonthAverage, model.currency)}
            delta="Average"
            subtitle="Rolling mean of income"
            direction="up"
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.incomeEur))}
          />
          <MetricCard
            label="Top Source Concentration"
            value={`${topSourceShare}%`}
            delta={sourceRows[0]?.label ?? "N/A"}
            subtitle="Largest source share of current income"
            direction={Number(topSourceShare) < 60 ? "up" : "down"}
            chartValues={sourceRows.slice(0, 5).map((row) => Number(row.amountEur))}
          />
          <MetricCard
            label="Dividends + Interest"
            value={formatCurrency(investmentIncome, model.currency)}
            delta="Investment income"
            subtitle="Shown here and on Investments"
            direction="up"
            chartValues={model.transactions.slice(0, 5).map((row) => Number(row.amountBaseEur))}
          />
        </div>

        <SectionCard title="Monthly Inflow Trend" subtitle="Trend" span="span-12">
          <MultiSeriesChart rows={model.summary.monthlySeries} />
        </SectionCard>

        <SectionCard title="Income Source Breakdown" subtitle="Current period" span="span-6">
          <DistributionList rows={sourceRows} currency={model.currency} />
        </SectionCard>

        <SectionCard title="Dividend and Interest Breakdown" subtitle="Investment income" span="span-6">
          <DistributionList
            rows={model.transactions
              .filter((row) => ["dividend", "interest"].includes(row.transactionClass))
              .map((row) => ({
                label: row.descriptionRaw,
                amountEur: row.amountBaseEur,
              }))}
            currency={model.currency}
          />
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={["Date", "Entity", "Account", "Source", "Class", "Category", "Amount", "Confidence"]}
          rows={model.transactions.map((row) => [
            row.transactionDate,
            model.dataset.entities.find((entity) => entity.id === row.economicEntityId)?.displayName ?? row.economicEntityId,
            model.dataset.accounts.find((account) => account.id === row.accountId)?.displayName ?? row.accountId,
            row.counterpartyName ?? row.merchantNormalized ?? row.descriptionRaw,
            row.transactionClass,
            row.categoryCode ?? "—",
            formatCurrency(row.amountBaseEur, model.currency),
            row.classificationConfidence,
          ])}
        />
      </div>
    </AppShell>
  );
}

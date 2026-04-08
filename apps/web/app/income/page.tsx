import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  MetricCard,
  MultiSeriesChart,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { formatCurrency, getIncomeModel } from "../../lib/queries";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";

export default async function IncomePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getIncomeModel(searchParams);

  return (
    <AppShell
      pathname="/income"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
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
            value={formatCurrency(model.incomeMetric?.valueDisplay, model.currency)}
            delta={`${model.incomeMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(model.incomeMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={Number(model.incomeMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"}
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.incomeEur))}
          />
          <MetricCard
            label="Trailing 3-Month Avg"
            value={formatCurrency(model.trailingThreeMonthAverage, model.currency)}
            delta="Average"
            subtitle="Rolling mean of income"
            direction="up"
            chartValues={model.summary.monthlySeries.slice(-5).map((row) => Number(row.incomeEur))}
          />
          <MetricCard
            label="Top Source Concentration"
            value={`${model.topSourceShare}%`}
            delta={model.sourceRows[0]?.label ?? "N/A"}
            subtitle="Largest source share of current income"
            direction={Number(model.topSourceShare) < 60 ? "up" : "down"}
            chartValues={model.sourceRows.slice(0, 5).map((row) => Number(row.amountEur))}
          />
          <MetricCard
            label="Dividends + Interest"
            value={formatCurrency(model.investmentIncome, model.currency)}
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
          <DistributionList rows={model.sourceRows} currency={model.currency} />
        </SectionCard>

        <SectionCard title="Dividend and Interest Breakdown" subtitle="Investment income" span="span-6">
          <DistributionList rows={model.investmentIncomeRows.map((row) => ({
            label: row.descriptionRaw,
            amountEur:
              convertBaseEurToDisplayAmount(
                model.dataset,
                row.amountBaseEur,
                model.currency,
                row.transactionDate,
              ) ?? "0.00",
          }))} currency={model.currency} />
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
            formatCurrency(
              convertBaseEurToDisplayAmount(
                model.dataset,
                row.amountBaseEur,
                model.currency,
                row.transactionDate,
              ),
              model.currency,
            ),
            row.classificationConfidence,
          ])}
        />
      </div>
    </AppShell>
  );
}

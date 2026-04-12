import type { ReactNode } from "react";

import { AppShell } from "./app-shell";
import { InvestmentPriceRefreshButton } from "./investment-price-refresh-button";
import { ManualInvestmentWorkbench } from "./manual-investment-workbench";
import {
  DistributionList,
  InvestmentAllocationCard,
  InvestmentMetricCard,
  SectionCard,
  SimpleTable,
} from "./primitives";
import { ReviewEditorCell } from "./review-editor-cell";
import { type InvestmentsPageModel } from "../lib/investments-page";

function PositionListSection({
  title,
  subtitle,
  rows,
  emptyMessage,
}: {
  title: string;
  subtitle: string;
  rows: InvestmentsPageModel["fundRows"];
  emptyMessage: string;
}) {
  return (
    <SectionCard title={title} subtitle={subtitle} span="span-4">
      <div className="investment-position-list">
        {rows.length === 0 ? (
          <div className="table-empty-state">{emptyMessage}</div>
        ) : (
          rows.map((row) => (
            <article className="investment-position-card" key={row.key}>
              <div className="investment-position-head">
                <div className="investment-position-copy">
                  <h3 className="investment-position-name">{row.title}</h3>
                  <p className="investment-position-symbol">{row.subtitle}</p>
                </div>
                <div className="investment-position-values">
                  <strong>{row.value}</strong>
                  {row.returnDisplay ? (
                    <span
                      className={`investment-return ${row.returnClass ?? "positive"}`}
                    >
                      {row.returnDisplay}
                    </span>
                  ) : (
                    <span className="muted">{row.fallbackNote}</span>
                  )}
                </div>
              </div>
            </article>
          ))
        )}
      </div>
    </SectionCard>
  );
}

function HoldingsTable({
  rows,
  headers,
  getCurrentPriceCell,
}: {
  rows: InvestmentsPageModel["holdingRows"];
  headers: string[];
  getCurrentPriceCell: (row: InvestmentsPageModel["holdingRows"][number]) => ReactNode;
}) {
  return (
    <SimpleTable
      span="span-12"
      headers={headers}
      rows={rows.map((row) => [
        row.securityName,
        row.symbol,
        row.accountName,
        row.quantityDisplay,
        row.avgCostDisplay,
        getCurrentPriceCell(row),
        row.currentValueDisplay,
        row.unrealizedDisplay,
        row.freshnessLabel,
      ])}
    />
  );
}

function ProcessedTransactionsSection({
  model,
}: {
  model: InvestmentsPageModel;
}) {
  return (
    <section className="section-card span-12 investment-review-section">
      <div className="investment-review-header">
        <div>
          <span className="investment-review-kicker">
            {model.securityFilter
              ? `${model.totalProcessedRows} of ${model.totalProcessedRowsOverall} resolved rows`
              : `${model.totalProcessedRows} resolved rows`}
          </span>
          <h2 className="investment-review-title">
            Processed Investment Transactions
          </h2>
          {model.securityFilter ? (
            <p className="muted" style={{ marginTop: 6, fontSize: 13 }}>
              Security filter: {model.securityFilter}
            </p>
          ) : null}
        </div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "flex-end",
            gap: 12,
          }}
        >
          <form
            action="/investments"
            method="get"
            className="inline-actions"
            style={{ justifyContent: "flex-end" }}
          >
            <input type="hidden" name="scope" value={model.scopeParam} />
            <input type="hidden" name="currency" value={model.currency} />
            <input type="hidden" name="period" value={model.period.preset} />
            {model.referenceDate ? (
              <input type="hidden" name="asOf" value={model.referenceDate} />
            ) : null}
            {model.period.preset === "custom" ? (
              <>
                <input type="hidden" name="start" value={model.period.start} />
                <input type="hidden" name="end" value={model.period.end} />
              </>
            ) : null}
            <label className="input-label" style={{ minWidth: 240 }}>
              <span>Filter by security</span>
              <input
                className="input-field"
                type="search"
                name="security"
                placeholder="Ticker, name, or ISIN"
                defaultValue={model.securityFilter}
              />
            </label>
            <button className="btn-ghost" type="submit">
              Filter
            </button>
            {model.securityFilter ? (
              <a className="btn-ghost" href={model.buildHref(1, "")}>
                Clear
              </a>
            ) : null}
          </form>
          {model.totalPages > 1 ? (
            <div className="investment-review-pagination">
              <span className="investment-review-page-pill">
                Page {model.safePage} of {model.totalPages}
              </span>
              {model.safePage > 1 ? (
                <a
                  className="btn-ghost"
                  href={model.buildHref(model.safePage - 1)}
                >
                  Previous
                </a>
              ) : null}
              {model.safePage < model.totalPages ? (
                <a className="btn-ghost" href={model.buildHref(model.safePage + 1)}>
                  Next
                </a>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
      {model.processedRows.length === 0 ? (
        <div className="table-empty-state">
          {model.securityFilter
            ? `No processed investment transactions match "${model.securityFilter}".`
            : "No processed investment transactions are available for this scope."}
        </div>
      ) : (
        <div className="investment-review-scroll">
          <div className="investment-review-table">
            <div
              className="investment-review-grid-head"
              style={{ gridTemplateColumns: model.processedLedgerColumns }}
            >
              {[
                "Date",
                "Description",
                "Class",
                "Qty",
                "Security",
                "Amount",
                "Review",
              ].map((header, index) => (
                <div
                  className={
                    index === 3
                      ? "investment-review-head-cell centered"
                      : index === 5
                        ? "investment-review-head-cell amount"
                        : "investment-review-head-cell"
                  }
                  key={header}
                >
                  {header}
                </div>
              ))}
            </div>
            {model.processedRows.map((row) => (
              <div
                className="investment-review-grid-row"
                key={row.id}
                style={{ gridTemplateColumns: model.processedLedgerColumns }}
              >
                <div className="investment-review-date">
                  <span>{row.dateTop}</span>
                  {row.dateBottom ? <span>{row.dateBottom}</span> : null}
                </div>
                <div className="investment-review-description">
                  {row.descriptionRaw}
                </div>
                <div className="investment-review-copy">{row.transactionClass}</div>
                <div className="investment-review-copy centered">
                  {row.quantityDisplay}
                </div>
                <div className="investment-review-copy breakable">
                  {row.securityLabel}
                </div>
                <div className="investment-review-copy amount">
                  {row.amountDisplay}
                </div>
                <div className="investment-review-panel">
                  <ReviewEditorCell
                    transactionId={row.id}
                    needsReview={row.needsReview}
                    reviewReason={row.reviewReason}
                    manualNotes={row.manualNotes}
                    transactionClass={row.transactionClass}
                    classificationSource={row.classificationSource}
                    securitySymbol={row.reviewSecuritySymbol}
                    quantity={row.quantity}
                    llmPayload={row.llmPayload}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function UnresolvedTransactionsSection({
  model,
}: {
  model: InvestmentsPageModel;
}) {
  return (
    <section className="section-card span-12 investment-review-section">
      <div className="investment-review-header">
        <div>
          <span className="investment-review-kicker">Review queue</span>
          <h2 className="investment-review-title">
            Unresolved Investment Events
          </h2>
        </div>
      </div>
      {model.unresolvedRows.length === 0 ? (
        <div className="table-empty-state">
          No unresolved investment transactions are waiting for review.
        </div>
      ) : (
        <div className="investment-review-scroll">
          <div className="investment-review-table">
            <div
              className="investment-review-grid-head"
              style={{ gridTemplateColumns: model.unresolvedLedgerColumns }}
            >
              {["Date", "Description", "Qty", "Security", "Amount", "Review"].map(
                (header, index) => (
                  <div
                    className={
                      index === 4
                        ? "investment-review-head-cell amount"
                        : index === 2
                          ? "investment-review-head-cell centered"
                          : "investment-review-head-cell"
                    }
                    key={header}
                  >
                    {header}
                  </div>
                ),
              )}
            </div>
            {model.unresolvedRows.map((row) => (
              <div
                className="investment-review-grid-row"
                key={row.id}
                style={{ gridTemplateColumns: model.unresolvedLedgerColumns }}
              >
                <div className="investment-review-date">
                  <span>{row.dateTop}</span>
                  {row.dateBottom ? <span>{row.dateBottom}</span> : null}
                </div>
                <div className="investment-review-description">
                  {row.descriptionRaw}
                </div>
                <div className="investment-review-copy centered">
                  {row.quantityDisplay}
                </div>
                <div className="investment-review-copy breakable">
                  {row.securityLabel}
                </div>
                <div className="investment-review-copy amount">
                  {row.amountDisplay}
                </div>
                <div className="investment-review-panel">
                  <ReviewEditorCell
                    transactionId={row.id}
                    needsReview={row.needsReview}
                    reviewReason={row.reviewReason}
                    manualNotes={row.manualNotes}
                    transactionClass={row.transactionClass}
                    classificationSource={row.classificationSource}
                    securitySymbol={row.reviewSecuritySymbol}
                    quantity={row.quantity}
                    llmPayload={row.llmPayload}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

export function InvestmentsPageView({
  model,
}: {
  model: InvestmentsPageModel;
}) {
  return (
    <AppShell
      pathname="/investments"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header investments-page-header">
          <div>
            <h1 className="page-title">Investments</h1>
            <p className="page-subtitle">
              Holdings are rebuilt live from resolved investment rows, explicit
              opening adjustments, manual fund valuations, and crypto treasury
              balances held in BTC and ETH business accounts. Global totals stay
              consolidated by default and can then be filtered by entity using
              the buttons above.
            </p>
          </div>
          <InvestmentPriceRefreshButton />
        </div>

        <div className="investments-hero">
          <div className="metrics-row metrics-row-investments">
            {model.metricCards.map((card) => (
              <InvestmentMetricCard key={card.label} {...card} />
            ))}
          </div>
          <InvestmentAllocationCard
            rows={model.portfolioAllocationRows}
            currency={model.currency}
          />
        </div>

        <SectionCard
          title="Snapshot by Asset Class"
          subtitle="Live rebuilt market values and open-position returns"
          span="span-12"
        >
          <div className="investment-breakdown-grid">
            {model.assetSummaries.map((summary) => (
              <article className="investment-summary-card" key={summary.key}>
                <div className="investment-summary-head">
                  <div>
                    <span className="label-sm">{summary.label}</span>
                    <h3 className="investment-summary-title">{summary.title}</h3>
                  </div>
                  <span className="pill">{summary.pill}</span>
                </div>
                <div className="investment-summary-value">{summary.value}</div>
                <div className="investment-summary-meta">
                  {summary.key === "cash" || summary.key === "crypto" ? (
                    <>
                      <span>{summary.metaPrimary}</span>
                      <span className="muted">{summary.metaSecondary}</span>
                    </>
                  ) : (
                    <>
                      <span className={`investment-return ${summary.returnClass ?? "positive"}`}>
                        {summary.metaPrimary}
                      </span>
                      <span className="muted">{summary.metaSecondary}</span>
                    </>
                  )}
                </div>
              </article>
            ))}
          </div>
        </SectionCard>

        <ManualInvestmentWorkbench
          entities={model.manualInvestmentEntities}
          cashAccounts={model.manualInvestmentCashAccounts}
          manualInvestments={model.manualInvestmentSummaries}
          referenceDate={model.referenceDate}
        />
        {model.manualInvestmentSummaries.length === 0 ? (
          <SectionCard
            title="Manual Fund Valuations"
            subtitle="Separate company-level inputs"
            span="span-4"
          >
            <div className="status-note" style={{ marginTop: 0 }}>
              No manual fund valuations are configured right now. The fund
              values you do see on this page are coming from your
              broker-imported fund holdings, not from separate manual company
              fund inputs.
            </div>
          </SectionCard>
        ) : null}

        <PositionListSection
          title="Funds"
          subtitle="Current value, unrealized EUR, and return %"
          rows={model.fundRows}
          emptyMessage="No funds are available for this scope."
        />

        <PositionListSection
          title="Stocks & ETF"
          subtitle="Current value, unrealized EUR, and return %"
          rows={model.stockRows}
          emptyMessage="No stocks or ETFs are available for this scope."
        />

        <SectionCard
          title="Crypto Treasury"
          subtitle="Current EUR value of BTC and ETH balances"
          span="span-4"
        >
          <div className="investment-position-list">
            {model.cryptoRows.length === 0 ? (
              <div className="table-empty-state">
                No crypto treasury balances are available for this scope.
              </div>
            ) : (
              model.cryptoRows.map((row) => (
                <article className="investment-position-card" key={row.key}>
                  <div className="investment-position-head">
                    <div className="investment-position-copy">
                      <h3 className="investment-position-name">{row.title}</h3>
                      <p className="investment-position-symbol">{row.subtitle}</p>
                    </div>
                    <div className="investment-position-values">
                      <strong>{row.value}</strong>
                      <span className="muted">{row.fallbackNote}</span>
                    </div>
                  </div>
                </article>
              ))
            )}
          </div>
        </SectionCard>

        <SectionCard
          title="Allocation by Account"
          subtitle="Broker, treasury, and crypto split"
          span="span-12"
        >
          <DistributionList rows={model.accountAllocationRows} currency={model.currency} />
        </SectionCard>

        <HoldingsTable
          rows={model.holdingRows}
          headers={[
            "Security",
            "Ticker",
            "Account",
            "Qty",
            "Avg Cost",
            "Current Price",
            "Current Value",
            "Unrealized",
            "Freshness",
          ]}
          getCurrentPriceCell={(row) => (
            <div style={{ display: "grid", gap: 4 }}>
              <span>{row.currentPricePrimary}</span>
              {row.currentPriceSecondary ? (
                <span className="muted" style={{ fontSize: 12 }}>
                  {row.currentPriceSecondary} native
                </span>
              ) : null}
              {row.currentPriceNote ? (
                <span className="muted" style={{ fontSize: 12 }}>
                  {row.currentPriceNote}
                </span>
              ) : null}
            </div>
          )}
        />

        <ProcessedTransactionsSection model={model} />
        <UnresolvedTransactionsSection model={model} />
      </div>
    </AppShell>
  );
}

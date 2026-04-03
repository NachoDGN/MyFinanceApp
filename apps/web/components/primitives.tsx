import type { ReactNode } from "react";

function formatCurrency(amount: string | null | undefined, currency: string) {
  if (amount === null || amount === undefined) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(Number(amount));
}

function formatPercent(value: string | null | undefined) {
  if (value === null || value === undefined) return "N/A";
  return `${Number(value).toFixed(2)}%`;
}

function formatDate(value: string) {
  const normalized = value.length <= 10 ? `${value.slice(0, 10)}T00:00:00Z` : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

export function MetricCard({
  label,
  value,
  delta,
  subtitle,
  direction,
  chartValues,
}: {
  label: string;
  value: string;
  delta: string;
  subtitle: string;
  direction: "up" | "down";
  chartValues: number[];
}) {
  const max = Math.max(...chartValues, 1);

  return (
    <div className="kpi-card">
      <div className="kpi-header">
        <span className="label-sm">{label}</span>
        <span className={`trend-indicator ${direction === "up" ? "trend-up" : "trend-down"}`}>
          {delta}
        </span>
      </div>
      <div className="metric-value">{value}</div>
      <div className="metric-nominal">{subtitle}</div>
      <div className="chart-container">
        {chartValues.map((bar, index) => (
          <div
            key={`${label}-${index}`}
            className={`chart-bar ${index === chartValues.length - 1 ? "active" : ""}`}
            style={{ height: `${Math.max(12, (bar / max) * 100)}%` }}
          />
        ))}
      </div>
    </div>
  );
}

export function TimelinePanel({
  title,
  actions,
  transactions,
  currency,
}: {
  title: string;
  actions?: ReactNode;
  transactions: Array<{
    id: string;
    transactionDate: string;
    descriptionRaw: string;
    amountDisplay: string;
    positive?: boolean;
  }>;
  currency: string;
}) {
  return (
    <div className="details-section">
      <div className="section-header">
        <h2 className="section-title">{title}</h2>
        {actions}
      </div>
      <div className="timeline-list">
        {transactions.map((transaction, index) => (
          <div className="timeline-item" key={transaction.id}>
            <span className="timeline-date">{formatDate(transaction.transactionDate)}</span>
            <div className="timeline-content">
              <div
                className="timeline-dot"
                style={
                  transaction.positive
                    ? {
                        background: "#1A1A1A",
                        boxShadow: "0 0 0 1px #1A1A1A",
                      }
                    : undefined
                }
              />
              <span className="timeline-label">{transaction.descriptionRaw}</span>
            </div>
            <span
              className="timeline-amount"
              style={transaction.positive ? { color: "var(--color-accent)" } : undefined}
            >
              {transaction.amountDisplay}
            </span>
          </div>
        ))}
      </div>
      <div className="view-all">
        <span className="timeline-date">Showing last {transactions.length} transactions</span>
        <a href={`/transactions?currency=${currency}`} className="view-all-link">
          View Ledger →
        </a>
      </div>
    </div>
  );
}

export function CategoryListCard({
  title,
  rows,
  ctaLabel,
  ctaHref,
}: {
  title: string;
  rows: Array<{ label: string; value: string; icon: string }>;
  ctaLabel?: string;
  ctaHref?: string;
}) {
  return (
    <div className="category-card">
      <span className="label-sm">{title}</span>
      <div style={{ marginTop: 16 }}>
        {rows.map((row) => (
          <div className="category-item" key={row.label}>
            <div className="category-info">
              <div className="category-icon">{row.icon}</div>
              <span className="timeline-label">{row.label}</span>
            </div>
            <span className="timeline-amount">{row.value}</span>
          </div>
        ))}
      </div>
      {ctaLabel && ctaHref ? (
        <a className="btn-ghost" style={{ width: "100%", marginTop: 16 }} href={ctaHref}>
          {ctaLabel}
        </a>
      ) : null}
    </div>
  );
}

export function HighlightCard({
  title,
  body,
  metric,
  footer,
}: {
  title: string;
  body: string;
  metric: string;
  footer: string;
}) {
  return (
    <div className="category-card" style={{ background: "var(--color-accent)", color: "white" }}>
      <span className="label-sm" style={{ color: "rgba(255,255,255,0.8)" }}>
        {title}
      </span>
      <p style={{ marginTop: 12, fontWeight: 500, fontSize: 14 }}>{body}</p>
      <div style={{ marginTop: 20, fontWeight: 700, fontSize: 24 }}>{metric}</div>
      <span className="label-sm" style={{ color: "rgba(255,255,255,0.8)", marginTop: 4 }}>
        {footer}
      </span>
    </div>
  );
}

export function QualityBanner({
  rows,
}: {
  rows: Array<{ label: string; value: string; meta?: string }>;
}) {
  return (
    <div className="banner-card">
      <div className="section-header">
        <div>
          <span className="label-sm">Data Quality</span>
          <h2 className="section-title">Numbers are accompanied by completeness metadata</h2>
        </div>
      </div>
      <div className="banner-grid" style={{ marginTop: 24 }}>
        {rows.map((row) => (
          <div key={row.label}>
            <span className="label-sm">{row.label}</span>
            <div className="metric-value" style={{ fontSize: 28 }}>
              {row.value}
            </div>
            {row.meta ? <div className="metric-nominal">{row.meta}</div> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

export function SectionCard({
  title,
  subtitle,
  span = "span-6",
  children,
  actions,
}: {
  title: string;
  subtitle?: string;
  span?: "span-12" | "span-8" | "span-6" | "span-4" | "span-3";
  children: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <section className={`section-card ${span}`}>
      <div className="section-header">
        <div>
          <span className="label-sm">{subtitle ?? "Section"}</span>
          <h2 className="section-title">{title}</h2>
        </div>
        {actions}
      </div>
      <div className="section-card-body">{children}</div>
    </section>
  );
}

export function MultiSeriesChart({
  rows,
}: {
  rows: Array<{ month: string; incomeEur: string; spendingEur: string; operatingNetEur: string }>;
}) {
  const max = Math.max(
    ...rows.flatMap((row) => [
      Number(row.incomeEur),
      Number(row.spendingEur),
      Math.abs(Number(row.operatingNetEur)),
    ]),
    1,
  );
  return (
    <div className="stack-bars">
      {rows.map((row) => (
        <div className="stack-month" key={row.month}>
          <div
            className="stack-piece income"
            style={{ height: `${Math.max(8, (Number(row.incomeEur) / max) * 180)}px` }}
          />
          <div
            className="stack-piece spending"
            style={{ height: `${Math.max(8, (Number(row.spendingEur) / max) * 180)}px` }}
          />
          <div
            className="stack-piece net"
            style={{ height: `${Math.max(8, (Math.abs(Number(row.operatingNetEur)) / max) * 180)}px` }}
          />
          <div className="stack-label">
            {new Intl.DateTimeFormat("en-US", { month: "short" }).format(
              new Date(`${row.month}T00:00:00Z`),
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

export function DistributionList({
  rows,
  currency,
}: {
  rows: Array<{ label: string; amountEur: string; allocationPercent?: string }>;
  currency: string;
}) {
  return (
    <div className="legend-list">
      {rows.map((row, index) => (
        <div className="legend-row" key={row.label}>
          <div className="legend-key">
            <span
              className="legend-swatch"
              style={{
                background:
                  index % 3 === 0
                    ? "var(--color-accent)"
                    : index % 3 === 1
                      ? "rgba(255,75,43,0.45)"
                      : "rgba(0,0,0,0.12)",
              }}
            />
            <span className="timeline-label">{row.label}</span>
          </div>
          <div className="timeline-amount">
            {formatCurrency(row.amountEur, currency)}
            {row.allocationPercent ? (
              <span className="muted" style={{ display: "block", fontSize: 12 }}>
                {formatPercent(row.allocationPercent)}
              </span>
            ) : null}
          </div>
        </div>
      ))}
    </div>
  );
}

export function SimpleTable({
  headers,
  rows,
  span = "span-12",
}: {
  headers: string[];
  rows: ReactNode[][];
  span?: "span-12" | "span-8" | "span-6" | "span-4" | "span-3";
}) {
  return (
    <div className={`table-card ${span}`}>
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              {headers.map((header) => (
                <th key={header}>{header}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={`row-${rowIndex}`}>
                {row.map((cell, cellIndex) => (
                  <td key={`cell-${rowIndex}-${cellIndex}`}>{cell}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function InsightCards({ insights }: { insights: Array<{ id: string; title: string; body: string; evidence: string[]; severity: string }> }) {
  return (
    <div className="legend-list">
      {insights.map((insight) => (
        <div
          key={insight.id}
          className="category-card"
          style={{
            padding: 20,
            border: insight.severity === "warning" ? "1px solid rgba(255,75,43,0.18)" : undefined,
          }}
        >
          <span className={`pill ${insight.severity === "warning" ? "warning" : ""}`}>
            {insight.severity}
          </span>
          <h3 style={{ marginTop: 12, fontSize: 18 }}>{insight.title}</h3>
          <p className="muted" style={{ marginTop: 10 }}>
            {insight.body}
          </p>
          <div className="evidence-list">
            {insight.evidence.map((item) => (
              <div key={item} className="evidence-item">
                {item}
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

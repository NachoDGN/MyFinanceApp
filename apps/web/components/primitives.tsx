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
  const normalized =
    value.length <= 10 ? `${value.slice(0, 10)}T00:00:00Z` : value;
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
  density = "default",
}: {
  label: string;
  value: string;
  delta: string;
  subtitle: string;
  direction: "up" | "down";
  chartValues: number[];
  density?: "default" | "compact";
}) {
  const max = Math.max(...chartValues, 1);

  return (
    <div
      className={`kpi-card ${density === "compact" ? "kpi-card-compact" : ""}`}
    >
      <div className="kpi-header">
        <span className="label-sm">{label}</span>
        <span
          className={`trend-indicator ${direction === "up" ? "trend-up" : "trend-down"}`}
        >
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

const allocationPalette = [
  "#ff4b2b",
  "#1a1a1a",
  "#d97d54",
  "#264653",
  "#2a9d8f",
  "#8ecae6",
  "#e9c46a",
  "#7d5fff",
  "#c1121f",
  "#6c757d",
];

function buildAllocationSlices(
  rows: Array<{ label: string; amountEur: string }>,
) {
  const positiveRows = rows
    .map((row) => ({
      label: row.label,
      amount: Number(row.amountEur ?? 0),
    }))
    .filter((row) => row.amount > 0)
    .sort((left, right) => right.amount - left.amount);

  const topRows = positiveRows.slice(0, 8);
  const remainder = positiveRows
    .slice(8)
    .reduce((sum, row) => sum + row.amount, 0);
  if (remainder > 0) {
    topRows.push({ label: "Other", amount: remainder });
  }

  const total = topRows.reduce((sum, row) => sum + row.amount, 0);

  return {
    total,
    slices: topRows.map((row, index) => ({
      ...row,
      percent: total > 0 ? (row.amount / total) * 100 : 0,
      color: allocationPalette[index % allocationPalette.length],
    })),
  };
}

export function PortfolioAllocationCard({
  title,
  subtitle,
  rows,
  currency,
}: {
  title: string;
  subtitle: string;
  rows: Array<{ label: string; amountEur: string }>;
  currency: string;
}) {
  const { total, slices } = buildAllocationSlices(rows);
  const size = 220;
  const center = size / 2;
  const radius = 74;
  const strokeWidth = 24;
  const circumference = 2 * Math.PI * radius;
  let runningOffset = 0;

  return (
    <section className="allocation-card">
      <div className="section-header">
        <div>
          <span className="label-sm">{subtitle}</span>
          <h2 className="section-title">{title}</h2>
        </div>
        <span className="pill">
          {formatCurrency(total.toFixed(2), currency)}
        </span>
      </div>
      <div className="allocation-card-body">
        <div className="allocation-donut">
          <svg
            viewBox={`0 0 ${size} ${size}`}
            role="img"
            aria-label={`${title} chart`}
          >
            <circle
              cx={center}
              cy={center}
              r={radius}
              fill="none"
              stroke="rgba(0,0,0,0.06)"
              strokeWidth={strokeWidth}
            />
            {slices.length === 0
              ? null
              : slices.map((slice) => {
                  const dash = (slice.percent / 100) * circumference;
                  const offset = circumference - runningOffset;
                  runningOffset += dash;
                  return (
                    <circle
                      key={slice.label}
                      cx={center}
                      cy={center}
                      r={radius}
                      fill="none"
                      stroke={slice.color}
                      strokeWidth={strokeWidth}
                      strokeDasharray={`${dash} ${circumference - dash}`}
                      strokeDashoffset={offset}
                      strokeLinecap="butt"
                      transform={`rotate(-90 ${center} ${center})`}
                    />
                  );
                })}
          </svg>
          <div className="allocation-donut-center">
            <span className="label-sm">Market value</span>
            <strong>{formatCurrency(total.toFixed(2), currency)}</strong>
            <span className="muted">Portfolio mix by security</span>
          </div>
        </div>
        <div className="allocation-legend">
          {slices.length === 0 ? (
            <div className="muted" style={{ fontSize: 14 }}>
              No priced securities available yet.
            </div>
          ) : (
            slices.map((slice) => (
              <div className="allocation-row" key={slice.label}>
                <div className="allocation-key">
                  <span
                    className="legend-swatch"
                    style={{ background: slice.color }}
                  />
                  <span className="timeline-label">{slice.label}</span>
                </div>
                <div className="timeline-amount">
                  {formatCurrency(slice.amount.toFixed(2), currency)}
                  <span
                    className="muted"
                    style={{ display: "block", fontSize: 12 }}
                  >
                    {slice.percent.toFixed(1)}%
                  </span>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
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
            <span className="timeline-date">
              {formatDate(transaction.transactionDate)}
            </span>
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
              <span className="timeline-label">
                {transaction.descriptionRaw}
              </span>
            </div>
            <span
              className="timeline-amount"
              style={
                transaction.positive
                  ? { color: "var(--color-accent)" }
                  : undefined
              }
            >
              {transaction.amountDisplay}
            </span>
          </div>
        ))}
      </div>
      <div className="view-all">
        <span className="timeline-date">
          Showing last {transactions.length} transactions
        </span>
        <a
          href={`/transactions?currency=${currency}`}
          className="view-all-link"
        >
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
        <a
          className="btn-ghost"
          style={{ width: "100%", marginTop: 16 }}
          href={ctaHref}
        >
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
    <div
      className="category-card"
      style={{ background: "var(--color-accent)", color: "white" }}
    >
      <span className="label-sm" style={{ color: "rgba(255,255,255,0.8)" }}>
        {title}
      </span>
      <p style={{ marginTop: 12, fontWeight: 500, fontSize: 14 }}>{body}</p>
      <div style={{ marginTop: 20, fontWeight: 700, fontSize: 24 }}>
        {metric}
      </div>
      <span
        className="label-sm"
        style={{ color: "rgba(255,255,255,0.8)", marginTop: 4 }}
      >
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
          <h2 className="section-title">
            Numbers are accompanied by completeness metadata
          </h2>
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

export function ReviewStateCell({
  needsReview,
  reviewReason,
}: {
  needsReview: boolean;
  reviewReason?: string | null;
}) {
  if (!needsReview) {
    return <span className="pill">OK</span>;
  }

  return (
    <div style={{ display: "grid", gap: 6, minWidth: 220 }}>
      <span className="pill warning">Needs review</span>
      <span
        className="muted"
        style={{ fontSize: 12, lineHeight: 1.4 }}
        title={reviewReason ?? "Reason unavailable."}
      >
        {reviewReason ?? "Reason unavailable."}
      </span>
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
  rows: Array<{
    month: string;
    incomeEur: string;
    spendingEur: string;
    operatingNetEur: string;
  }>;
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
            style={{
              height: `${Math.max(8, (Number(row.incomeEur) / max) * 180)}px`,
            }}
          />
          <div
            className="stack-piece spending"
            style={{
              height: `${Math.max(8, (Number(row.spendingEur) / max) * 180)}px`,
            }}
          />
          <div
            className="stack-piece net"
            style={{
              height: `${Math.max(8, (Math.abs(Number(row.operatingNetEur)) / max) * 180)}px`,
            }}
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
              <span
                className="muted"
                style={{ display: "block", fontSize: 12 }}
              >
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

export function InsightCards({
  insights,
}: {
  insights: Array<{
    id: string;
    title: string;
    body: string;
    evidence: string[];
    severity: string;
  }>;
}) {
  return (
    <div className="legend-list">
      {insights.map((insight) => (
        <div
          key={insight.id}
          className="category-card"
          style={{
            padding: 20,
            border:
              insight.severity === "warning"
                ? "1px solid rgba(255,75,43,0.18)"
                : undefined,
          }}
        >
          <span
            className={`pill ${insight.severity === "warning" ? "warning" : ""}`}
          >
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

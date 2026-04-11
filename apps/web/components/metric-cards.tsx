import {
  formatCurrency,
  formatPercent,
} from "../lib/formatters";

function buildSparkBars(chartValues: number[], targetBars = 5) {
  const absoluteValues = chartValues
    .map((value) => {
      if (!Number.isFinite(value)) return 0;
      return Math.abs(value);
    })
    .filter((value) => value > 0);

  const values = absoluteValues.slice(-targetBars);
  while (values.length < targetBars) {
    values.unshift(0);
  }

  const max = Math.max(...values, 1);
  return values.map((value, index) => ({
    key: `${index}-${value}`,
    height: value > 0 ? Math.max(16, (value / max) * 100) : 12,
    active: index === values.length - 1 && value > 0,
  }));
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

export function InvestmentMetricCard({
  label,
  value,
  badge,
  subtitle,
  chartValues,
  badgeTone = "accent",
}: {
  label: string;
  value: string;
  badge: string;
  subtitle: string;
  chartValues: number[];
  badgeTone?: "accent" | "neutral";
}) {
  const bars = buildSparkBars(chartValues);

  return (
    <section className="investment-kpi-card">
      <div className="investment-kpi-copy">
        <div className="investment-kpi-header">
          <h2 className="investment-kpi-label">{label}</h2>
          <span
            className={`investment-kpi-badge ${badgeTone === "neutral" ? "neutral" : ""}`}
          >
            {badge}
          </span>
        </div>
        <div className="investment-kpi-value">{value}</div>
        <p className="investment-kpi-subtitle">{subtitle}</p>
      </div>
      <div className="investment-spark-bars" aria-hidden="true">
        {bars.map((bar) => (
          <div
            key={bar.key}
            className={`investment-spark-bar ${bar.active ? "active" : ""}`}
            style={{ height: `${bar.height}%` }}
          />
        ))}
      </div>
    </section>
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
  const size = 440;
  const center = size / 2;
  const radius = 144;
  const strokeWidth = 42;
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

export function InvestmentAllocationCard({
  rows,
  currency,
}: {
  rows: Array<{ label: string; amountEur: string }>;
  currency: string;
}) {
  const { total, slices } = buildAllocationSlices(rows);
  const radius = 40;
  const strokeWidth = 18;
  const circumference = 2 * Math.PI * radius;
  let runningOffset = 0;

  return (
    <section className="investment-allocation-panel">
      <div className="investment-allocation-layout">
        <div className="investment-allocation-donut">
          <svg
            viewBox="0 0 100 100"
            role="img"
            aria-label="Portfolio allocation"
          >
            <circle
              cx="50"
              cy="50"
              r={radius}
              fill="none"
              stroke="rgba(0, 0, 0, 0.08)"
              strokeWidth={strokeWidth}
            />
            {slices.map((slice) => {
              const dash = (slice.percent / 100) * circumference;
              const segment = (
                <circle
                  key={slice.label}
                  cx="50"
                  cy="50"
                  r={radius}
                  fill="none"
                  stroke={slice.color}
                  strokeWidth={strokeWidth}
                  strokeDasharray={`${dash} ${circumference - dash}`}
                  strokeDashoffset={-runningOffset}
                  transform="rotate(-90 50 50)"
                />
              );
              runningOffset += dash;
              return segment;
            })}
          </svg>
          <div className="investment-allocation-center">
            <span className="investment-allocation-label">Market Value</span>
            <strong>{formatCurrency(total.toFixed(2), currency)}</strong>
            <span className="investment-allocation-caption">
              Portfolio mix by security
            </span>
          </div>
        </div>
        <div className="investment-allocation-legend">
          {slices.length === 0 ? (
            <div className="investment-allocation-empty">
              No priced securities available yet.
            </div>
          ) : (
            slices.map((slice) => (
              <div className="investment-allocation-row" key={slice.label}>
                <div className="investment-allocation-key">
                  <span
                    className="investment-allocation-dot"
                    style={{ backgroundColor: slice.color }}
                  />
                  <span className="investment-allocation-name">
                    {slice.label}
                  </span>
                </div>
                <div className="investment-allocation-values">
                  <div>{formatCurrency(slice.amount.toFixed(2), currency)}</div>
                  <div className="investment-allocation-share">
                    {slice.percent.toFixed(1)}%
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
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
        <div className="legend-row" key={`${row.label}-${index}`}>
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

import { getTransactionReviewState } from "@myfinance/domain/client";
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

export function TimelinePanel({
  title,
  actions,
  transactions,
  currency,
  viewAllHref,
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
  viewAllHref?: string;
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
          href={viewAllHref ?? `/transactions?currency=${currency}`}
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

function formatReviewReason(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        return formatReviewReason(JSON.parse(trimmed)) ?? trimmed;
      } catch {
        return trimmed;
      }
    }
    return trimmed;
  }
  if (Array.isArray(value)) {
    const issueMessages = value
      .map((entry) => {
        if (!entry || typeof entry !== "object") {
          return null;
        }
        const path = Array.isArray((entry as { path?: unknown }).path)
          ? ((entry as { path: unknown[] }).path
              .filter((segment) => typeof segment === "string")
              .join(".") || null)
          : null;
        const message =
          typeof (entry as { message?: unknown }).message === "string"
            ? (entry as { message: string }).message
            : null;
        if (!path && !message) {
          return null;
        }
        return path ? `${path}: ${message ?? "Invalid value"}` : message;
      })
      .filter((entry): entry is string => Boolean(entry));
    if (issueMessages.length > 0) {
      return `LLM output validation failed: ${issueMessages.join("; ")}`;
    }
    return JSON.stringify(value);
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function ReviewStateCell({
  needsReview,
  reviewReason,
  transactionClass,
  classificationSource,
  securitySymbol,
  quantity,
  llmPayload,
}: {
  needsReview: boolean;
  reviewReason?: unknown;
  transactionClass?: string | null;
  classificationSource?: string | null;
  securitySymbol?: string | null;
  quantity?: string | null;
  llmPayload?: unknown;
}) {
  const reviewState = getTransactionReviewState({ needsReview, llmPayload });
  const normalizedReviewReason = formatReviewReason(reviewReason);
  const normalizedPendingReason = normalizedReviewReason?.replace(
    "Pending enrichment pipeline.",
    "Queued for automatic transaction analysis.",
  );
  const normalizedQuantity =
    quantity && Number(quantity) !== 0
      ? Number.isInteger(Number(quantity))
        ? String(Number(quantity))
        : Number(quantity)
            .toFixed(4)
            .replace(/\.?0+$/, "")
      : null;
  const sourceLabel =
    classificationSource === "investment_parser"
      ? "investment parser"
      : classificationSource === "user_rule"
        ? "rule"
        : classificationSource === "llm"
          ? "LLM"
          : classificationSource === "transfer_matcher"
            ? "transfer matcher"
            : classificationSource === "manual_override"
              ? "manual review"
              : null;
  const explanation =
    llmPayload &&
    typeof llmPayload === "object" &&
    "explanation" in llmPayload &&
    typeof (llmPayload as { explanation?: unknown }).explanation === "string"
      ? (llmPayload as { explanation: string }).explanation
      : null;
  const llmReason =
    llmPayload &&
    typeof llmPayload === "object" &&
    "reason" in llmPayload &&
    typeof (llmPayload as { reason?: unknown }).reason === "string"
      ? (llmPayload as { reason: string }).reason
      : null;
  const llmModel =
    llmPayload &&
    typeof llmPayload === "object" &&
    "model" in llmPayload &&
    typeof (llmPayload as { model?: unknown }).model === "string"
      ? (llmPayload as { model: string }).model
      : null;
  const llmCompletedAt =
    llmPayload &&
    typeof llmPayload === "object" &&
    "timing" in llmPayload &&
    typeof (llmPayload as { timing?: unknown }).timing === "object" &&
    (llmPayload as { timing: { completedAt?: unknown } }).timing &&
    typeof (llmPayload as { timing: { completedAt?: unknown } }).timing
      .completedAt === "string"
      ? ((llmPayload as { timing: { completedAt: string } }).timing
          .completedAt ?? null)
      : null;
  const llmDurationMs =
    llmPayload &&
    typeof llmPayload === "object" &&
    "timing" in llmPayload &&
    typeof (llmPayload as { timing?: unknown }).timing === "object" &&
    (llmPayload as { timing: { durationMs?: unknown } }).timing &&
    typeof (llmPayload as { timing: { durationMs?: unknown } }).timing
      .durationMs === "number"
      ? ((llmPayload as { timing: { durationMs: number } }).timing.durationMs ??
        null)
      : null;
  const llmTrigger =
    llmPayload &&
    typeof llmPayload === "object" &&
    "reviewContext" in llmPayload &&
    typeof (llmPayload as { reviewContext?: unknown }).reviewContext ===
      "object" &&
    (llmPayload as { reviewContext: { trigger?: unknown } }).reviewContext &&
    typeof (llmPayload as { reviewContext: { trigger?: unknown } })
      .reviewContext.trigger === "string"
      ? ((llmPayload as { reviewContext: { trigger: string } }).reviewContext
          .trigger ?? null)
      : null;
  const llmLogSummary =
    llmModel || llmCompletedAt || llmDurationMs !== null
      ? [
          llmTrigger === "manual_review_update"
            ? "manual re-review"
            : llmTrigger === "manual_resolved_review"
              ? "resolved re-review"
              : llmTrigger === "import_classification"
                ? "import enrichment"
                : null,
          llmModel,
          llmCompletedAt
            ? new Intl.DateTimeFormat("en-US", {
                month: "short",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
              }).format(new Date(llmCompletedAt))
            : null,
          typeof llmDurationMs === "number"
            ? `${Math.round(llmDurationMs)}ms`
            : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : null;
  const resolvedSummary =
    sourceLabel || transactionClass || securitySymbol || normalizedQuantity
      ? [
          sourceLabel ? `Resolved by ${sourceLabel}` : null,
          transactionClass ? transactionClass.replace(/_/g, " ") : null,
          securitySymbol ? `security ${securitySymbol}` : null,
          normalizedQuantity ? `qty ${normalizedQuantity}` : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : null;

  if (reviewState === "resolved") {
    return (
      <div style={{ display: "grid", gap: 6, minWidth: 220 }}>
        <span className="pill">Resolved</span>
        {resolvedSummary ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {resolvedSummary}
          </span>
        ) : null}
        {explanation && explanation !== resolvedSummary ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {explanation}
          </span>
        ) : null}
        {llmLogSummary ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {llmLogSummary}
          </span>
        ) : null}
      </div>
    );
  }

  if (reviewState === "pending_enrichment") {
    return (
      <div style={{ display: "grid", gap: 6, minWidth: 220 }}>
        <span className="pill">Analyzing</span>
        <span
          className="muted"
          style={{ fontSize: 12, lineHeight: 1.4 }}
          title={
            normalizedPendingReason ??
            "Queued for automatic transaction analysis."
          }
        >
          {normalizedPendingReason ?? "Queued for automatic transaction analysis."}
        </span>
        {llmLogSummary ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {llmLogSummary}
          </span>
        ) : null}
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 6, minWidth: 220 }}>
      <span className="pill warning">Needs review</span>
      <span
        className="muted"
        style={{ fontSize: 12, lineHeight: 1.4 }}
        title={normalizedReviewReason ?? "Reason unavailable."}
      >
        {normalizedReviewReason ?? "Reason unavailable."}
      </span>
      {explanation && explanation !== normalizedReviewReason ? (
        <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
          {explanation}
        </span>
      ) : null}
      {llmReason &&
      llmReason !== normalizedReviewReason &&
      llmReason !== explanation ? (
        <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
          Latest analyzer: {llmReason}
        </span>
      ) : null}
      {llmLogSummary ? (
        <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
          {llmLogSummary}
        </span>
      ) : null}
    </div>
  );
}

export function ReviewQueueList({
  rows,
  currency,
}: {
  rows: Array<{
    label: string;
    amountEur: string;
    reviewReason?: unknown;
    securitySymbol?: string | null;
    transactionClass?: string | null;
  }>;
  currency: string;
}) {
  return (
    <div className="review-queue">
      {rows.map((row, index) => (
        <div className="review-queue-item" key={`${row.label}-${index}`}>
          <div className="review-queue-header">
            <div className="review-queue-title">
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
            <span className="timeline-amount">
              {formatCurrency(row.amountEur, currency)}
            </span>
          </div>
          {row.securitySymbol || row.transactionClass ? (
            <div className="review-queue-meta">
              {row.securitySymbol ? (
                <span className="pill">{row.securitySymbol}</span>
              ) : null}
              {row.transactionClass ? (
                <span className="pill">
                  {row.transactionClass.replace(/_/g, " ")}
                </span>
              ) : null}
            </div>
          ) : null}
          <p className="review-queue-reason">
            {formatReviewReason(row.reviewReason) ?? "Reason unavailable."}
          </p>
        </div>
      ))}
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

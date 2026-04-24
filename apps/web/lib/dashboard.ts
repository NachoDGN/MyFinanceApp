export function formatMonthLabel(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
  }).format(new Date(`${value}T00:00:00Z`));
}

export function formatMonthRange(start: string, end: string) {
  return `${formatMonthLabel(start)} ${start.slice(0, 4)} — ${formatMonthLabel(end)} ${end.slice(0, 4)}`;
}

export function getPeriodLabel(period: {
  preset: string;
  start: string;
  end: string;
}) {
  if (period.preset === "all") return "All Time";
  if (period.preset === "mtd") return "Month to Date";
  if (period.preset === "ytd") return "Year to Date";
  if (period.preset === "week") return "Week to Date";
  if (period.preset === "24m") return "Trailing 24 Months";
  return formatMonthRange(period.start, period.end);
}

export function formatPercentLabel(value: number | string | null | undefined) {
  const numeric = typeof value === "number" ? value : Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "0.00%";
  }

  return `${numeric.toFixed(2)}%`;
}

export function buildTrendPoints(
  values: number[],
  width: number,
  height: number,
) {
  const safeValues = values.length > 0 ? values : [0];
  const max = Math.max(...safeValues, 1);

  return safeValues.map((value, index) => {
    const x =
      safeValues.length === 1
        ? width / 2
        : (index / (safeValues.length - 1)) * width;
    const y = height - (Math.max(value, 0) / max) * height;
    return { x, y };
  });
}

export function buildLinePath(points: Array<{ x: number; y: number }>) {
  if (points.length === 0) return "";
  return points
    .map(
      (point, index) =>
        `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`,
    )
    .join(" ");
}

export function buildAreaPath(
  points: Array<{ x: number; y: number }>,
  width: number,
  height: number,
) {
  if (points.length === 0) return "";
  const line = buildLinePath(points);
  return `${line} L ${width} ${height} L 0 ${height} Z`;
}

export function formatDeltaBadge(deltaPercent: string | null | undefined) {
  if (!deltaPercent) {
    return "0.00%";
  }

  const numeric = Number(deltaPercent);
  if (!Number.isFinite(numeric)) {
    return "0.00%";
  }

  return `${numeric.toFixed(2)}%`;
}

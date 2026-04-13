export function formatMonthLabel(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
  }).format(new Date(`${value}T00:00:00Z`));
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

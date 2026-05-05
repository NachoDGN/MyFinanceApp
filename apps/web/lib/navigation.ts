export type NavigationState = {
  scopeParam: string;
  currency: string;
  period: string;
  referenceDate?: string;
  latestReferenceDate?: string;
  start?: string;
  end?: string;
};

type NavigationOverrides = Partial<
  Pick<
    NavigationState,
    "scopeParam" | "currency" | "period" | "referenceDate" | "start" | "end"
  >
>;

export function buildHref(
  pathname: string,
  current: NavigationState,
  overrides: NavigationOverrides,
  extraParams: Record<string, string | undefined> = {},
) {
  const period = overrides.period ?? current.period;
  const query = new URLSearchParams({
    scope: overrides.scopeParam ?? current.scopeParam,
    currency: overrides.currency ?? current.currency,
    period,
  });
  const referenceDate = overrides.referenceDate ?? current.referenceDate;
  if (referenceDate) {
    query.set("asOf", referenceDate);
  }
  const start = overrides.start ?? current.start;
  const end = overrides.end ?? current.end;
  if (period === "custom" && start && end) {
    query.set("start", start);
    query.set("end", end);
  }
  for (const [key, value] of Object.entries(extraParams)) {
    if (typeof value === "string" && value.trim() !== "") {
      query.set(key, value);
    } else {
      query.delete(key);
    }
  }
  return `${pathname}?${query.toString()}`;
}

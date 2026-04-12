const ISO_DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const ISO_DATE_TIME_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/;

export function normalizeSqlDateValue(value: unknown): string | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime())
      ? null
      : value.toISOString().slice(0, 10);
  }

  if (typeof value !== "string") {
    return null;
  }

  if (ISO_DATE_ONLY_PATTERN.test(value)) {
    return value;
  }

  if (!ISO_DATE_TIME_PATTERN.test(value)) {
    return null;
  }

  const normalized = new Date(value);
  return Number.isNaN(normalized.getTime())
    ? null
    : normalized.toISOString().slice(0, 10);
}

export function isIsoDateString(value: string | null | undefined): value is string {
  return Boolean(value && ISO_DATE_ONLY_PATTERN.test(value));
}
